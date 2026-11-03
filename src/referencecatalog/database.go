package referencecatalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

var tables = map[string]string{"news-sources": "content_sources", "media-sources": "content_sources", "discovery-profiles": "discovery_profiles", "topics": "topics", "topic-categories": "topic_categories", "quality-profiles": "quality_profiles"}

type row map[string]any
type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func load(ctx context.Context, q queryer, kind, tenant, scope string) ([]row, error) {
	table := tables[kind]
	where := "tenant_id=$1"
	args := []any{tenant}
	if scope == "global" {
		where = "tenant_id IS NULL"
		args = nil
	}
	if kind == "news-sources" {
		where += " AND category='news'"
	}
	if kind == "media-sources" {
		where += " AND category='media'"
	}
	rs, e := q.QueryContext(ctx, "SELECT to_jsonb(t) FROM "+table+" t WHERE "+where+" ORDER BY id", args...)
	if e != nil {
		return nil, e
	}
	defer rs.Close()
	out := []row{}
	for rs.Next() {
		var b []byte
		if e = rs.Scan(&b); e != nil {
			return nil, e
		}
		var r row
		if e = json.Unmarshal(b, &r); e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	return out, rs.Err()
}
func project(kind string, r row) map[string]any {
	out := map[string]any{}
	for _, k := range fields[kind] {
		if v, ok := r[k]; ok {
			if (kind == "news-sources" || kind == "media-sources") && (k == "metadata" || k == "api_config") {
				if v == nil {
					v = map[string]any{}
				}
				if m, ok := v.(map[string]any); ok {
					filtered := map[string]any{}
					allowed := configFields
					if k == "metadata" {
						allowed = metadataFields
					}
					for key, value := range m {
						if has(allowed, key) {
							filtered[key] = value
						}
					}
					v = filtered
				}
			}
			out[k] = v
		}
	}
	return out
}
func identity(kind string, d map[string]any) string {
	switch kind {
	case "topics", "topic-categories":
		return str(d["slug"])
	case "news-sources", "media-sources":
		return str(d["type"]) + ":" + canonicalLocator(str(d["feed_url"]))
	case "discovery-profiles":
		return str(d["category"]) + ":" + strings.ToLower(str(d["name"]))
	default:
		return fmt.Sprint(d["source_type"]) + ":" + str(d["profile_purpose"]) + ":" + strings.ToLower(str(d["name"]))
	}
}
func keyFor(kind string, d map[string]any) string {
	return strings.ReplaceAll(kind, "-", ".") + "." + Hash(identity(kind, d))[:20]
}
func Export(ctx context.Context, stable, dev, tenant, release string) (Catalog, error) {
	c := Catalog{Manifest: Manifest{FormatVersion: 1, CatalogName: "wahb", ReleaseVersion: release, RequiredMigration: Migration, Files: map[string]string{}, Origins: map[string]Origin{}}}
	for _, env := range []string{"stable", "dev"} {
		raw := stable
		if env == "dev" {
			raw = dev
		}
		db, e := sql.Open("postgres", raw)
		if e != nil {
			return c, errors.New("export connection configuration failed")
		}
		e = exportDB(ctx, db, env, tenant, &c)
		db.Close()
		if e != nil {
			return c, e
		}
	}
	return c, nil
}
func exportDB(ctx context.Context, db *sql.DB, env, tenant string, c *Catalog) error {
	tx, e := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if e != nil {
		return errors.New("origin database unavailable")
	}
	defer tx.Rollback()
	profiles := map[string]string{}
	for _, kind := range Groups {
		if env == "dev" && kind != "media-sources" && kind != "discovery-profiles" {
			continue
		}
		if env == "stable" && kind == "media-sources" {
			continue
		}
		rs, e := load(ctx, tx, kind, tenant, "tenant")
		if e != nil {
			return fmt.Errorf("export %s: %w", kind, e)
		}
		if kind == "quality-profiles" {
			global, e := load(ctx, tx, kind, tenant, "global")
			if e != nil {
				return e
			}
			rs = append(rs, global...)
		}
		for _, r := range rs {
			if kind == "discovery-profiles" {
				want := "news"
				if env == "dev" {
					want = "media"
				}
				if r["category"] != want {
					continue
				}
				profiles[fmt.Sprint(r["id"])] = keyFor(kind, project(kind, r))
			}
		}
		if kind == "discovery-profiles" { // Sources are emitted after profile keys have been collected below.
			for _, r := range rs {
				want := "news"
				if env == "dev" {
					want = "media"
				}
				if r["category"] != want {
					continue
				}
				d := project(kind, r)
				c.Records = append(c.Records, Record{keyFor(kind, d), kind, "tenant", d, ""})
			}
			continue
		}
		for _, r := range rs {
			for _, field := range []string{"metadata", "api_config"} {
				if m, ok := r[field].(map[string]any); ok {
					allowed := configFields
					if field == "metadata" {
						allowed = metadataFields
					}
					for key := range m {
						if !has(allowed, key) && !(field == "metadata" && has([]string{"media_finding_health", "media_finding_sample_items"}, key)) {
							return fmt.Errorf("unclassified export field %s.%s", field, key)
						}
					}
				}
			}
			d := project(kind, r)
			scope := "tenant"
			if kind == "quality-profiles" && r["tenant_id"] == nil {
				scope = "global"
			}
			rec := Record{Key: keyFor(kind, d), Kind: kind, Scope: scope, Definition: d}
			if kind == "quality-profiles" && scope == "tenant" {
				rec.Key = "quality.profiles." + Hash("tenant:" + identity(kind, d))[:20]
			}
			if kind == "news-sources" || kind == "media-sources" {
				if ref := r["discovery_profile_id"]; ref != nil {
					var ok bool
					rec.ProfileKey, ok = profiles[fmt.Sprint(ref)]
					if !ok {
						return fmt.Errorf("source discovery profile is outside the selected %s origin/category", env)
					}
				}
			}
			c.Records = append(c.Records, rec)
		}
	}
	for _, g := range []string{"news-sources", "media-sources", "news-discovery", "media-discovery", "topic-categories", "topics", "quality-profiles"} {
		want := "stable"
		if strings.HasPrefix(g, "media-") {
			want = "dev"
		}
		if env != want {
			continue
		}
		selected := []Record{}
		for _, r := range c.Records {
			if r.Kind == g || (g == "news-discovery" && r.Kind == "discovery-profiles" && r.Definition["category"] == "news") || (g == "media-discovery" && r.Kind == "discovery-profiles" && r.Definition["category"] == "media") {
				selected = append(selected, r)
			}
		}
		sort.Slice(selected, func(i, j int) bool { return selected[i].Key < selected[j].Key })
		c.Manifest.Origins[g] = Origin{Environment: env, Tenant: tenant, CapturedAt: time.Now().UTC().Format(time.RFC3339), ProjectionHash: Hash(selected)}
	}
	return tx.Commit()
}

// Target excludes passwords and connection parameters.
func Target(raw string) string {
	u, _ := url.Parse(raw)
	if u == nil {
		return "invalid"
	}
	return u.Host + u.EscapedPath()
}

type Binding struct {
	ID         string
	Projection map[string]any
}
type Action struct {
	Record        Record         `json:"record"`
	State         string         `json:"state"`
	ID            string         `json:"id,omitempty"`
	Before        map[string]any `json:"before,omitempty"`
	BindingBefore string         `json:"binding_before"`
	RowBefore     string         `json:"row_before,omitempty"`
}
type Plan struct {
	CatalogHash string   `json:"catalog_hash"`
	Target      string   `json:"target"`
	Environment string   `json:"environment"`
	Tenant      string   `json:"tenant"`
	Global      bool     `json:"include_global"`
	Contract    string   `json:"contract"`
	Actions     []Action `json:"actions"`
	Digest      string   `json:"digest"`
}

func contract(ctx context.Context, q queryer) (string, error) {
	queries := []string{"SELECT coalesce(jsonb_agg(to_jsonb(t)),'[]') FROM wahb_database_identity t", "SELECT coalesce(jsonb_agg(to_jsonb(t)),'[]') FROM wahb_database_writer_fence t", "SELECT coalesce(jsonb_agg(to_jsonb(t)),'[]') FROM database_migration_owner_control t", "SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY version),'[]') FROM cms_schema_migrations t"}
	all := []any{os.Getenv("CMS_DATABASE_CONTRACT_MODE")}
	for _, s := range queries {
		rs, e := q.QueryContext(ctx, s)
		if e != nil {
			return "", errors.New("owner contract evidence unavailable")
		}
		var b []byte
		for rs.Next() {
			e = rs.Scan(&b)
		}
		if e == nil {
			e = rs.Err()
		}
		rs.Close()
		if e != nil {
			return "", e
		}
		var v any
		if e = json.Unmarshal(b, &v); e != nil {
			return "", e
		}
		all = append(all, v)
	}
	ledger, ok := all[len(all)-1].([]any)
	if !ok {
		return "", errors.New("invalid CMS migration evidence")
	}
	required := false
	for _, entry := range ledger {
		record, ok := entry.(map[string]any)
		if ok && record["version"] == Migration {
			required = true
		}
	}
	if !required {
		return "", errors.New("required CMS reference-data migration is not applied")
	}
	return Hash(all), nil
}
func scopeKey(r Record, t string) string {
	if r.Scope == "global" {
		return "@global"
	}
	return t
}
func bindings(ctx context.Context, q queryer, tenant string) (map[string]Binding, error) {
	rs, e := q.QueryContext(ctx, "SELECT kind,catalog_key,scope_key,target_id,projection FROM reference_catalog_bindings WHERE catalog_name='wahb' AND scope_key IN ($1,'@global')", tenant)
	if e != nil {
		return nil, e
	}
	defer rs.Close()
	out := map[string]Binding{}
	for rs.Next() {
		var k, key, scope, id string
		var b []byte
		if e = rs.Scan(&k, &key, &scope, &id, &b); e != nil {
			return nil, e
		}
		var d map[string]any
		if e = json.Unmarshal(b, &d); e != nil {
			return nil, e
		}
		out[k+":"+scope+":"+key] = Binding{id, d}
	}
	return out, rs.Err()
}
func Preview(ctx context.Context, db *sql.DB, c Catalog, target, env, tenant string, global bool, decisions map[string]string) (Plan, error) {
	if err := Validate(c); err != nil {
		return Plan{}, err
	}
	tx, e := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if e != nil {
		return Plan{}, e
	}
	defer tx.Rollback()
	p, e := preview(ctx, tx, c, target, env, tenant, global, decisions)
	if e != nil {
		return p, e
	}
	return p, tx.Commit()
}
func preview(ctx context.Context, q queryer, c Catalog, target, env, tenant string, global bool, decisions map[string]string) (Plan, error) {
	knownDecisions := map[string]bool{}
	for _, r := range c.Records {
		knownDecisions[r.Kind+":"+r.Key] = true
	}
	for key, value := range decisions {
		if !knownDecisions[key] || !has([]string{"use-catalog", "keep-target"}, value) {
			return Plan{}, fmt.Errorf("unknown resolution key or decision: %s", key)
		}
	}
	p := Plan{CatalogHash: c.Digest(), Target: target, Environment: env, Tenant: tenant, Global: global, Actions: []Action{}}
	if tenant == "" || tenant == "@global" || len(tenant) > 64 || strings.ContainsAny(tenant, " \t\r\n/\\") {
		return p, errors.New("explicit tenant required")
	}
	var e error
	p.Contract, e = contract(ctx, q)
	if e != nil {
		return p, e
	}
	bs, e := bindings(ctx, q, tenant)
	if e != nil {
		return p, e
	}
	for _, r := range c.Records {
		if r.Scope == "global" && !global {
			return p, errors.New("catalog contains global profiles; explicit --include-global is required")
		}
		rs, e := load(ctx, q, r.Kind, tenant, r.Scope)
		if e != nil {
			return p, e
		}
		b, bound := bs[r.Kind+":"+scopeKey(r, tenant)+":"+r.Key]
		a := Action{Record: r, State: "create", BindingBefore: Hash(b)}
		var candidates []row
		for _, v := range rs {
			if bound {
				if fmt.Sprint(v["id"]) == b.ID {
					candidates = append(candidates, v)
				}
			} else if identity(r.Kind, project(r.Kind, v)) == identity(r.Kind, r.Definition) {
				candidates = append(candidates, v)
			}
		}
		if len(candidates) > 1 || (bound && len(candidates) == 0) {
			a.State = "conflict"
		} else if len(candidates) == 1 {
			v := candidates[0]
			a.ID = fmt.Sprint(v["id"])
			a.Before = project(r.Kind, v)
			if r.Kind == "news-sources" || r.Kind == "media-sources" {
				pk := ""
				if v["discovery_profile_id"] != nil {
					pk = "(missing discovery profile)"
				}
				profiles, err := load(ctx, q, "discovery-profiles", tenant, "tenant")
				if err != nil {
					return p, err
				}
				for _, pr := range profiles {
					if fmt.Sprint(pr["id"]) == fmt.Sprint(v["discovery_profile_id"]) {
						pk = keyFor("discovery-profiles", project("discovery-profiles", pr))
						for bk, bv := range bs {
							if strings.HasPrefix(bk, "discovery-profiles:"+tenant+":") && bv.ID == fmt.Sprint(pr["id"]) {
								pk = strings.TrimPrefix(bk, "discovery-profiles:"+tenant+":")
							}
						}
					}
				}
				a.Before["catalog_discovery_profile_key"] = pk
			}
			a.RowBefore = Hash(v)
			if Hash(a.Before) == Hash(desired(r)) {
				a.State = "adopt"
				if bound && Hash(b.Projection) == Hash(a.Before) {
					a.State = "unchanged"
				}
			} else if bound && Hash(a.Before) == Hash(b.Projection) && sameUpstream(r.Kind, a.Before, r.Definition) {
				a.State = "update"
			} else {
				a.State = "conflict"
			}
		}
		if a.State == "conflict" {
			switch decisions[r.Kind+":"+r.Key] {
			case "keep-target":
				a.State = "skip"
			case "use-catalog":
				if len(candidates) == 1 && sameUpstream(r.Kind, a.Before, r.Definition) {
					a.State = "update"
				}
			}
		}
		p.Actions = append(p.Actions, a)
	}
	known := map[string]bool{}
	for _, a := range p.Actions {
		known[a.Record.Kind+":"+scopeKey(a.Record, tenant)+":"+a.Record.Key] = true
	}
	keys := []string{}
	for key := range bs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if known[key] {
			continue
		}
		parts := strings.SplitN(key, ":", 3)
		if len(parts) != 3 {
			continue
		}
		if parts[1] == "@global" && !global {
			continue
		}
		scope := "tenant"
		if parts[1] == "@global" {
			scope = "global"
		}
		p.Actions = append(p.Actions, Action{Record: Record{Kind: parts[0], Key: parts[2], Scope: scope, Definition: bs[key].Projection}, State: "retained", ID: bs[key].ID, BindingBefore: Hash(bs[key])})
	}
	p.Digest = Hash(p)
	return p, nil
}
func HasConflicts(p Plan) bool {
	for _, a := range p.Actions {
		if a.State == "conflict" {
			return true
		}
	}
	return false
}
func WritePlan(path string, p Plan) error {
	if _, e := os.Lstat(path); !os.IsNotExist(e) {
		return errors.New("plan output already exists")
	}
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return e
	}
	return writeJSON(path, p)
}

func ReadPlan(path string) (Plan, error) {
	var p Plan
	b, e := os.ReadFile(path)
	if e == nil {
		e = Decode(b, &p)
	}
	return p, e
}
func Apply(ctx context.Context, db *sql.DB, c Catalog, p Plan, digest, actor string) (string, error) {
	if err := Validate(c); err != nil {
		return "", err
	}
	given := p.Digest
	p.Digest = ""
	if digest != given || Hash(p) != given || p.CatalogHash != c.Digest() || HasConflicts(p) {
		return "", errors.New("invalid catalog, digest or unresolved conflict")
	}
	p.Digest = given
	tx, e := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if e != nil {
		return "", e
	}
	defer tx.Rollback()
	var locked bool
	if e = tx.QueryRowContext(ctx, "SELECT pg_try_advisory_xact_lock(hashtext('wahb-local-environment-preparation/v1'))").Scan(&locked); e != nil || !locked {
		return "", errors.New("another owner operation is active")
	}
	for _, table := range []string{"reference_catalog_applications", "reference_catalog_bindings", "wahb_database_identity", "content_sources", "discovery_profiles", "topic_categories", "topics", "quality_profiles"} {
		if _, e = tx.ExecContext(ctx, "LOCK TABLE "+table+" IN SHARE ROW EXCLUSIVE MODE"); e != nil {
			return "", e
		}
	}
	// Capture timestamps and file ordering are provenance, not release definitions.
	receipts, err := tx.QueryContext(ctx, "SELECT evidence FROM reference_catalog_applications WHERE release_version=$1", c.Manifest.ReleaseVersion)
	if err != nil {
		return "", err
	}
	for receipts.Next() {
		var data []byte
		if err = receipts.Scan(&data); err != nil {
			receipts.Close()
			return "", err
		}
		var previous Plan
		if err = json.Unmarshal(data, &previous); err != nil {
			receipts.Close()
			return "", errors.New("invalid release evidence")
		}
		records := []Record{}
		for _, a := range previous.Actions {
			if a.State != "retained" {
				records = append(records, a.Record)
			}
		}
		if definitionDigest(records) != definitionDigest(c.Records) {
			receipts.Close()
			return "", errors.New("release version already applied with different catalog content")
		}
	}
	err = receipts.Err()
	receipts.Close()
	if err != nil {
		return "", err
	}

	var prior string
	e = tx.QueryRowContext(ctx, "SELECT operation_id::text FROM reference_catalog_applications WHERE plan_digest=$1", given).Scan(&prior)
	if e == nil {
		return prior, tx.Commit()
	}
	if e != sql.ErrNoRows {
		return "", e
	}
	var fence, control string
	var fenceEpoch int64
	if e = tx.QueryRowContext(ctx, "SELECT state,epoch FROM wahb_database_writer_fence WHERE singleton=true FOR SHARE").Scan(&fence, &fenceEpoch); e != nil {
		return "", errors.New("writer fence unavailable")
	}
	if e = tx.QueryRowContext(ctx, "SELECT state FROM database_migration_owner_control WHERE singleton=true FOR SHARE").Scan(&control); e != nil {
		return "", e
	}
	if (fence != "open" && fence != "successor_open") || control != "running" {
		return "", errors.New("database owner has fenced writes")
	}
	var epochs []int64
	identityRows, err := tx.QueryContext(ctx, "SELECT database_epoch FROM wahb_database_identity WHERE singleton=true FOR SHARE")
	if err != nil {
		return "", err
	}
	for identityRows.Next() {
		var epoch int64
		if err = identityRows.Scan(&epoch); err != nil {
			identityRows.Close()
			return "", err
		}
		epochs = append(epochs, epoch)
	}
	err = identityRows.Err()
	identityRows.Close()
	if err != nil {
		return "", err
	}
	if len(epochs) > 1 || (len(epochs) == 1 && epochs[0] != fenceEpoch) || (len(epochs) == 0 && strings.EqualFold(os.Getenv("CMS_DATABASE_CONTRACT_MODE"), "enforce")) {
		return "", errors.New("owner database identity/epoch does not authorize this application")
	}
	var claims int
	if e = tx.QueryRowContext(ctx, "SELECT count(*) FROM lifecycle_operation_claims WHERE state='active'").Scan(&claims); e != nil || claims > 0 {
		return "", errors.New("active or unavailable lifecycle claims")
	}
	decisions := map[string]string{}
	for _, a := range p.Actions {
		if a.State == "skip" {
			decisions[a.Record.Kind+":"+a.Record.Key] = "keep-target"
		} else if a.State == "update" {
			decisions[a.Record.Kind+":"+a.Record.Key] = "use-catalog"
		}
	}
	fresh, e := preview(ctx, tx, c, p.Target, p.Environment, p.Tenant, p.Global, decisions)
	if e != nil {
		return "", e
	}
	if fresh.Digest != p.Digest {
		return "", errors.New("stale preview; regenerate after stopping the stack")
	}
	mapping := map[string]string{}
	changed := false
	for _, a := range p.Actions {
		if a.Record.Kind == "discovery-profiles" && a.ID != "" && a.State != "skip" {
			mapping[a.Record.Key] = a.ID
		}
	}
	for _, a := range p.Actions {
		r := a.Record
		if a.State == "skip" || a.State == "retained" {
			continue
		}
		if a.State == "unchanged" {
			continue
		}
		if a.State == "conflict" {
			return "", errors.New("unresolved conflict")
		}
		changed = true
		d := map[string]any{}
		for k, v := range r.Definition {
			d[k] = v
		}
		if r.Kind == "news-sources" || r.Kind == "media-sources" {
			d["discovery_profile_id"] = nil
			if r.ProfileKey != "" {
				id, ok := mapping[r.ProfileKey]
				if !ok {
					return "", errors.New("discovery dependency was skipped or missing")
				}
				d["discovery_profile_id"] = json.Number(id)
			}
		}
		id := a.ID
		if a.State != "adopt" {
			var e error
			id, e = writeRecord(ctx, tx, r, p.Tenant, id, d, a.Before)
			if e != nil {
				return "", e
			}
		}
		if r.Kind == "discovery-profiles" {
			mapping[r.Key] = id
		}
		b, _ := json.Marshal(desired(r))
		_, e = tx.ExecContext(ctx, `INSERT INTO reference_catalog_bindings(catalog_name,scope_key,kind,catalog_key,target_id,projection,definition_hash,release_version) VALUES('wahb',$1,$2,$3,$4,$5::jsonb,$6,$7) ON CONFLICT(catalog_name,scope_key,kind,catalog_key) DO UPDATE SET target_id=excluded.target_id,projection=excluded.projection,definition_hash=excluded.definition_hash,release_version=excluded.release_version,updated_at=now()`, scopeKey(r, p.Tenant), r.Kind, r.Key, id, string(b), Hash(desired(r)), c.Manifest.ReleaseVersion)
		if e != nil {
			return "", e
		}
	}
	if !changed {
		return "unchanged", tx.Commit()
	}
	id := uuid.NewString()
	evidence, _ := json.Marshal(p)
	_, e = tx.ExecContext(ctx, `INSERT INTO reference_catalog_applications(operation_id,plan_digest,catalog_hash,release_version,target,tenant_id,actor,evidence) VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb)`, id, p.Digest, c.Digest(), c.Manifest.ReleaseVersion, p.Target, p.Tenant, actor, string(evidence))
	if e != nil {
		return "", e
	}
	return id, tx.Commit()
}
func writeRecord(ctx context.Context, tx *sql.Tx, r Record, tenant, id string, d map[string]any, before map[string]any) (string, error) {
	table := tables[r.Kind]
	if r.Kind == "topics" && str(d["category_slug"]) != "" {
		var exists bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM topic_categories WHERE tenant_id=$1 AND slug=$2)", tenant, d["category_slug"]).Scan(&exists); err != nil {
			return "", err
		}
		if !exists {
			return "", errors.New("topic category dependency unavailable")
		}
	}
	creating := id == ""
	if creating {
		if r.Scope == "tenant" {
			d["tenant_id"] = tenant
		}
		switch r.Kind {
		case "news-sources", "media-sources":
			d["is_active"] = false
		case "discovery-profiles":
			d["enabled"] = false
		case "quality-profiles":
			d["is_active"] = false
		case "topics":
			d["created_from"] = "manual"
			d["needs_remap"] = true
		}
	}
	if !creating {
		if r.Kind == "topics" && changedFields(before, d, []string{"slug", "label_ar", "label_en", "category_slug", "active"}) {
			d["needs_remap"] = true
			d["centroid"] = nil
			d["centroid_model"] = nil
			d["centroid_space_id"] = nil
			d["centroid_producer_id"] = nil
		}
		if r.Kind == "discovery-profiles" && changedFields(before, d, []string{"name", "description", "keywords"}) {
			for _, k := range []string{"embedding", "embedding_model", "embedding_space_id", "embedding_producer_id"} {
				d[k] = nil
			}
		}
	}
	keys := []string{}
	for k := range d {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	b, _ := json.Marshal(d)
	cols := strings.Join(keys, ",")
	query := "INSERT INTO " + table + " (" + cols + ") SELECT " + cols + " FROM jsonb_populate_record(NULL::" + table + ",$1::jsonb) RETURNING id::text"
	args := []any{string(b)}
	if !creating {
		sets := []string{}
		for _, k := range keys {
			if (r.Kind == "news-sources" || r.Kind == "media-sources") && (k == "api_config" || k == "metadata") {
				allowed := configFields
				if k == "metadata" {
					allowed = metadataFields
				}
				quoted := []string{}
				for _, key := range allowed {
					quoted = append(quoted, "'"+key+"'")
				}
				sets = append(sets, k+"=(coalesce(t."+k+",'{}'::jsonb)-ARRAY["+strings.Join(quoted, ",")+"]) || coalesce(v."+k+",'{}'::jsonb)")
			} else {
				sets = append(sets, k+"=v."+k)
			}
		}
		query = "UPDATE " + table + " t SET " + strings.Join(sets, ",") + ",updated_at=now() FROM jsonb_populate_record(NULL::" + table + ",$1::jsonb) v WHERE t.id=$2::bigint RETURNING t.id::text"
		args = append(args, id)
	}
	var out string
	e := tx.QueryRowContext(ctx, query, args...).Scan(&out)
	return out, e
}

func desired(r Record) map[string]any {
	d := map[string]any{}
	for k, v := range r.Definition {
		d[k] = v
	}
	if r.Kind == "news-sources" || r.Kind == "media-sources" {
		d["catalog_discovery_profile_key"] = r.ProfileKey
	}
	return d
}

func canonicalLocator(raw string) string {
	u, e := url.Parse(strings.TrimSpace(raw))
	if e != nil {
		return raw
	}
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	return u.String()
}

func changedFields(a, b map[string]any, fields []string) bool {
	for _, k := range fields {
		if Hash(a[k]) != Hash(b[k]) {
			return true
		}
	}
	return false
}

func sameUpstream(kind string, a, b map[string]any) bool {
	if kind != "news-sources" && kind != "media-sources" {
		return true
	}
	return identity(kind, a) == identity(kind, b)
}

func definitionDigest(records []Record) string {
	ordered := append([]Record(nil), records...)
	sort.Slice(ordered, func(i, j int) bool {
		a, b := ordered[i], ordered[j]
		return a.Kind+":"+a.Scope+":"+a.Key < b.Kind+":"+b.Scope+":"+b.Key
	})
	return Hash(ordered)
}
