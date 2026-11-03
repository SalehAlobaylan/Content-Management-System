package referencecatalog

import (
	"content-management-system/src/tests/testdb"
	"context"
	"database/sql"
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

func fixture() Catalog {
	c := Catalog{Manifest: Manifest{FormatVersion: 1, CatalogName: "wahb", ReleaseVersion: "test-1", RequiredMigration: Migration, Files: map[string]string{}, Origins: map[string]Origin{}}, Records: []Record{{Key: "news.example", Kind: "news-sources", Scope: "tenant", Definition: map[string]any{"name": "Example", "type": "RSS", "category": "news", "feed_url": "https://example.org/rss", "image_url": nil, "api_config": map[string]any{}, "metadata": map[string]any{}}}}}
	seal(&c)
	return c
}
func seal(c *Catalog) {
	for _, g := range Groups {
		rs := []Record{}
		for _, r := range c.Records {
			if r.Kind == g || (g == "news-discovery" && r.Kind == "discovery-profiles" && r.Definition["category"] == "news") || (g == "media-discovery" && r.Kind == "discovery-profiles" && r.Definition["category"] == "media") {
				rs = append(rs, r)
			}
		}
		c.Manifest.Files[g+".json"] = Hash(rs)
	}
	for _, g := range []string{"news-sources", "media-sources", "news-discovery", "media-discovery", "topic-categories", "topics", "quality-profiles"} {
		env := "stable"
		if strings.HasPrefix(g, "media-") {
			env = "dev"
		}
		rs := []Record{}
		for _, r := range c.Records {
			if r.Kind == g || (g == "news-discovery" && r.Kind == "discovery-profiles" && r.Definition["category"] == "news") || (g == "media-discovery" && r.Kind == "discovery-profiles" && r.Definition["category"] == "media") {
				rs = append(rs, r)
			}
		}
		sort.Slice(rs, func(i, j int) bool { return rs[i].Key < rs[j].Key })
		c.Manifest.Origins[g] = Origin{Environment: env, Tenant: "default", ProjectionHash: Hash(rs)}
	}
}
func TestValidation(t *testing.T) {
	c := fixture()
	if e := Validate(c); e != nil {
		t.Fatal(e)
	}
	for _, input := range []string{`{"a":1,"a":2}`, `{"a":{"x":1,"x":2}}`, `{} {}`} {
		var v map[string]any
		if Decode([]byte(input), &v) == nil {
			t.Fatal("accepted duplicate/trailing JSON")
		}
	}
	c.Manifest.Origins["media-sources"] = Origin{Environment: "stable", Tenant: "default", ProjectionHash: Hash([]Record{})}
	if Validate(c) == nil {
		t.Fatal("accepted wrong media origin")
	}
	c = fixture()
	c.Records[0].Definition["feed_url"] = "https://user:secret@example.org/feed"
	if Validate(c) == nil {
		t.Fatal("accepted URL secret")
	}
	c = fixture()
	c.Records[0].Definition["is_active"] = true
	if Validate(c) == nil {
		t.Fatal("accepted runtime state")
	}
}
func TestRoundTrip(t *testing.T) {
	c := fixture()
	dir := filepath.Join(t.TempDir(), "catalog")
	if e := Write(dir, c); e != nil {
		t.Fatal(e)
	}
	a, e := Read(dir)
	if e != nil {
		t.Fatal(e)
	}
	if Hash(a.Records) != Hash(c.Records) {
		t.Fatal("round trip changed definitions")
	}
	if Write(dir, c) == nil {
		t.Fatal("overwrote catalog")
	}
}
func TestProjectionPreservesSeparation(t *testing.T) {
	r := row{"name": "a", "is_active": true, "last_fetched_at": "yesterday", "api_config": map[string]any{"username": "abc", "private_token": "secret"}, "metadata": map[string]any{"media_finding_health": true}}
	p := project("news-sources", r)
	if p["is_active"] != nil || p["last_fetched_at"] != nil {
		t.Fatal("operational data projected")
	}
	if len(p["api_config"].(map[string]any)) != 1 {
		t.Fatal("unclassified JSON projected")
	}
}
func TestApplyDatabase(t *testing.T) {
	if os.Getenv("CMS_TEST_ADMIN_URL") == "" {
		t.Skip("guarded random-database qualification requires CMS_TEST_ADMIN_URL")
	}
	db := testdb.Open(t)
	sqlDB, e := db.DB()
	if e != nil {
		t.Fatal(e)
	}
	var name string
	sqlDB.QueryRow("select current_database()").Scan(&name)
	u, _ := url.Parse(os.Getenv("CMS_TEST_ADMIN_URL"))
	u.Path = "/" + name
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	root, e := filepath.Abs("../..")
	if e != nil {
		t.Fatal(e)
	}
	cmd := exec.CommandContext(ctx, "go", "run", "./cmd/migrate", "--all", "--bootstrap-empty", "--allow-destructive")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "DATABASE_URL="+u.String(), "WAHB_MANAGED_ENV=1", "CMS_MIGRATION_BOOTSTRAP_DISPOSABLE=I_UNDERSTAND_THIS_DATABASE_IS_DISPOSABLE")
	if out, e := cmd.CombinedOutput(); e != nil {
		t.Fatalf("canonical fixture migration failed: %v (%d log bytes withheld)", e, len(out))
	}
	var admission string
	if err := sqlDB.QueryRow("SELECT epoch FROM source_run_admission_protocol WHERE protocol_key='source-run/v1'").Scan(&admission); err != nil || admission != "durable_required" {
		t.Fatal("fresh bootstrap left source admission inactive", admission, err)
	}
	var provisioned int
	if err := sqlDB.QueryRow("SELECT count(*) FROM source_run_admission_cutovers WHERE tenant_id='default' AND lane IN ('news','media') AND mode='durable' AND protocol='source-run/v1'").Scan(&provisioned); err != nil || provisioned != 2 {
		t.Fatal("fresh bootstrap did not provision both source lanes", provisioned, err)
	}
	c := fixture()
	p, e := Preview(ctx, sqlDB, c, Target(u.String()), "test", "default", true, nil)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = Apply(ctx, sqlDB, c, p, p.Digest, "test"); e != nil {
		t.Fatal(e)
	}
	var active bool
	var due sql.NullTime
	var version int64
	if e = sqlDB.QueryRow("select is_active,next_due_at,source_config_version from content_sources where name='Example'").Scan(&active, &due, &version); e != nil {
		t.Fatal(e)
	}
	if active || due.Valid {
		t.Fatal("import activated source")
	}
	p, e = Preview(ctx, sqlDB, c, Target(u.String()), "test", "default", true, nil)
	if e != nil {
		t.Fatal(e)
	}
	if p.Actions[0].State != "unchanged" {
		b, _ := json.Marshal(p.Actions)
		t.Fatalf("not unchanged: %s", b)
	}
	if id, e := Apply(ctx, sqlDB, c, p, p.Digest, "test"); e != nil || id != "unchanged" {
		t.Fatal(id, e)
	}
	sqlDB.Exec(`update content_sources set is_active=true,fetch_interval_minutes=123,failure_streak=7,metadata='{"environment_only":"preserve"}'::jsonb where name='Example'`)
	p, e = Preview(ctx, sqlDB, c, Target(u.String()), "test", "default", true, nil)
	if e != nil || p.Actions[0].State != "unchanged" {
		t.Fatal("operational state became drift", e)
	}
	c.Manifest.ReleaseVersion = "test-2"
	c.Records[0].Definition["name"] = "Renamed"
	seal(&c)
	p, e = Preview(ctx, sqlDB, c, Target(u.String()), "test", "default", true, nil)
	if e != nil {
		t.Fatal(e)
	}
	sqlDB.Exec("update content_sources set name='Local edit' where name='Example'")
	if _, e = Apply(ctx, sqlDB, c, p, p.Digest, "test"); e == nil {
		t.Fatal("stale preview applied")
	}
	p, e = Preview(ctx, sqlDB, c, Target(u.String()), "test", "default", true, nil)
	if e != nil || !HasConflicts(p) {
		t.Fatal("local edit not a conflict", e)
	}
	p, e = Preview(ctx, sqlDB, c, Target(u.String()), "test", "default", true, map[string]string{"news-sources:news.example": "use-catalog"})
	if e != nil {
		t.Fatal(e)
	}
	var beforeVersion, afterVersion int64
	sqlDB.QueryRow("select source_config_version from content_sources where name='Local edit'").Scan(&beforeVersion)
	if _, e = Apply(ctx, sqlDB, c, p, p.Digest, "test"); e != nil {
		t.Fatal(e)
	}
	sqlDB.QueryRow("select source_config_version from content_sources where name='Renamed'").Scan(&afterVersion)
	if afterVersion != beforeVersion+1 {
		t.Fatal("source version did not advance exactly once")
	}
	var interval, failures int
	sqlDB.QueryRow("select is_active,fetch_interval_minutes,failure_streak from content_sources where name='Renamed'").Scan(&active, &interval, &failures)
	var md string
	sqlDB.QueryRow("select metadata->>'environment_only' from content_sources where name='Renamed'").Scan(&md)
	if md != "preserve" {
		t.Fatal("unmanaged JSON was overwritten")
	}
	if !active || interval != 123 || failures != 7 {
		t.Fatal("lost environment state")
	}

	published, err := Read(filepath.Join(root, "reference-data"))
	if err != nil {
		t.Fatal(err)
	}
	choices := map[string]string{}
	for _, r := range published.Records {
		if r.Kind == "topics" && r.Definition["slug"] == "politics" {
			choices[r.Kind+":"+r.Key] = "keep-target"
		}
	}
	realPlan, err := Preview(ctx, sqlDB, published, Target(u.String()), "test", "default", true, choices)
	if err != nil {
		t.Fatal(err)
	}
	if HasConflicts(realPlan) {
		for _, a := range realPlan.Actions {
			if a.State == "conflict" {
				t.Log(a.Record.Kind, a.Record.Key, a.Before, a.Record.Definition)
			}
		}
		t.Fatal("unexpected published catalog conflict")
	}
	if _, err = Apply(ctx, sqlDB, published, realPlan, realPlan.Digest, "test"); err != nil {
		t.Fatal(err)
	}
	var enabled, dueCount int
	if err = sqlDB.QueryRow("select count(*) filter(where is_active),count(*) filter(where next_due_at is not null) from content_sources where name<>'Renamed'").Scan(&enabled, &dueCount); err != nil {
		t.Fatal(err)
	}
	if enabled != 0 || dueCount != 0 {
		t.Fatal("published sources scheduled")
	}
	again, err := Preview(ctx, sqlDB, published, Target(u.String()), "test", "default", true, choices)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range again.Actions {
		if a.State != "unchanged" && a.State != "skip" && a.State != "retained" {
			t.Fatalf("published rerun not no-op: %s %s %s", a.Record.Kind, a.Record.Key, a.State)
		}
	}

	for _, table := range []string{"discovery_profiles", "quality_profiles"} {
		var on int
		column := "enabled"
		if table == "quality_profiles" {
			column = "is_active"
		}
		if e = sqlDB.QueryRow("SELECT count(*) FROM " + table + " WHERE " + column).Scan(&on); e != nil || on != 0 {
			t.Fatal("new execution profile active", table, e)
		}
	}

	linked := fixture()
	linked.Manifest.ReleaseVersion = "linked-1"
	linked.Records[0].Definition["name"] = "Renamed"
	linked.Records[0].Definition["api_config"] = map[string]any{"min_text_length": float64(3)}
	for _, r := range published.Records {
		if r.Kind == "discovery-profiles" {
			linked.Records[0].ProfileKey = r.Key
			linked.Records = append(linked.Records, r)
			break
		}
	}
	seal(&linked)
	lp, err := Preview(ctx, sqlDB, linked, Target(u.String()), "test", "default", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Apply(ctx, sqlDB, linked, lp, lp.Digest, "test"); err != nil {
		t.Fatal(err)
	}
	linked.Manifest.ReleaseVersion = "linked-2"
	linked.Records = linked.Records[:1]
	linked.Records[0].ProfileKey = ""
	linked.Records[0].Definition["api_config"] = map[string]any{}
	seal(&linked)
	lp, err = Preview(ctx, sqlDB, linked, Target(u.String()), "test", "default", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Apply(ctx, sqlDB, linked, lp, lp.Digest, "test"); err != nil {
		t.Fatal(err)
	}
	var removed, unlinked bool
	if err = sqlDB.QueryRow("select NOT(api_config ? 'min_text_length'), discovery_profile_id IS NULL from content_sources where name='Renamed'").Scan(&removed, &unlinked); err != nil || !removed || !unlinked {
		t.Fatal("shared setting or profile link was not removed", err)
	}
	// Rebuilding identical definitions with fresh capture provenance is a no-op.
	for key, o := range linked.Manifest.Origins {
		o.CapturedAt = "2030-01-01T00:00:00Z"
		linked.Manifest.Origins[key] = o
	}
	lp, err = Preview(ctx, sqlDB, linked, Target(u.String()), "test", "default", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if id, err := Apply(ctx, sqlDB, linked, lp, lp.Digest, "test"); err != nil || id != "unchanged" {
		t.Fatal("capture provenance blocked unchanged rebuild", id, err)
	}

	previousMode := os.Getenv("CMS_DATABASE_CONTRACT_MODE")
	t.Setenv("CMS_DATABASE_CONTRACT_MODE", "enforce")
	enforced, err := Preview(ctx, sqlDB, linked, Target(u.String()), "test", "default", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Apply(ctx, sqlDB, linked, enforced, enforced.Digest, "test"); err == nil {
		t.Fatal("enforcement allowed an unidentified database")
	}
	t.Setenv("CMS_DATABASE_CONTRACT_MODE", previousMode)
	var ledgerRow []byte
	if err = sqlDB.QueryRow("DELETE FROM cms_schema_migrations WHERE version=$1 RETURNING to_jsonb(cms_schema_migrations)", Migration).Scan(&ledgerRow); err != nil {
		t.Fatal(err)
	}
	if _, err = Preview(ctx, sqlDB, linked, Target(u.String()), "test", "default", true, nil); err == nil {
		t.Fatal("missing required migration was not rejected")
	}
	if _, err = sqlDB.Exec("INSERT INTO cms_schema_migrations SELECT * FROM jsonb_populate_record(NULL::cms_schema_migrations,$1::jsonb)", string(ledgerRow)); err != nil {
		t.Fatal(err)
	}
	concurrent := fixture()
	concurrent.Manifest.ReleaseVersion = "concurrent-1"
	concurrent.Records[0].Key = "news.concurrent"
	concurrent.Records[0].Definition["feed_url"] = "https://example.org/concurrent"
	seal(&concurrent)
	cp, err := Preview(ctx, sqlDB, concurrent, Target(u.String()), "test", "default", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { _, err := Apply(ctx, sqlDB, concurrent, cp, cp.Digest, "test"); results <- err }()
	}
	aerr, berr := <-results, <-results
	if aerr != nil && berr != nil {
		t.Fatal("both concurrent attempts failed", aerr, berr)
	}
	var n int
	sqlDB.QueryRow("select count(*) from content_sources where feed_url='https://example.org/concurrent'").Scan(&n)
	if n != 1 {
		t.Fatal("concurrent duplicate", n)
	}
	sqlDB.Exec(`CREATE FUNCTION reject_catalog_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected audit failure'; END $$; CREATE TRIGGER reject_catalog_receipt BEFORE INSERT ON reference_catalog_applications FOR EACH ROW EXECUTE FUNCTION reject_catalog_receipt()`)
	failed := fixture()
	failed.Manifest.ReleaseVersion = "audit-failure"
	failed.Records[0].Key = "news.failed"
	failed.Records[0].Definition["feed_url"] = "https://example.org/failed"
	seal(&failed)
	fp, err := Preview(ctx, sqlDB, failed, Target(u.String()), "test", "default", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Apply(ctx, sqlDB, failed, fp, fp.Digest, "test"); err == nil {
		t.Fatal("audit failure committed")
	}
	sqlDB.QueryRow("select count(*) from content_sources where feed_url='https://example.org/failed'").Scan(&n)
	if n != 0 {
		t.Fatal("partial application survived rollback")
	}
	if _, err = Preview(ctx, sqlDB, published, Target(u.String()), "test", "default", false, nil); err == nil {
		t.Fatal("global write scope inferred")
	}

	other := testdb.Open(t)
	otherSQL, _ := other.DB()
	var otherName string
	otherSQL.QueryRow("select current_database()").Scan(&otherName)
	otherURL := *u
	otherURL.Path = "/" + otherName
	migrateOther := exec.CommandContext(ctx, "go", "run", "./cmd/migrate", "--all", "--bootstrap-empty", "--allow-destructive")
	migrateOther.Dir = root
	migrateOther.Env = append(os.Environ(), "DATABASE_URL="+otherURL.String(), "WAHB_MANAGED_ENV=1", "CMS_MIGRATION_BOOTSTRAP_DISPOSABLE=I_UNDERSTAND_THIS_DATABASE_IS_DISPOSABLE")
	if out, err := migrateOther.CombinedOutput(); err != nil {
		t.Fatalf("second canonical fixture failed: %v (%d log bytes withheld)", err, len(out))
	}
	otherSQL.Exec("select setval(pg_get_serial_sequence('content_sources','id'),1000)")
	otherPlan, err := Preview(ctx, otherSQL, published, Target(otherURL.String()), "other", "default", true, choices)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Apply(ctx, otherSQL, published, otherPlan, otherPlan.Digest, "test"); err != nil {
		t.Fatal(err)
	}
	var minID int
	otherSQL.QueryRow("select min(id) from content_sources").Scan(&minID)
	if minID <= 1000 {
		t.Fatal("catalog copied destination IDs")
	}
	var broken int
	otherSQL.QueryRow("select count(*) from content_sources s left join discovery_profiles p on p.id=s.discovery_profile_id where s.discovery_profile_id is not null and p.id is null").Scan(&broken)
	if broken != 0 {
		t.Fatal("catalog copied foreign references")
	}

	// An origin link outside its selected category must not silently disappear.
	var mediaProfileID, originSourceID int64
	if err = otherSQL.QueryRow("insert into discovery_profiles(tenant_id,name,category,enabled) values('default','Media-only interest','media',false) returning id").Scan(&mediaProfileID); err != nil {
		t.Fatal(err)
	}
	if err = otherSQL.QueryRow("select id from content_sources where category='news' and discovery_profile_id is null limit 1").Scan(&originSourceID); err != nil {
		t.Fatal(err)
	}
	if _, err = otherSQL.Exec("update content_sources set discovery_profile_id=$1 where id=$2", mediaProfileID, originSourceID); err != nil {
		t.Fatal(err)
	}
	if _, err = Export(ctx, otherURL.String(), otherURL.String(), "default", "export-test"); err == nil {
		t.Fatal("export silently dropped cross-category discovery relation")
	}
	wrong := p
	wrong.Target = "other"
	if _, e = Apply(ctx, sqlDB, c, wrong, p.Digest, "test"); e == nil {
		t.Fatal("tampered target applied")
	}
}

func TestPolishedValidation(t *testing.T) {
	for _, value := range []any{nil, "not-a-url", "http://[::1]/rss", "http://10.0.0.1/feed"} {
		c := fixture()
		c.Records[0].Definition["feed_url"] = value
		seal(&c)
		if Validate(c) == nil {
			t.Fatalf("accepted invalid upstream locator %v", value)
		}
	}
	for _, value := range []any{nil, 2.5, -1.0} {
		c := fixture()
		c.Records[0].Definition["api_config"] = map[string]any{"max_results": value}
		seal(&c)
		if Validate(c) == nil {
			t.Fatal("accepted invalid capacity value")
		}
	}
	c := fixture()
	duplicate := c.Records[0]
	duplicate.Key = "another-key"
	c.Records = append(c.Records, duplicate)
	seal(&c)
	if Validate(c) == nil {
		t.Fatal("two keys can create the same source")
	}
	c = fixture()
	c.Records[0].Definition["is_active"] = true
	dir := filepath.Join(t.TempDir(), "invalid")
	if Write(dir, c) == nil {
		t.Fatal("invalid catalog written")
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("invalid build left publishable artifacts")
	}
}

func TestCheckedInSchemaMatchesOwner(t *testing.T) {
	b, err := os.ReadFile("../../reference-data/schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err = Decode(b, &schema); err != nil {
		t.Fatal(err)
	}
	if Hash(schema) != Hash(Schema()) {
		t.Fatal("checked-in schema is stale; regenerate with reference-data schema")
	}
}
