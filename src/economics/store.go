package economics

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"content-management-system/src/utils"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrConflict = errors.New("evidence conflicts with an accepted revision")
var ErrDisabled = errors.New("economics collection is disabled")

func Available(db *gorm.DB) bool {
	var name *string
	return db.Raw("SELECT to_regclass('economics_policies')::text").Scan(&name).Error == nil && name != nil
}

// The shared lock is taken only by background/owner transactions and explicit lifecycle actions.
func Lock(tx *gorm.DB) error {
	fence, e := utils.ReadWriterFence(tx)
	if e != nil {
		return e
	}
	if fence.State != "open" && fence.State != "successor_open" && fence.State != "compatibility_open" {
		return fmt.Errorf("database writers are fenced")
	}
	return tx.Exec("SELECT pg_advisory_xact_lock(731903186)").Error
}
func SubjectHash(db *gorm.DB, subject string) (string, error) {
	key, e := lifecycleKey(db)
	if e != nil {
		return "", e
	}
	h := hmac.New(sha256.New, key)
	h.Write([]byte("economics-erasure/v1:" + subject))
	return hex.EncodeToString(h.Sum(nil)), nil
}
func ResolveSubject(tx *gorm.DB, subject string, at time.Time) (string, bool, error) {
	if !strings.HasPrefix(subject, "account:") {
		return subject, false, nil
	}
	hash, e := SubjectHash(tx, subject)
	if e != nil {
		return "", false, e
	}
	var barrier struct {
		Erased    bool
		ClearedAt *time.Time
	}
	e = tx.Table("economics_lifecycle_barriers").Where("subject_hash=?", hash).Take(&barrier).Error
	if errors.Is(e, gorm.ErrRecordNotFound) {
		return subject, false, nil
	}
	if e != nil {
		return "", false, e
	}
	if barrier.Erased {
		return "removed", true, nil
	}
	return subject, barrier.ClearedAt != nil && !at.After(*barrier.ClearedAt), nil
}
func Ingest(db *gorm.DB, source string, b Batch) error {
	db = privateDB(db)
	if b.Environment != EnvironmentName() || b.Version != 1 || len(b.Rows) > MaxRows || len(b.Rows) == 0 && b.Lost == 0 && !b.Health && !b.Closed || b.Sequence < 1 || len(b.Epoch) > 96 || b.Epoch == "" || len(b.TenantID) > 64 || b.TenantID == "" || b.Lost < 0 || b.EndedAt.Before(b.StartedAt) || b.EndedAt.After(time.Now().Add(5*time.Minute)) || b.StartedAt.Before(time.Now().AddDate(0, 0, -90)) {
		return fmt.Errorf("invalid usage envelope")
	}
	bytes, _ := json.Marshal(b)
	if len(bytes) > MaxEnvelope {
		return fmt.Errorf("usage envelope too large")
	}
	sum := sha256.Sum256(bytes)
	hash := hex.EncodeToString(sum[:])
	return db.Transaction(func(tx *gorm.DB) error {
		if e := Lock(tx); e != nil {
			return e
		}
		var p Policy
		if e := tx.Where("tenant_id=?", b.TenantID).Take(&p).Error; e != nil {
			return e
		}
		if !p.CollectionEnabled {
			return ErrDisabled
		}
		var state struct {
			Sequence int64
			LastHash string
			Closed   bool
		}
		e := tx.Table("economics_collection_state").Where("tenant_id=? AND source=? AND epoch=?", b.TenantID, source, b.Epoch).Take(&state).Error
		if e != nil && !errors.Is(e, gorm.ErrRecordNotFound) {
			return e
		}
		if e == nil {
			if b.Sequence == state.Sequence && hash == state.LastHash {
				return nil
			}
			if state.Closed || b.Sequence <= state.Sequence || b.Sequence != state.Sequence+1 {
				return ErrConflict
			}
		} else if b.Sequence != 1 {
			return ErrConflict
		}
		subjects := make([]string, 0, len(b.Rows))
		for _, r := range b.Rows {
			subjects = append(subjects, r.Subject)
		}
		barriers, e := loadSubjectBarriers(tx, subjects)
		if e != nil {
			return e
		}
		rows := make([]Usage, 0, len(b.Rows))
		for _, row := range b.Rows {
			if _, ok := Operations[row.Operation]; !ok || !Units[row.Unit] || row.Value < 0 || row.Value > 1e15 || len(row.Subject) > 160 || len(row.Variant) > 32 || row.Day.Before(b.StartedAt.UTC().Truncate(24*time.Hour)) || row.Day.After(b.EndedAt.UTC().Truncate(24*time.Hour)) {
				return fmt.Errorf("invalid usage row")
			}
			if !ValidSubject(row.Subject) {
				return fmt.Errorf("invalid subject")
			}
			row.Subject, _ = barriers.Resolve(row.Subject, b.EndedAt)
			row.TenantID = b.TenantID
			rows = append(rows, row)
		}
		if e := addUsage(tx, rows); e != nil {
			return e
		}
		return tx.Exec(`INSERT INTO economics_collection_state(tenant_id,source,epoch,sequence,last_hash,started_at,last_seen_at,lost,closed) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(tenant_id,source,epoch) DO UPDATE SET sequence=EXCLUDED.sequence,last_hash=EXCLUDED.last_hash,last_seen_at=EXCLUDED.last_seen_at,lost=economics_collection_state.lost+EXCLUDED.lost,closed=EXCLUDED.closed`, b.TenantID, source, b.Epoch, b.Sequence, hash, b.StartedAt, b.EndedAt, b.Lost, b.Closed).Error
	})
}
func ValidSubject(s string) bool {
	if s == "public" || s == "system" || s == "admin" || s == "unknown" || s == "removed" || s == "synthetic" {
		return true
	}
	if !strings.HasPrefix(s, "account:") {
		return false
	}
	_, e := uuid.Parse(strings.TrimPrefix(s, "account:"))
	return e == nil
}
func RegisterAttribution(tx *gorm.DB, a Attribution) error {
	tx = privateDB(tx)
	if !ValidSubject(a.Subject) || a.ID == "" || a.TenantID == "" || a.Source == "" || Operations[a.Feature] == "" && a.Feature != "transcription" && a.Feature != "upload" && a.Feature != "auth" && a.Feature != "pods" && a.Feature != "news" {
		return fmt.Errorf("invalid attribution")
	}
	if a.Benefit != "private" && a.Benefit != "shared_content" {
		return fmt.Errorf("invalid benefit")
	}
	if e := Lock(tx); e != nil {
		return e
	}
	if a.CreatedAt.IsZero() {
		a.CreatedAt = time.Now().UTC()
	}
	subject, cleared, e := ResolveSubject(tx, a.Subject, a.CreatedAt)
	if e != nil {
		return e
	}
	a.Subject = subject
	if cleared && subject != "removed" {
		a.Subject = "cleared"
	}
	if subject == "removed" {
		a.ContentID = ""
	}
	if a.CreatedAt.IsZero() {
		a.CreatedAt = time.Now().UTC()
	}
	if a.OwnerSubject != "" {
		owner, _, e := ResolveSubject(tx, a.OwnerSubject, a.CreatedAt)
		if e != nil {
			return e
		}
		a.OwnerSubject = owner
	}
	var old Attribution
	e = tx.Where("id=?", a.ID).Take(&old).Error
	if e == nil {
		if old.TenantID != a.TenantID || old.Source != a.Source || old.Subject != a.Subject || old.Benefit != a.Benefit || old.ContentID != a.ContentID {
			return ErrConflict
		}
		return nil
	}
	if !errors.Is(e, gorm.ErrRecordNotFound) {
		return e
	}
	return tx.Create(&a).Error
}
func ValidateCost(c *Cost) error {
	if !ValidResource(c.Resource) || c.TenantID == "" || len(c.LogicalKey) > 160 || c.LogicalKey == "" || len(c.SourceID) > 160 || c.SourceID == "" || len(c.Reason) > 2000 || c.Reason == "" || c.PeriodStart.IsZero() || !c.PeriodStart.Equal(c.PeriodStart.UTC().Truncate(24*time.Hour)) || !c.PeriodEnd.Equal(c.PeriodEnd.UTC().Truncate(24*time.Hour)) || !c.PeriodEnd.After(c.PeriodStart) || c.PeriodEnd.Sub(c.PeriodStart) > 32*24*time.Hour || c.Currency != "USD" || c.AllocableBPS < 0 || c.AllocableBPS > 10000 {
		return fmt.Errorf("invalid cost: use USD, a period up to 32 days, resource, source, stable line key and reason")
	}
	a, e := ParseAmount(string(c.Net))
	if e != nil {
		return e
	}
	c.Net = a
	if c.BeforeBenefits != nil {
		a, e := ParseAmount(string(*c.BeforeBenefits))
		if e != nil {
			return e
		}
		c.BeforeBenefits = &a
	}
	switch c.Destination {
	case "direct":
		if !strings.HasPrefix(c.Subject, "account:") || !ValidSubject(c.Subject) {
			return fmt.Errorf("direct cost requires a verified account reference")
		}
	case "infrastructure", "content", "overhead", "unallocated", "reconciliation":
	default:
		return fmt.Errorf("invalid cost destination")
	}
	if c.Destination == "content" && c.ContentID == "" {
		return fmt.Errorf("content pool requires canonical content")
	}
	if c.Destination == "infrastructure" && c.Metric != "requests" && c.Metric != "db_work" && c.Metric != "work_ms" && c.Metric != "byte_seconds" {
		return fmt.Errorf("unsupported allocation metric")
	}
	return nil
}
func AppendCost(db *gorm.DB, c Cost) (Cost, error) {
	db = privateDB(db)
	if e := ValidateCost(&c); e != nil {
		return c, e
	}
	c.ID = ""
	c.Revision = 0
	c.CreatedAt = time.Time{}
	c.Checksum = ""
	checksum := financialFingerprint(c)

	e := db.Transaction(func(tx *gorm.DB) error {
		if e := Lock(tx); e != nil {
			return e
		}
		subject, cleared, e := ResolveSubject(tx, c.Subject, c.PeriodStart)
		if e != nil {
			return e
		}
		if cleared && subject != "removed" {
			subject = "cleared"
		}
		c.Subject = subject
		if subject == "removed" || subject == "cleared" {
			c.Destination = "unallocated"
		}
		var old Cost
		e = tx.Where("tenant_id=? AND source_id=? AND logical_key=?", c.TenantID, c.SourceID, c.LogicalKey).Order("revision DESC").Take(&old).Error
		if e != nil && !errors.Is(e, gorm.ErrRecordNotFound) {
			return e
		}
		if e == nil && old.Subject == "expired" {
			c.Subject = "expired"
		}
		if e == nil && old.Checksum == checksum && old.Subject == c.Subject && old.ContentID == c.ContentID {
			c = old
			return nil
		}
		c.Revision = old.Revision + 1
		c.Checksum = checksum
		c.ID = uuid.NewString()
		c.CreatedAt = time.Now().UTC()
		return tx.Create(&c).Error
	})
	return c, e
}

// Erase and Clear run in the caller's business transaction, before it acknowledges lifecycle completion.
func Erase(tx *gorm.DB, account string) error { return EraseMany(tx, []string{account}) }
func EraseMany(tx *gorm.DB, accounts []string) error {
	tx = privateDB(tx)
	if len(accounts) == 0 || !Available(tx) {
		return nil
	}
	if len(accounts) > 100 {
		return fmt.Errorf("erasure batch exceeds lifecycle bound")
	}
	if e := Lock(tx); e != nil {
		return e
	}
	subjects := []string{}
	var barriers []struct {
		SubjectHash string
		Erased      bool
		UpdatedAt   time.Time
	}
	for _, id := range accounts {
		if _, e := uuid.Parse(id); e != nil {
			return e
		}
		subject := "account:" + id
		hash, e := SubjectHash(tx, subject)
		if e != nil {
			return e
		}
		subjects = append(subjects, subject)
		barriers = append(barriers, struct {
			SubjectHash string
			Erased      bool
			UpdatedAt   time.Time
		}{hash, true, time.Now().UTC()})
	}
	if e := tx.Table("economics_lifecycle_barriers").Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "subject_hash"}}, DoUpdates: clause.AssignmentColumns([]string{"erased", "updated_at"})}).CreateInBatches(barriers, 100).Error; e != nil {
		return e
	}
	for _, table := range []string{"economics_usage_daily", "economics_usage_monthly"} {
		if e := tx.Exec(`INSERT INTO `+table+` SELECT tenant_id,day,'removed',operation,unit,variant,sum(value) FROM `+table+` WHERE subject IN ? GROUP BY tenant_id,day,operation,unit,variant ON CONFLICT(tenant_id,day,subject,operation,unit,variant) DO UPDATE SET value=`+table+`.value+EXCLUDED.value`, subjects).Error; e != nil {
			return e
		}
		if e := tx.Table(table).Where("subject IN ?", subjects).Delete(&Usage{}).Error; e != nil {
			return e
		}
	}
	for _, q := range []string{`UPDATE economics_cost_summaries SET subject='removed' WHERE subject IN ?`, `UPDATE economics_artifact_snapshots SET owner_subject='removed' WHERE owner_subject IN ?`, `UPDATE economics_cost_revisions SET subject='removed' WHERE subject IN ?`, `UPDATE economics_attributions SET subject='removed',content_id='' WHERE subject IN ?`, `UPDATE economics_attributions SET owner_subject='removed' WHERE owner_subject IN ?`, `DELETE FROM economics_diagnostic_examples WHERE subject IN ?`, `DELETE FROM economics_issues WHERE scope IN ?`} {
		if e := tx.Exec(q, subjects).Error; e != nil {
			return e
		}
	}
	if e := anonymizeSubjectsWeights(tx, subjects, "removed"); e != nil {
		return e
	}
	return invalidatePublications(tx)
}
func invalidatePublications(tx *gorm.DB) error {
	// No private staged snapshot may survive a history clear or account erasure.
	var supported bool
	if e := tx.Raw("SELECT to_regclass('economics_projection_jobs') IS NOT NULL").Scan(&supported).Error; e != nil {
		return e
	}
	if supported {
		if e := tx.Exec("UPDATE economics_runs SET status='superseded',completed_at=now() WHERE id IN(SELECT id FROM economics_projection_jobs)").Error; e != nil {
			return e
		}
		if e := tx.Exec("DELETE FROM economics_projection_jobs").Error; e != nil {
			return e
		}
	}
	// Keep the report's real as_of; only identity visibility changed.
	return tx.Exec(`UPDATE economics_publications SET revision=?`, uuid.NewString()).Error
}
func Clear(tx *gorm.DB, account string, now time.Time) error {
	tx = privateDB(tx)
	if !Available(tx) {
		return nil
	}
	if e := Lock(tx); e != nil {
		return e
	}
	subject := "account:" + account
	hash, e := SubjectHash(tx, subject)
	if e != nil {
		return e
	}
	if e = tx.Exec(`INSERT INTO economics_lifecycle_barriers(subject_hash,cleared_at) VALUES(?,?) ON CONFLICT(subject_hash) DO UPDATE SET cleared_at=EXCLUDED.cleared_at,updated_at=now()`, hash, now).Error; e != nil {
		return e
	}
	if e := anonymizeWeights(tx, subject, "cleared"); e != nil {
		return e
	}
	if e := retainClearedResourceTotals(tx, subject); e != nil {
		return e
	}
	for _, q := range []string{`UPDATE economics_attributions SET subject='cleared' WHERE subject=?`,
		`UPDATE economics_cost_revisions SET subject='cleared' WHERE subject=?`, `DELETE FROM economics_diagnostic_examples WHERE subject=?`} {
		if e := tx.Exec(q, subject).Error; e != nil {
			return e
		}
	}
	return invalidatePublications(tx)
}
func UpsertSource(db *gorm.DB, s Source) error {
	return db.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "id"}}, DoUpdates: clause.AssignmentColumns([]string{"status", "note", "updated_at", "config", "capability"})}).Create(&s).Error
}

// LinkReceipt stores only an opaque reference; legacy AI reads never expose consumer identity.
func LinkReceipt(tx *gorm.DB, eventID uuid.UUID, tenant, ref string) error {
	if ref == "" || len(ref) > 160 || !DefaultCollector.CapturesReferences(tenant) {
		return nil
	}
	return tx.Exec(`INSERT INTO economics_receipt_links(event_id,tenant_id,attribution_ref) VALUES(?,?,?) ON CONFLICT(event_id) DO NOTHING`, eventID, tenant, ref).Error
}
func ProjectContentAttributions(tx *gorm.DB, tenant string, collectionEnabled bool) error {
	var exists bool
	if e := tx.Raw("SELECT to_regclass('content_items') IS NOT NULL").Scan(&exists).Error; e != nil || !exists {
		return e
	}
	var rows []struct {
		ID        string
		AuthorID  *string
		CreatedAt time.Time
		Type      string
		StoryID   *string
	}
	if e := tx.Raw(`SELECT c.public_id::text id,c.author_id::text author_id,c.created_at,c.type,c.story_id::text story_id FROM content_items c WHERE c.tenant_id=? AND NOT EXISTS(SELECT 1 FROM economics_attributions a WHERE a.id='content:'||c.public_id::text AND a.tenant_id=c.tenant_id) ORDER BY c.id LIMIT 1000`, tenant).Scan(&rows).Error; e != nil {
		return e
	}
	subjects := []string{}
	for _, r := range rows {
		if r.AuthorID != nil && collectionEnabled {
			subjects = append(subjects, "account:"+*r.AuthorID)
		}
	}
	barriers, e := loadSubjectBarriers(tx, subjects)
	if e != nil {
		return e
	}
	links := make([]Attribution, 0, len(rows))
	for _, r := range rows {
		owner := ""
		if r.AuthorID != nil && collectionEnabled {
			owner, _ = barriers.Resolve("account:"+*r.AuthorID, r.CreatedAt)
		}
		content, feature := r.ID, "pods"
		if r.Type == "NEWS" {
			feature = "news"
			if r.StoryID != nil {
				content = *r.StoryID
			}
		}
		// A content reference describes ownership, not the requester of every
		// later regeneration or maintenance attempt. Explicit requests use job refs.
		initiator := "system"
		links = append(links, Attribution{ID: "content:" + r.ID, TenantID: tenant, Source: "cms", Subject: initiator, OwnerSubject: owner, Feature: feature, Benefit: "shared_content", ContentID: content, CreatedAt: r.CreatedAt})
	}
	if len(links) == 0 {
		return nil
	}
	return tx.CreateInBatches(links, 500).Error
}

func anonymizeWeights(tx *gorm.DB, subject, destination string) error {
	return anonymizeSubjectsWeights(tx, []string{subject}, destination)
}
func anonymizeSubjectsWeights(tx *gorm.DB, subjects []string, destination string) error {
	if e := tx.Exec(`INSERT INTO economics_content_weights(tenant_id,month,content_id,subject,segment,kind,client,value,observed_at) SELECT tenant_id,month,content_id,?,'',kind,client,sum(value),min(observed_at) FROM economics_content_weights WHERE subject IN ? GROUP BY tenant_id,month,content_id,kind,client ON CONFLICT(tenant_id,month,content_id,subject,segment,kind,client) DO UPDATE SET value=economics_content_weights.value+EXCLUDED.value,observed_at=LEAST(economics_content_weights.observed_at,EXCLUDED.observed_at)`, destination, subjects).Error; e != nil {
		return e
	}
	return tx.Where("subject IN ?", subjects).Delete(&Weight{}).Error
}

// Preserve resource totals without an expense/content join after history clearing.
// Anonymous offsets preserve the platform total; future pool corrections remain aggregate.
func retainClearedResourceTotals(tx *gorm.DB, subject string) error {
	selectCols := `tenant_id,revision,grain,day,?,resource,feature,sum(direct),sum(shared),CASE WHEN count(*)=count(before_benefits) THEN sum(before_benefits) END,bool_and(reconciled),?, ?,rule_version,max(published_at),''`
	insertCols := `(tenant_id,revision,grain,day,subject,resource,feature,direct,shared,before_benefits,reconciled,method,source_id,rule_version,published_at,expense_key)`
	from := ` FROM economics_cost_summaries WHERE subject=? AND source_id NOT IN('retained-resource','cleared-resource-offset') GROUP BY tenant_id,revision,grain,day,resource,feature,rule_version`
	// Capture a detached account/resource aggregate, then a cancelling platform adjustment.
	if e := tx.Exec(`INSERT INTO economics_cost_summaries `+insertCols+` SELECT `+selectCols+from, subject, "resource_total_after_history_clear", "retained-resource", subject).Error; e != nil {
		return e
	}
	negativeCols := `tenant_id,revision,grain,day,'cleared',resource,feature,-sum(direct),-sum(shared),CASE WHEN count(*)=count(before_benefits) THEN -sum(before_benefits) END,bool_and(reconciled),'history_clear_aggregate_bridge','cleared-resource-offset',rule_version,max(published_at),''`
	if e := tx.Exec(`INSERT INTO economics_cost_summaries `+insertCols+` SELECT `+negativeCols+from, subject).Error; e != nil {
		return e
	}
	return tx.Exec(`UPDATE economics_cost_summaries SET subject='cleared' WHERE subject=? AND source_id NOT IN('retained-resource','cleared-resource-offset')`, subject).Error
}

func ProjectStoryAttributions(tx *gorm.DB, tenant string) error {
	var exists bool
	if e := tx.Raw("SELECT to_regclass('stories') IS NOT NULL").Scan(&exists).Error; e != nil || !exists {
		return e
	}
	var rows []struct {
		ID        string
		CreatedAt time.Time
	}
	if e := tx.Raw(`SELECT s.public_id::text id,s.created_at FROM stories s WHERE s.tenant_id=? AND NOT EXISTS(SELECT 1 FROM economics_attributions a WHERE a.id='story:'||s.public_id::text AND a.tenant_id=s.tenant_id) ORDER BY s.id LIMIT 1000`, tenant).Scan(&rows).Error; e != nil {
		return e
	}
	links := make([]Attribution, 0, len(rows))
	for _, r := range rows {
		links = append(links, Attribution{ID: "story:" + r.ID, TenantID: tenant, Source: "cms", Subject: "system", Feature: "news", Benefit: "shared_content", ContentID: r.ID, CreatedAt: r.CreatedAt})
	}
	if len(links) == 0 {
		return nil
	}
	return tx.CreateInBatches(links, 500).Error
}

func financialFingerprint(c Cost) string {
	c.ID, c.Checksum, c.Subject, c.ContentID = "", "", "", ""
	c.Revision = 0
	c.CreatedAt = time.Time{}
	payload, _ := json.Marshal(c)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

// Only trusted aggregate financial adapters call this under Lock. No consumer
// identity is accepted here; manual/private writes use AppendCost and barriers.
func appendKnownFinancialRevisions(tx *gorm.DB, proposals, previous []Cost) error {
	latest := map[string]Cost{}
	for _, c := range previous {
		latest[c.SourceID+":"+c.LogicalKey] = c
	}
	rows := make([]Cost, 0, len(proposals))
	for _, c := range proposals {
		if c.Subject != "" || c.AttributionRef != "" {
			return fmt.Errorf("aggregate adapter cannot assign a consumer")
		}
		if e := ValidateCost(&c); e != nil {
			return e
		}
		prior := latest[c.SourceID+":"+c.LogicalKey]
		c.Checksum = financialFingerprint(c)
		if prior.Checksum == c.Checksum {
			continue
		}
		c.ID = uuid.NewString()
		c.Revision = prior.Revision + 1
		c.CreatedAt = time.Now().UTC()
		rows = append(rows, c)
	}
	if len(rows) == 0 {
		return nil
	}
	return tx.CreateInBatches(rows, 500).Error
}
