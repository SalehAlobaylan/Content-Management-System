package newscollection

import (
	"content-management-system/src/models"
	"content-management-system/src/supply"
	"content-management-system/src/tests/testdb"
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func testCaps() Capabilities {
	return Capabilities{CheckedAt: time.Now().UTC(), SchedulerReady: true, Providers: map[string]Provider{"RSS": {Ready: true}, "TWITTER": {Ready: true}, "TELEGRAM": {Ready: false, Reason: "telegram_not_configured"}}}
}
func TestSourceBlockers(t *testing.T) {
	feed := "https://example.org/feed"
	s := models.ContentSource{Type: models.SourceTypeRSS, FeedURL: &feed}
	if SourceBlocker(s, testCaps()) != "" {
		t.Fatal("configured RSS rejected")
	}
	s.Type = models.SourceTypeTelegram
	if SourceBlocker(s, testCaps()) != "telegram_not_configured" {
		t.Fatal("missing session was hidden")
	}
	s.Type = models.SourceTypeTwitter
	s.APIConfig = []byte(`{"mode":"api"}`)
	if SourceBlocker(s, testCaps()) != "provider_readiness_unavailable" {
		t.Fatal("API configuration was confused with scrape mode")
	}
}

func canonicalDB(t *testing.T) *gorm.DB {
	t.Helper()
	if os.Getenv("CMS_TEST_ADMIN_URL") == "" {
		t.Skip("requires guarded disposable PostgreSQL")
	}
	db := testdb.Open(t)
	var name string
	if err := db.Raw("SELECT current_database()").Scan(&name).Error; err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(os.Getenv("CMS_TEST_ADMIN_URL"))
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	root, _ := filepath.Abs("../..")
	cmd := exec.CommandContext(ctx, "go", "run", "./cmd/migrate", "--all", "--bootstrap-empty", "--allow-destructive")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "DATABASE_URL="+u.String(), "WAHB_MANAGED_ENV=1", "CMS_MIGRATION_BOOTSTRAP_DISPOSABLE=I_UNDERSTAND_THIS_DATABASE_IS_DISPOSABLE")
	if raw, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("canonical migration failed: %v (%d bytes withheld)", err, len(raw))
	}
	return db
}
func source(t *testing.T, db *gorm.DB, tenant, category string, active bool) models.ContentSource {
	t.Helper()
	id := uuid.New()
	feed := "https://example.org/" + id.String()
	if err := db.Model(&models.ContentSource{}).Create(map[string]any{"public_id": id, "tenant_id": tenant, "name": id.String(), "category": category, "type": "RSS", "feed_url": feed, "is_active": active, "fetch_interval_minutes": 37}).Error; err != nil {
		t.Fatal(err)
	}
	var s models.ContentSource
	if err := db.Where("public_id=?", id).First(&s).Error; err != nil {
		t.Fatal(err)
	}
	return s
}
func preview(t *testing.T, db *gorm.DB, action string, ids ...uuid.UUID) models.NewsSourceOperation {
	t.Helper()
	scope := "selected"
	if len(ids) == 0 {
		scope = "all"
	}
	op, err := Preview(db, "default", "operator", Request{Action: action, Scope: scope, IDs: ids}, testCaps())
	if err != nil {
		t.Fatal(err)
	}
	return op
}
func apply(t *testing.T, db *gorm.DB, op models.NewsSourceOperation) Result {
	t.Helper()
	r, err := Apply(db, "default", "operator", op.PublicID, testCaps())
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func reload(t *testing.T, db *gorm.DB, id uuid.UUID) models.ContentSource {
	t.Helper()
	var s models.ContentSource
	if err := db.Where("public_id=?", id).First(&s).Error; err != nil {
		t.Fatal(err)
	}
	return s
}

func TestCanonicalNewsOperations(t *testing.T) {
	db := canonicalDB(t)
	a := source(t, db, "default", "news", false)
	b := source(t, db, "default", "news", false)
	pods := source(t, db, "default", "media", false)
	other := source(t, db, "other", "news", false)
	for _, id := range []uuid.UUID{pods.PublicID, other.PublicID} {
		if _, err := Preview(db, "default", "operator", Request{Action: "enable", Scope: "selected", IDs: []uuid.UUID{id}}, testCaps()); err == nil {
			t.Fatal("foreign scope accepted")
		}
	}
	op := preview(t, db, "enable")
	later := source(t, db, "default", "news", false)
	// A known excluded provider is reviewed and never silently enabled.
	telegram := source(t, db, "default", "news", false)
	if err := db.Model(&telegram).Update("type", "TELEGRAM").Error; err != nil {
		t.Fatal(err)
	}
	if _, err := Apply(db, "other", "operator", op.PublicID, testCaps()); err == nil {
		t.Fatal("cross tenant apply")
	}
	if _, err := Apply(db, "default", "different-actor", op.PublicID, testCaps()); err == nil {
		t.Fatal("cross actor apply")
	}
	var wg sync.WaitGroup
	results := make(chan Result, 2)
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := Apply(db, "default", "operator", op.PublicID, testCaps())
			results <- r
			errs <- e
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for r := range results {
		if r.Changed != 2 {
			t.Fatalf("idempotency: %+v", r)
		}
	}
	for _, id := range []uuid.UUID{a.PublicID, b.PublicID} {
		s := reload(t, db, id)
		if !s.IsActive || s.NextDueAt == nil || s.FetchIntervalMinutes != 37 {
			t.Fatal("activation or schedule missing")
		}
	}
	if reload(t, db, later.PublicID).IsActive || reload(t, db, pods.PublicID).IsActive || reload(t, db, other.PublicID).IsActive {
		t.Fatal("selection widened")
	}
	all := apply(t, db, preview(t, db, "enable"))
	if all.Changed != 1 || all.Excluded != 1 || all.Unchanged != 2 {
		t.Fatalf("exclusions/noops: %+v", all)
	}
	// An older enabled row with no due date must not stay silently unscheduled.
	if err := db.Model(&a).UpdateColumn("next_due_at", nil).Error; err != nil {
		t.Fatal(err)
	}
	repaired := apply(t, db, preview(t, db, "enable", a.PublicID))
	if repaired.Changed != 1 || reload(t, db, a.PublicID).NextDueAt == nil {
		t.Fatal("missing schedule was not initialized")
	}
	// New config versions invalidate a review atomically.
	stale := preview(t, db, "disable", a.PublicID, b.PublicID)
	if err := db.Model(&b).Update("name", "edited after review").Error; err != nil {
		t.Fatal(err)
	}
	if _, err := Apply(db, "default", "operator", stale.PublicID, testCaps()); err == nil {
		t.Fatal("stale preview applied")
	}
	if !reload(t, db, a.PublicID).IsActive {
		t.Fatal("partial activation committed")
	}
	// Failure to save the audit receipt must also roll back source writes.
	rollback := preview(t, db, "disable", a.PublicID)
	if err := db.Exec(`CREATE FUNCTION reject_news_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture receipt failure'; END $$; CREATE TRIGGER reject_news_receipt BEFORE UPDATE ON news_source_operations FOR EACH ROW EXECUTE FUNCTION reject_news_receipt()`).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := Apply(db, "default", "operator", rollback.PublicID, testCaps()); err == nil {
		t.Fatal("receipt failure accepted")
	}
	if !reload(t, db, a.PublicID).IsActive {
		t.Fatal("audit rollback failed")
	}
	if err := db.Exec(`DROP TRIGGER reject_news_receipt ON news_source_operations; DROP FUNCTION reject_news_receipt()`).Error; err != nil {
		t.Fatal(err)
	}
	// Preserve a pre-existing schedule, backoff, checkpoint and cadence.
	due := time.Now().UTC().Add(time.Hour)
	checkpoint := time.Now().UTC().Add(-time.Hour)
	if err := db.Model(&a).Updates(map[string]any{"next_due_at": due, "failure_streak": 3, "intake_circuit_until": due, "last_provider_success_at": checkpoint}).Error; err != nil {
		t.Fatal(err)
	}
	apply(t, db, preview(t, db, "disable", a.PublicID))
	apply(t, db, preview(t, db, "enable", a.PublicID))
	s := reload(t, db, a.PublicID)
	if s.FailureStreak != 3 || s.NextDueAt == nil || s.IntakeCircuitUntil == nil || s.LastProviderSuccessAt == nil || s.FetchIntervalMinutes != 37 || s.NextDueAt.Sub(due) > time.Millisecond {
		t.Fatal("operational history was reset")
	}
	// A collection pause preserves inclusion and invalidates older previews.
	beforePause := preview(t, db, "disable", b.PublicID)
	apply(t, db, preview(t, db, "pause"))
	if _, err := Apply(db, "default", "operator", beforePause.PublicID, testCaps()); err == nil {
		t.Fatal("control version staleness ignored")
	}
	if !reload(t, db, b.PublicID).IsActive || reload(t, db, pods.PublicID).IsActive {
		t.Fatal("pause changed roster/Pods")
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		allowed, err := supply.MayPullNews(tx, "default")
		if err != nil {
			return err
		}
		if allowed {
			t.Fatal("pause ignored")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	apply(t, db, preview(t, db, "resume"))
	var count int64
	if err := db.Model(&models.SourceRunRequest{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatal("activation directly created source runs", err, count)
	}
	// Missing admission blocks enable/resume but never a safety pause.
	if err := db.Where("tenant_id='default' AND lane='news'").Delete(&models.SourceRunAdmissionCutover{}).Error; err != nil {
		t.Fatal(err)
	}
	blocked := preview(t, db, "enable", telegram.PublicID)
	var snapshot Snapshot
	_ = json.Unmarshal(blocked.Snapshot, &snapshot)
	if snapshot.Readiness.Ready {
		t.Fatal("missing lane hidden")
	}
	if _, err := Apply(db, "default", "operator", blocked.PublicID, testCaps()); err == nil {
		t.Fatal("unprovisioned lane activated")
	}
	apply(t, db, preview(t, db, "pause"))
	// Explicit inactive-lane provisioning never activates the roster.
	apply(t, db, preview(t, db, "disable"))
	lane, err := supply.PreviewInactiveLane(db, "default", "news")
	if err != nil {
		t.Fatal(err)
	}
	if err := supply.ProvisionInactiveLane(db, "default", "news", "wrong", "operator"); err == nil {
		t.Fatal("unbound provisioning accepted")
	}
	if err := supply.ProvisionInactiveLane(db, "default", "news", lane.Digest, "operator"); err != nil {
		t.Fatal(err)
	}
	if err := supply.RequireDurableAdmission(db, "default", "news"); err != nil {
		t.Fatal(err)
	}
	var active int64
	if err := db.Model(&models.ContentSource{}).Where("category='news' AND tenant_id='default' AND is_active=TRUE").Count(&active).Error; err != nil || active != 0 {
		t.Fatal("provisioning activated sources", err, active)
	}
}
