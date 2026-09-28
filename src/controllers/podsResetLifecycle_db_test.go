package controllers

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"content-management-system/src/tests/testdb"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// This migration-backed failure test runs only against the explicitly guarded
// local disposable CMS test target. It never loads service .env configuration.
func TestPodsResetSchemaAndDatabaseFences(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CMS_TEST_ADMIN_URL")) == "" && strings.TrimSpace(os.Getenv("CMS_TEST_DATABASE_URL")) == "" {
		t.Skip("set CMS_TEST_ADMIN_URL or CMS_TEST_DATABASE_URL plus CMS_TEST_DISPOSABLE to run guarded Pods reset schema tests")
	}
	db := testdb.Open(t)
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)

	schemaName := "pods_reset_qual_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if err := db.Exec("CREATE SCHEMA \"" + schemaName + "\"").Error; err != nil {
		t.Fatalf("create isolated reset test schema: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Exec("SET search_path TO public").Error
		if err := db.Exec("DROP SCHEMA IF EXISTS \"" + schemaName + "\" CASCADE").Error; err != nil {
			t.Errorf("drop isolated reset test schema: %v", err)
		}
	})
	if err := db.Exec("SET search_path TO \"" + schemaName + "\", public").Error; err != nil {
		t.Fatalf("select isolated reset test schema: %v", err)
	}

	for _, statement := range []string{
		`CREATE TABLE retention_execution_controls (tenant_id varchar(64) PRIMARY KEY)`,
		`CREATE TABLE content_items (
			public_id uuid PRIMARY KEY,
			tenant_id varchar(64) NOT NULL,
			idempotency_key varchar(512),
			parent_content_item_id uuid REFERENCES content_items(public_id) ON DELETE SET NULL,
			transcript_id uuid,
			story_id uuid,
			status varchar(24) NOT NULL DEFAULT 'READY',
			feed_visibility varchar(24) NOT NULL DEFAULT 'visible'
		)`,
		`CREATE TABLE transcripts (
			public_id uuid PRIMARY KEY DEFAULT gen_random_uuid(), content_item_id uuid NOT NULL,
			full_text text NOT NULL DEFAULT '', retired_payload_at timestamptz
		)`,
		`CREATE TABLE media_circulation_overrides (
			public_id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id varchar(64) NOT NULL,
			subject_kind varchar(24) NOT NULL, subject_id uuid NOT NULL, override_type varchar(32) NOT NULL,
			expires_at timestamptz
		)`,
		`CREATE TABLE retention_holds (
			public_id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id varchar(64) NOT NULL,
			target_type varchar(24) NOT NULL, target_id uuid NOT NULL, released_at timestamptz, expires_at timestamptz
		)`,
		`CREATE TABLE moderation_reports (
			public_id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id varchar(64) NOT NULL,
			target_type varchar(16) NOT NULL, target_id uuid NOT NULL, status varchar(24) NOT NULL DEFAULT 'open'
		)`,
		`CREATE TABLE media_supply_action_requests (
			public_id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id varchar(64) NOT NULL,
			target_type varchar(32) NOT NULL, target_id uuid NOT NULL
		)`,
		`CREATE TABLE media_supply_action_previews (
			public_id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id varchar(64) NOT NULL,
			target_type varchar(64) NOT NULL, target_id uuid NOT NULL, state varchar(24) NOT NULL,
			expires_at timestamptz NOT NULL
		)`,
		`CREATE TABLE media_circulation_recommendations (
			public_id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id varchar(64) NOT NULL,
			unit_type varchar(24) NOT NULL, subject_id uuid NOT NULL, subject_kind varchar(24), status varchar(24) NOT NULL
		)`,
		`CREATE TABLE content_flags (
			public_id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id varchar(64) NOT NULL,
			content_item_id uuid NOT NULL REFERENCES content_items(public_id)
		)`,
		`CREATE TABLE experience_events (id bigserial PRIMARY KEY, tenant_id varchar(64) NOT NULL, content_id uuid)`,
		`CREATE TABLE enrichment_autopilot_actions (id bigserial PRIMARY KEY, tenant_id varchar(64) NOT NULL, content_id uuid)`,
		`CREATE TABLE news_month_archive_story_sources (id bigserial PRIMARY KEY, original_content_id uuid NOT NULL)`,
		`CREATE TABLE feed_recovery_plan_targets (
			id bigserial PRIMARY KEY, tenant_id varchar(64) NOT NULL, target_type varchar(32) NOT NULL, target_id uuid NOT NULL
		)`,
		`CREATE TABLE feed_recovery_plans (id bigserial PRIMARY KEY, tenant_id varchar(64) NOT NULL, state varchar(32) NOT NULL, expires_at timestamptz NOT NULL)`,
		`CREATE TABLE feed_recovery_runs (id bigserial PRIMARY KEY, plan_id bigint NOT NULL, phase varchar(32) NOT NULL, rollback_deadline timestamptz)`,
		`CREATE TABLE retention_compaction_batches (
			id bigserial PRIMARY KEY, tenant_id varchar(64) NOT NULL, target_ids jsonb NOT NULL, state varchar(32) NOT NULL
		)`,
		`CREATE TABLE retention_compaction_manifests (
			id bigserial PRIMARY KEY, tenant_id varchar(64) NOT NULL, anchor_content_ids jsonb NOT NULL,
			protected_content_ids jsonb NOT NULL, retire_content_ids jsonb NOT NULL,
			state varchar(24) NOT NULL, expires_at timestamptz NOT NULL
		)`,
		`CREATE TABLE news_ingest_tombstones (id bigserial PRIMARY KEY, tenant_id varchar(64) NOT NULL, original_content_id uuid NOT NULL)`,
		`CREATE TABLE feed_generation_memberships (
			generation_id uuid NOT NULL, member_type varchar(16) NOT NULL, member_id uuid NOT NULL,
			attached_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (generation_id, member_type, member_id)
		)`,
	} {
		if err := db.Exec(statement).Error; err != nil {
			t.Fatalf("create canonical-shape reset fixture: %v", err)
		}
	}
	migration, err := os.ReadFile(filepath.Join("..", "..", "migrations", "20260927100000_pods_reset_execution.sql"))
	if err != nil {
		t.Fatalf("read canonical reset migration: %v", err)
	}
	if err := db.Exec(string(migration)).Error; err != nil {
		t.Fatalf("apply reset migration in isolated disposable schema: %v", err)
	}
	lifecycleMigration, err := os.ReadFile(filepath.Join("..", "..", "migrations", "20260928100000_pods_reset_lifecycle_safety.sql"))
	if err != nil {
		t.Fatalf("read canonical reset lifecycle migration: %v", err)
	}
	if err := db.Exec(string(lifecycleMigration)).Error; err != nil {
		t.Fatalf("apply lifecycle safety migration in isolated disposable schema: %v", err)
	}

	fingerprint, err := podsResetSchemaFingerprint(db)
	if err != nil || fingerprint == "" {
		t.Fatalf("canonical FK and retirement trigger coverage was not proven: fingerprint=%q err=%v", fingerprint, err)
	}
	if err := db.Exec(`CREATE TABLE reset_test_unknown_reference (id bigserial PRIMARY KEY, item_ref uuid NOT NULL REFERENCES content_items(public_id))`).Error; err != nil {
		t.Fatalf("create unknown-reference fixture: %v", err)
	}
	if _, err := podsResetSchemaFingerprint(db); err == nil || !strings.Contains(err.Error(), "no explicit reset disposition") {
		t.Fatalf("unknown content reference was not rejected closed: %v", err)
	}
	if err := db.Exec(`DROP TABLE reset_test_unknown_reference`).Error; err != nil {
		t.Fatalf("drop unknown-reference fixture: %v", err)
	}

	itemID, parentID, runID, fenceID, executorID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	if err := db.Exec(`INSERT INTO content_items (public_id, tenant_id, idempotency_key) VALUES (?, 'default', 'retired-source-key')`, itemID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO content_items (public_id, tenant_id, idempotency_key) VALUES (?, 'default', 'parent-source-key')`, parentID).Error; err != nil {
		t.Fatal(err)
	}
	transcriptID := uuid.New()
	if err := db.Exec(`INSERT INTO transcripts (public_id, content_item_id, full_text) VALUES (?, ?, 'legacy transcript')`, transcriptID, itemID).Error; err != nil {
		t.Fatal(err)
	}
	consumedPreviewID := uuid.New()
	if err := db.Exec(`INSERT INTO media_supply_action_previews (public_id, tenant_id, target_type, target_id, state, expires_at) VALUES (?, 'default', 'content_item', ?, 'consumed', NOW() - INTERVAL '1 hour')`, consumedPreviewID, itemID).Error; err != nil {
		t.Fatal(err)
	}
	dismissedRecommendationID := uuid.New()
	if err := db.Exec(`INSERT INTO media_circulation_recommendations (public_id, tenant_id, unit_type, subject_id, subject_kind, status) VALUES (?, 'default', 'item_family', ?, 'content_item', 'dismissed')`, dismissedRecommendationID, itemID).Error; err != nil {
		t.Fatal(err)
	}
	closedReportID := uuid.New()
	if err := db.Exec(`INSERT INTO moderation_reports (public_id, tenant_id, target_type, target_id, status) VALUES (?, 'default', 'content', ?, 'closed')`, closedReportID, itemID).Error; err != nil {
		t.Fatal(err)
	}
	manifestHash := strings.Repeat("a", 64)
	if err := db.Exec(`INSERT INTO pods_reset_runs (public_id, tenant_id, state, manifest_hash, schema_fingerprint, manifest, created_by, expires_at, fencing_token, execution_token, execution_lease_until)
		VALUES (?, 'default', 'executing', ?, ?, '{}', 'fixture', ?, ?, ?, ?)`, runID, manifestHash, fingerprint, time.Now().Add(time.Hour), fenceID, executorID, time.Now().Add(time.Hour)).Error; err != nil {
		t.Fatal(err)
	}
	identityHash := podsResetIdentityHash("default", "retired-source-key")
	if err := db.Exec(`INSERT INTO pods_reset_retirements (tenant_id, content_item_id, run_public_id, manifest_hash, fencing_token, identity_hash, state)
		VALUES ('default', ?, ?, ?, ?, ?, 'retiring')`, itemID, runID, manifestHash, fenceID, identityHash).Error; err != nil {
		t.Fatal(err)
	}

	assertRejected := func(name, statement string, args ...interface{}) {
		t.Helper()
		if err := db.Exec(statement, args...).Error; err == nil {
			t.Errorf("retirement fence allowed %s", name)
		}
	}
	assertRejected("an editorial flag for retired content", `INSERT INTO content_flags (tenant_id, content_item_id) VALUES ('default', ?)`, itemID)
	assertRejected("an active circulation protection on retired content", `INSERT INTO media_circulation_overrides (tenant_id, subject_kind, subject_id, override_type) VALUES ('default', 'item', ?, 'never_archive')`, itemID)
	assertRejected("an active retention hold on retired content", `INSERT INTO retention_holds (tenant_id, target_type, target_id) VALUES ('default', 'content', ?)`, itemID)
	assertRejected("a new content moderation report", `INSERT INTO moderation_reports (tenant_id, target_type, target_id) VALUES ('default', 'content', ?)`, itemID)
	assertRejected("a new active supply preview for retired content", `INSERT INTO media_supply_action_previews (tenant_id, target_type, target_id, state, expires_at) VALUES ('default', 'content_item', ?, 'active', NOW() + INTERVAL '1 hour')`, itemID)
	assertRejected("reactivation of a supply preview for retired content", `UPDATE media_supply_action_previews SET state='active', expires_at=NOW() + INTERVAL '1 hour' WHERE public_id=?`, consumedPreviewID)
	assertRejected("a pending circulation recommendation for retired content", `INSERT INTO media_circulation_recommendations (tenant_id, unit_type, subject_id, subject_kind, status) VALUES ('default', 'item_family', ?, 'content_item', 'pending')`, itemID)
	assertRejected("reactivation of a circulation recommendation for retired content", `UPDATE media_circulation_recommendations SET status='pending' WHERE public_id=?`, dismissedRecommendationID)
	assertRejected("a new feed recovery target for retired content", `INSERT INTO feed_recovery_plan_targets (tenant_id, target_type, target_id) VALUES ('default', 'media_content', ?)`, itemID)
	assertRejected("an unverified retention batch targeting retired content", `INSERT INTO retention_compaction_batches (tenant_id, target_ids, state) VALUES ('default', jsonb_build_array(?::text), 'running')`, itemID)
	assertRejected("an active retention manifest targeting retired content", `INSERT INTO retention_compaction_manifests (tenant_id, anchor_content_ids, protected_content_ids, retire_content_ids, state, expires_at) VALUES ('default', '[]', '[]', jsonb_build_array(?::text), 'approved', NOW() + INTERVAL '1 hour')`, itemID)
	assertRejected("a news ingest tombstone for retired content", `INSERT INTO news_ingest_tombstones (tenant_id, original_content_id) VALUES ('default', ?)`, itemID)
	assertRejected("a new telemetry event for retired content", `INSERT INTO experience_events (tenant_id, content_id) VALUES ('default', ?)`, itemID)
	if err := db.Exec(`UPDATE moderation_reports SET status='resolved' WHERE public_id=?`, closedReportID).Error; err != nil {
		t.Errorf("terminal moderation history update should remain writable: %v", err)
	}
	if err := db.Exec(`INSERT INTO retention_holds (tenant_id, target_type, target_id, released_at) VALUES ('default', 'content', ?, NOW())`, itemID).Error; err != nil {
		t.Errorf("terminal retention history should remain writable for retired identity: %v", err)
	}
	assertRejected("a new feed membership for retired content", `INSERT INTO feed_generation_memberships (generation_id, member_type, member_id) VALUES (?, 'feed_unit', ?)`, uuid.New(), itemID)
	assertRejected("a new child linked to a retired parent", `INSERT INTO content_items (public_id, tenant_id, idempotency_key, parent_content_item_id) VALUES (?, 'default', 'child-key', ?)`, uuid.New(), itemID)
	assertRejected("reuse of a retired source idempotency identity", `INSERT INTO content_items (public_id, tenant_id, idempotency_key) VALUES (?, 'default', 'retired-source-key')`, uuid.New())
	assertRejected("hard deletion of the retained identity", `DELETE FROM content_items WHERE public_id=?`, itemID)
	assertRejected("unfenced mutation of the retired identity", `UPDATE content_items SET status='READY' WHERE public_id=?`, itemID)

	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("SELECT set_config('wahb.pods_reset.run_id', ?, true)", runID.String()).Error; err != nil {
			return err
		}
		if err := tx.Exec("SELECT set_config('wahb.pods_reset.fencing_token', ?, true)", fenceID.String()).Error; err != nil {
			return err
		}
		result := tx.Exec(`UPDATE content_items SET status='ARCHIVED', feed_visibility='hidden' WHERE public_id=?`, itemID)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("reset-owned mutation affected %d content rows", result.RowsAffected)
		}
		return tx.Exec(`UPDATE transcripts SET retired_payload_at=NOW() WHERE public_id=?`, transcriptID).Error
	}); err != nil {
		t.Fatalf("active reset owner could not complete its fenced retirement mutation: %v", err)
	}
	assertRejected("a new link to retired transcript payload", `INSERT INTO content_items (public_id, tenant_id, idempotency_key, transcript_id) VALUES (?, 'default', 'new-link-after-payload-retirement', ?)`, uuid.New(), transcriptID)

	if err := db.Exec(`CREATE TABLE atomization_chapter_units (tenant_id varchar(64), state varchar(24), result jsonb NOT NULL DEFAULT '{}'::jsonb)`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO atomization_chapter_units (tenant_id, state) VALUES ('default', 'verified')`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Table("atomization_chapter_units").Where("tenant_id=?", "default").Updates(map[string]interface{}{"state": "superseded", "result": "{}"}).Error; err != nil {
		t.Fatalf("reset finalization JSON result write violated NOT NULL schema: %v", err)
	}
}
