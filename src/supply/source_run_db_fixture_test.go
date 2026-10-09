package supply

import (
	"os"
	"testing"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/tests/testdb"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

func supplyDisposableDatabaseConfigured() bool {
	return os.Getenv("CMS_TEST_ADMIN_URL") != "" || os.Getenv("CMS_TEST_DATABASE_URL") != ""
}

// openSupplyFixtureDB deliberately creates only the model surface exercised by
// transactional protocol tests. Canonical migration application is covered by
// the separately guarded disposable-Neon ledger harness.
func openSupplyFixtureDB(t *testing.T) *gorm.DB {
	t.Helper()
	if !supplyDisposableDatabaseConfigured() {
		t.Skip("set guarded CMS_TEST_ADMIN_URL or CMS_TEST_DATABASE_URL to run supply DB tests")
	}
	db := testdb.Open(t)
	if err := db.AutoMigrate(
		&models.ContentSource{}, &models.ContentItem{}, &models.ContentProcessingEvent{},
		&models.SourceRunRequest{}, &models.SourceRunAttempt{}, &models.SourceRunExecutionUnit{},
		&models.SourceRunReceipt{}, &models.SourceRunRetainedReceipt{}, &models.SourceRunReconciliationEvent{},
		&models.SourceRunVerificationTask{}, &models.SourceRunProjectionWork{},
		&models.SourceRunAdmissionProtocol{}, &models.SourceRunAdmissionCutover{},
		&models.MediaSupplyControl{},
	); err != nil {
		t.Fatalf("migrate supply fixture schema: %v", err)
	}
	// These canonical composite keys are SQL-owned, so AutoMigrate cannot
	// supply the constraints used by owner idempotency and receipt admission.
	for _, statement := range []string{
		`CREATE UNIQUE INDEX IF NOT EXISTS fixture_request_identity ON source_run_requests (tenant_id,idempotency_key)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS fixture_attempt_number ON source_run_attempts (tenant_id,source_run_request_id,attempt_number)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS fixture_attempt_fence ON source_run_attempts (tenant_id,fence_token)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS fixture_unit_key ON source_run_execution_units (tenant_id,source_run_attempt_id,unit_key)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS fixture_unit_job ON source_run_execution_units (tenant_id,job_id)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS fixture_receipt_key ON source_run_receipts (tenant_id,producer_event_key)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS fixture_retained_receipt_key ON source_run_retained_receipts (tenant_id,producer_event_key)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS fixture_verification_key ON source_run_verification_tasks (tenant_id,task_key)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS fixture_reconciliation_key ON source_run_reconciliation_events (tenant_id,event_key)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS fixture_projection_key ON source_run_projection_work (tenant_id,evidence_kind,evidence_id,reducer_version)`,
	} {
		if err := db.Exec(statement).Error; err != nil {
			t.Fatalf("create supply fixture constraint: %v", err)
		}
	}
	clearSupplyTables(t, db,
		"source_run_projection_work", "source_run_verification_tasks", "source_run_reconciliation_events", "source_run_retained_receipts", "source_run_receipts", "source_run_execution_units", "source_run_attempts", "content_processing_events", "source_run_requests", "source_run_admission_cutovers", (models.SourceRunAdmissionProtocol{}).TableName(), "media_supply_controls", "content_items", "content_sources",
	)
	return db
}

func clearSupplyTables(t *testing.T, db *gorm.DB, tables ...string) {
	t.Helper()
	for _, table := range tables {
		if err := db.Exec("DELETE FROM " + table).Error; err != nil {
			t.Fatalf("clear %s: %v", table, err)
		}
	}
}

func provisionSupplyFixture(t *testing.T, db *gorm.DB, tenant, lane string) models.ContentSource {
	t.Helper()
	if err := db.Where("protocol_key = ?", admissionProtocolKey).Delete(&models.SourceRunAdmissionProtocol{}).Error; err != nil {
		t.Fatal(err)
	}
	protocol := models.SourceRunAdmissionProtocol{ProtocolKey: admissionProtocolKey, Epoch: admissionEpochDurable, Build: ContractVersion}
	if err := db.Create(&protocol).Error; err != nil {
		t.Fatal(err)
	}
	cutover := models.SourceRunAdmissionCutover{TenantID: tenant, Lane: lane, Protocol: ContractVersion, Mode: admissionModeDurable}
	if err := db.Create(&cutover).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Add(-time.Minute)
	url := "https://example.test/" + tenant + "/feed"
	sourceType := models.SourceTypePodcast
	if lane == "news" {
		sourceType = models.SourceTypeRSS
	}
	source := models.ContentSource{PublicID: uuid.New(), TenantID: tenant, Name: tenant + " fixture", Type: sourceType, Category: lane, FeedURL: &url, IsActive: true, FetchIntervalMinutes: 30, NextDueAt: &now, SourceConfigVersion: 1}
	if err := db.Create(&source).Error; err != nil {
		t.Fatal(err)
	}
	return source
}

func supplyFixtureIdentity(source models.ContentSource) RequestIdentity {
	due := time.Now().UTC().Truncate(time.Minute)
	return RequestIdentity{TenantID: source.TenantID, ContentSourceID: source.PublicID.String(), Lane: source.Category, Purpose: "baseline", CadenceWindowStart: due, SourceConfigVersion: source.SourceConfigVersion, PolicyFingerprint: "fixture-policy", ArgumentFingerprint: "fixture-args"}
}
