package contentstage

import (
	"content-management-system/src/models"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"testing"
	"time"
)

func repairMockDB(t *testing.T) (*gorm.DB, sqlmock.Sqlmock) {
	t.Helper()
	sqldb, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sqldb.Close() })
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqldb}), &gorm.Config{SkipDefaultTransaction: true})
	if err != nil {
		t.Fatal(err)
	}
	return db, mock
}
func repairAttemptRows(a models.ContentStageAttempt) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"public_id", "tenant_id", "request_id", "lane", "stage", "owner", "input_fingerprint", "state", "fence_token", "claim_token", "lease_epoch", "lease_expires_at"}).AddRow(a.PublicID, a.TenantID, a.RequestID, a.Lane, a.Stage, a.Owner, a.InputFingerprint, a.State, a.FenceToken, a.ClaimToken, a.LeaseEpoch, a.LeaseExpiresAt)
}
func repairRequestRows(r models.ContentStageRequest) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"public_id", "tenant_id", "content_item_id", "processing_generation", "lane", "stage", "owner", "input_fingerprint", "state"}).AddRow(r.PublicID, r.TenantID, r.ContentItemID, r.ProcessingGeneration, r.Lane, r.Stage, r.Owner, r.InputFingerprint, r.State)
}
func expectNoEffectPreview(mock sqlmock.Sqlmock, a models.ContentStageAttempt, r models.ContentStageRequest) {
	mock.ExpectQuery(`SELECT .* FROM "content_stage_attempts"`).WillReturnRows(repairAttemptRows(a))
	mock.ExpectQuery(`SELECT .* FROM "content_stage_requests"`).WillReturnRows(repairRequestRows(r))
	mock.ExpectQuery(`SELECT .* FROM "content_stage_receipts"`).WillReturnRows(sqlmock.NewRows([]string{"public_id"}))
	mock.ExpectQuery(`SELECT .* FROM "content_items"`).WillReturnRows(sqlmock.NewRows([]string{"public_id"}))
}

func TestAttemptRepairTransactionRejectsStaleReviewBeforeAnyWrites(t *testing.T) {
	db, mock := repairMockDB(t)
	mock.ExpectBegin()
	mock.ExpectExec(`LOCK TABLE content_stage_requests`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT .* FROM "content_stage_attempts"`).WillReturnRows(sqlmock.NewRows([]string{"public_id"}))
	mock.ExpectRollback()
	err := db.Transaction(func(tx *gorm.DB) error {
		_, err := ApplyMigrationAttemptRepair(tx, "stale", "iam:admin", time.Now().UTC())
		return err
	})
	if err == nil {
		t.Fatal("stale review wrote effects")
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAttemptRepairRollsBackWhenAuditCannotBeRetained(t *testing.T) {
	db, mock := repairMockDB(t)
	a, r, _, _, now := attemptRepairEvidence()
	a.State, a.EffectStartedAt, r.State = models.ContentStageClaimed, nil, models.ContentStageQueued
	expectNoEffectPreview(mock, a, r)
	p, err := PreviewMigrationAttemptRepair(db, now)
	if err != nil || len(p.Actions) != 1 {
		t.Fatal(p, err)
	}
	mock.ExpectBegin()
	mock.ExpectExec(`LOCK TABLE content_stage_requests`).WillReturnResult(sqlmock.NewResult(0, 0))
	expectNoEffectPreview(mock, a, r)
	mock.ExpectExec(`UPDATE "content_stage_attempts".*tenant_id=.*public_id=.*request_id=.*state IN`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT .* FROM "content_stage_requests"`).WillReturnRows(repairRequestRows(r))
	mock.ExpectQuery(`SELECT COALESCE\(MAX\(sequence\),0\) FROM "content_stage_events"`).WillReturnRows(sqlmock.NewRows([]string{"sequence"}).AddRow(0))
	auditFailure := errors.New("audit unavailable")
	mock.ExpectQuery(`INSERT INTO "content_stage_events"`).WillReturnError(auditFailure)
	mock.ExpectRollback()
	err = db.Transaction(func(tx *gorm.DB) error {
		_, err := ApplyMigrationAttemptRepair(tx, p.Digest, "iam:admin", now)
		return err
	})
	if !errors.Is(err, auditFailure) {
		t.Fatal(err)
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestNormalVerificationLeavesUncorrelatedAttemptsOpen(t *testing.T) {
	db, mock := repairMockDB(t)
	a, r, _, receipt, _ := attemptRepairEvidence()
	receipt.AttemptID = uuid.New() // Another attempt cannot settle this one.
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT .* FROM "content_stage_attempts"`).WillReturnRows(repairAttemptRows(a))
	mock.ExpectQuery(`SELECT .* FROM "content_stage_receipts"`).WillReturnRows(sqlmock.NewRows([]string{"public_id", "attempt_id"}).AddRow(receipt.PublicID, receipt.AttemptID))
	mock.ExpectCommit()
	if err := db.Transaction(func(tx *gorm.DB) error { return settleVerifiedAttemptReceipts(tx, r) }); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestNormalVerificationClosesOnlyItsReceiptBoundAttemptAndAuditsIt(t *testing.T) {
	db, mock := repairMockDB(t)
	a, r, _, c, _ := attemptRepairEvidence()
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT .* FROM "content_stage_attempts"`).WillReturnRows(repairAttemptRows(a))
	mock.ExpectQuery(`SELECT .* FROM "content_stage_receipts"`).WillReturnRows(sqlmock.NewRows([]string{"public_id", "tenant_id", "request_id", "attempt_id", "content_item_id", "processing_generation", "lane", "stage", "owner", "producer_event_id", "fence_token", "input_fingerprint", "outcome", "payload_digest"}).AddRow(c.PublicID, c.TenantID, c.RequestID, c.AttemptID, c.ContentItemID, c.ProcessingGeneration, c.Lane, c.Stage, c.Owner, c.ProducerEventID, c.FenceToken, c.InputFingerprint, c.Outcome, c.PayloadDigest))
	mock.ExpectExec(`UPDATE "content_stage_attempts".*tenant_id=.*public_id=.*request_id=.*state IN`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT COALESCE\(MAX\(sequence\),0\) FROM "content_stage_events"`).WillReturnRows(sqlmock.NewRows([]string{"sequence"}).AddRow(0))
	mock.ExpectQuery(`INSERT INTO "content_stage_events"`).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectCommit()
	if err := db.Transaction(func(tx *gorm.DB) error { return settleVerifiedAttemptReceipts(tx, r) }); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func attemptRepairEvidence() (models.ContentStageAttempt, models.ContentStageRequest, models.ContentItem, models.ContentStageReceipt, time.Time) {
	now := time.Now().UTC()
	r := models.ContentStageRequest{PublicID: uuid.New(), TenantID: "tenant-a", ContentItemID: uuid.New(), ProcessingGeneration: 1, Lane: "pods", Stage: models.ContentStagePodsTextEmbedding, Owner: models.ContentStageOwnerAggregationPods, State: models.ContentStageVerified, InputFingerprint: "exact-input"}
	a := models.ContentStageAttempt{PublicID: uuid.New(), TenantID: r.TenantID, RequestID: r.PublicID, Lane: r.Lane, Stage: r.Stage, Owner: r.Owner, InputFingerprint: r.InputFingerprint, State: models.ContentStageVerifying, FenceToken: uuid.New(), ClaimToken: uuid.New(), LeaseEpoch: 1, LeaseExpiresAt: now.Add(-time.Hour), EffectStartedAt: &now}
	i := models.ContentItem{PublicID: r.ContentItemID, TenantID: r.TenantID, ProcessingGeneration: 1}
	c := models.ContentStageReceipt{PublicID: uuid.New(), TenantID: r.TenantID, RequestID: r.PublicID, AttemptID: a.PublicID, ContentItemID: i.PublicID, ProcessingGeneration: 1, Lane: r.Lane, Stage: r.Stage, Owner: models.ContentStageOwnerEnrichment, FenceToken: a.FenceToken, InputFingerprint: a.InputFingerprint, Outcome: "persisted", ProducerEventID: uuid.New(), PayloadDigest: "retained-payload"}
	return a, r, i, c, now
}
func TestAttemptRepairRequiresThisAttemptsPersistedEffect(t *testing.T) {
	a, r, i, c, now := attemptRepairEvidence()
	if kind, _ := classifyMigrationAttempt(a, r, i, []models.ContentStageReceipt{c}, true, now); kind != "persisted" {
		t.Fatal("exact receipt-bound result was not repairable")
	}
	for _, change := range []func(*models.ContentStageReceipt){
		func(c *models.ContentStageReceipt) { c.AttemptID = uuid.New() },
		func(c *models.ContentStageReceipt) { c.FenceToken = uuid.New() },
		func(c *models.ContentStageReceipt) { c.TenantID = "other" },
		func(c *models.ContentStageReceipt) { c.ProcessingGeneration = 2 },
		func(c *models.ContentStageReceipt) { c.RequestID = uuid.New() },
		func(c *models.ContentStageReceipt) { c.ContentItemID = uuid.New() },
		func(c *models.ContentStageReceipt) { c.InputFingerprint = "changed" },
		func(c *models.ContentStageReceipt) { c.Owner = "media" },
		func(c *models.ContentStageReceipt) { c.Outcome = "unknown" },
	} {
		bad := c
		change(&bad)
		if kind, _ := classifyMigrationAttempt(a, r, i, []models.ContentStageReceipt{bad}, true, now); kind != "" {
			t.Fatal("uncorrelated effect was approved", bad)
		}
	}
	if kind, _ := classifyMigrationAttempt(a, r, i, nil, true, now); kind != "" {
		t.Fatal("terminal request substituted for effect proof")
	}
	if kind, _ := classifyMigrationAttempt(a, r, i, []models.ContentStageReceipt{c}, false, now); kind != "" {
		t.Fatal("absent artifact was approved")
	}
	i.ProcessingGeneration = 2
	if kind, _ := classifyMigrationAttempt(a, r, i, []models.ContentStageReceipt{c}, true, now); kind != "" {
		t.Fatal("changed content generation was approved")
	}
}
func TestAttemptRepairNeverInfersNoEffectFromExpiryAlone(t *testing.T) {
	a, r, i, c, now := attemptRepairEvidence()
	a.State = models.ContentStageClaimed
	a.EffectStartedAt = nil
	r.State = models.ContentStageQueued
	if kind, _ := classifyMigrationAttempt(a, r, i, nil, false, now); kind != "no_effect" {
		t.Fatal("proven unstarted expired claim was not repairable")
	}
	a.EffectStartedAt = &now
	if kind, _ := classifyMigrationAttempt(a, r, i, nil, false, now); kind != "" {
		t.Fatal("effect-started claim was discarded")
	}
	a.EffectStartedAt = nil
	if kind, _ := classifyMigrationAttempt(a, r, i, []models.ContentStageReceipt{c}, false, now); kind != "" {
		t.Fatal("receipt-bearing claim was declared unstarted")
	}
	a.LeaseExpiresAt = now.Add(time.Hour)
	if kind, _ := classifyMigrationAttempt(a, r, i, nil, false, now); kind != "" {
		t.Fatal("live claim was retired")
	}
	a.LeaseExpiresAt = now.Add(-time.Hour)
	r.State = models.ContentStageRunning
	if kind, _ := classifyMigrationAttempt(a, r, i, nil, false, now); kind != "" {
		t.Fatal("running request was discarded")
	}
}
