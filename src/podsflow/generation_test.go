package podsflow

import (
	"content-management-system/src/models"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"testing"
	"time"
)

func TestActivationRequiresReadyEditoriallyAdmittedChildren(t *testing.T) {
	duration := 600
	for _, tc := range []struct {
		visibility string
		status     models.ContentStatus
		want       bool
	}{
		{"embedding_pending", models.ContentStatusReady, true},
		{"visible", models.ContentStatusReady, true},
		{"review", models.ContentStatusReady, false},
		{"hidden", models.ContentStatusReady, false},
		{"embedding_pending", models.ContentStatusProcessing, false},
	} {
		item := models.ContentItem{Status: tc.status, FeedVisibility: tc.visibility, IsFeedUnit: true, DurationSec: &duration}
		if got := childReadyForActivation(item); got != tc.want {
			t.Fatalf("%s/%s: got %v want %v", tc.status, tc.visibility, got, tc.want)
		}
	}
}

func TestPartialUploadRecoveryStopsAtAttemptLimit(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{SkipDefaultTransaction: true})
	if err != nil {
		t.Fatal(err)
	}
	root := models.ContentItem{PublicID: uuid.New(), TenantID: "tenant-a", ProcessingGeneration: 1}
	mock.ExpectQuery("SELECT .*atomization_chapter_units.*FOR UPDATE SKIP LOCKED").WillReturnRows(sqlmock.NewRows([]string{"public_id", "tenant_id", "generation_id", "state", "fence_token", "attempt_count", "lease_expires_at"}).AddRow(uuid.New(), root.TenantID, uuid.New(), "uncertain", uuid.New(), 2, time.Now().Add(-time.Minute)))
	mock.ExpectQuery("SELECT count.*media_artifact_manifests").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery("SELECT count.*media_artifact_manifests").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec("UPDATE .atomization_chapter_units. SET .*failure_class.*").WithArgs(nil, "partial_attempt_budget_exhausted", nil, "failed", sqlmock.AnyArg(), root.TenantID, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := ReconcileAbsentUnits(db, root); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestFinalizationRetryRejectsMissingGeneration(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery("SELECT .*atomization_generations.*FOR UPDATE").WillReturnRows(sqlmock.NewRows([]string{"public_id"}))
	if err := ValidateFinalizationRetry(db, models.ContentStageRequest{PublicID: uuid.New(), TenantID: "tenant-a"}); err != gorm.ErrRecordNotFound {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestFinalizationRecoveryBudgetUsesOnlyExplicitApprovalBoundary(t *testing.T) {
	for _, tc := range []struct {
		count   int64
		allowed bool
	}{{0, true}, {1, true}, {2, false}, {10, false}} {
		sqlDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
		if err != nil {
			t.Fatal(err)
		}
		request := models.ContentStageRequest{PublicID: uuid.New(), TenantID: "tenant-a"}
		mock.ExpectQuery("SELECT .*content_stage_events.*event_type=.*ORDER BY sequence DESC").
			WithArgs(request.TenantID, request.PublicID, "manual_atomization_retry_approved", 1).
			WillReturnRows(sqlmock.NewRows([]string{"sequence"}).AddRow(7))
		mock.ExpectQuery("SELECT count.*content_stage_events.*sequence>.*payload->>'recovery'").
			WithArgs(request.TenantID, request.PublicID, "atomization_finalization_requeued", int64(7), "finalization_gap_requeued").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(tc.count))
		allowed, err := finalizationRecoveryAllowed(db, request)
		if err != nil || allowed != tc.allowed {
			t.Fatalf("count=%d allowed=%v err=%v", tc.count, allowed, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
		sqlDB.Close()
	}
}
