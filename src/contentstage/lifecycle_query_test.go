package contentstage

import (
	"strings"
	"testing"
	"time"

	"content-management-system/src/models"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func claimQueryDryRunDB(t *testing.T) *gorm.DB {
	t.Helper()
	sqlDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{
		DryRun:                 true,
		SkipDefaultTransaction: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func requireSQLContains(t *testing.T, sql string, fragments ...string) {
	t.Helper()
	normalized := strings.ToLower(sql)
	for _, fragment := range fragments {
		if !strings.Contains(normalized, strings.ToLower(fragment)) {
			t.Fatalf("SQL is missing %q:\n%s", fragment, sql)
		}
	}
}

func requireSQLExcludes(t *testing.T, sql string, fragments ...string) {
	t.Helper()
	normalized := strings.ToLower(sql)
	for _, fragment := range fragments {
		if strings.Contains(normalized, strings.ToLower(fragment)) {
			t.Fatalf("SQL unexpectedly contains %q:\n%s", fragment, sql)
		}
	}
}

func TestClaimCandidateRequiresDraftAwareWorker(t *testing.T) {
	filter := claimCandidateFilter{lane: models.ContentStageLanePods, expectedOwner: models.ContentStageOwnerAggregationPods, now: time.Now()}
	for _, supported := range []bool{false, true} {
		db := claimQueryDryRunDB(t).Set("chapter_plan_v1", supported).Session(&gorm.Session{})
		var rows []models.ContentStageRequest
		query := eligibleClaimCandidateScope(db, filter).Find(&rows)
		if supported {
			requireSQLExcludes(t, query.Statement.SQL.String(), "chapter_plan_id")
		} else {
			requireSQLContains(t, query.Statement.SQL.String(), "chapter_plan_id")
		}
	}
}

func TestClaimCandidateQueriesKeepAggregateAndLockingStatementsIndependent(t *testing.T) {
	db := claimQueryDryRunDB(t)
	filter := claimCandidateFilter{
		lane:          models.ContentStageLanePods,
		expectedOwner: models.ContentStageOwnerAggregationPods,
		allowedStages: []string{models.ContentStagePodsMediaArtifacts, models.ContentStagePodsTextEmbedding},
		now:           time.Date(2026, time.August, 29, 12, 0, 0, 0, time.UTC),
	}

	var tenants []models.ContentStageRequest
	tenantQuery := rankedClaimTenantScope(db, filter).Find(&tenants)
	if tenantQuery.Error != nil {
		t.Fatal(tenantQuery.Error)
	}
	tenantSQL := tenantQuery.Statement.SQL.String()
	requireSQLContains(t, tenantSQL,
		"from \"content_stage_requests\"", "lane=", "owner=", "state in", "not_before_at",
		"cancellation_requested_at is null", "stage in", "blocking_scope<>", "group by",
		"max(csa.created_at)", "min(content_stage_requests.created_at)", "limit",
		"pods_episode_dispositions", "d.disposition<>'active'",
		"item_media.state =",
	)
	requireSQLExcludes(t, tenantSQL, "for update", "skip locked")

	rowFilter := filter
	rowFilter.tenantID = "tenant-a"
	var rows []models.ContentStageRequest
	rowQuery := lockedClaimCandidateScope(db, rowFilter, 1).Find(&rows)
	if rowQuery.Error != nil {
		t.Fatal(rowQuery.Error)
	}
	rowSQL := rowQuery.Statement.SQL.String()
	requireSQLContains(t, rowSQL,
		"from \"content_stage_requests\"", "lane=", "owner=", "state in", "not_before_at",
		"cancellation_requested_at is null", "tenant_id=", "stage in", "blocking_scope<>",
		"priority desc, created_at asc, public_id asc", "limit", "for update skip locked",
		"pods_episode_execution_slot", "coalesce(leaf.parent_content_item_id,leaf.public_id)",
		"pods_episode_dispositions",
		"item_media.state =",
	)
	requireSQLExcludes(t, rowSQL, "group by", "max(csa.created_at)", "min(content_stage_requests.created_at)")
}

func TestClaimCandidateScopeKeepsRequiredAndOptionalFiltersExclusive(t *testing.T) {
	db := claimQueryDryRunDB(t)
	base := claimCandidateFilter{
		tenantID:      "tenant-a",
		lane:          models.ContentStageLaneNews,
		expectedOwner: models.ContentStageOwnerAggregationNews,
		allowedStages: []string{models.ContentStageNewsTextEmbedding},
		now:           time.Date(2026, time.August, 29, 12, 0, 0, 0, time.UTC),
	}

	var requiredRows []models.ContentStageRequest
	required := eligibleClaimCandidateScope(db, base).Find(&requiredRows)
	if required.Error != nil {
		t.Fatal(required.Error)
	}
	requireSQLContains(t, required.Statement.SQL.String(), "blocking_scope<>")
	requireSQLExcludes(t, required.Statement.SQL.String(), "earlier_item.content_source_id")

	base.optional = true
	var optionalRows []models.ContentStageRequest
	optional := eligibleClaimCandidateScope(db, base).Find(&optionalRows)
	if optional.Error != nil {
		t.Fatal(optional.Error)
	}
	requireSQLContains(t, optional.Statement.SQL.String(), "blocking_scope=")
	requireSQLExcludes(t, optional.Statement.SQL.String(), "earlier_item.content_source_id")
}

func TestClaimNextAnyTenantTreatsAbsentSchemaAsCompatibilityNoWork(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{SkipDefaultTransaction: true})
	if err != nil {
		t.Fatal(err)
	}
	ResetSchemaAvailabilityCache()
	t.Cleanup(ResetSchemaAvailabilityCache)
	mock.ExpectQuery(`SELECT count\(\*\) FROM information_schema\.tables WHERE table_schema = CURRENT_SCHEMA\(\) AND table_name = \$1 AND table_type = \$2`).
		WithArgs("content_stage_requests", "BASE TABLE").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	envelope, found, err := ClaimNextAnyTenant(db, models.ContentStageLaneNews, "news-test")
	if err != nil {
		t.Fatal(err)
	}
	if found || envelope.RequestID != uuid.Nil {
		t.Fatalf("schema-pending claim must be an empty compatibility result: %#v", envelope)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
