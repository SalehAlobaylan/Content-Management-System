package controllers

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"content-management-system/src/models"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Opt-in PostgreSQL tests for the Operations Command Center, following the
// Feed Integrity _db_test pattern. They pin the two things unit tests cannot:
// (1) every hand-written registry/attention SQL string actually executes against
// the real schema — the adapter contract; (2) a pause value written to a
// `timestamp` policy column round-trips close enough to the `timestamptz`
// command ledger for the resume ownership check to recognize its own pause.
// Run with OPS_TEST_DATABASE_URL pointing at a disposable test DB.
func opsTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv("OPS_TEST_DATABASE_URL"))
	if dsn == "" {
		t.Skip("set OPS_TEST_DATABASE_URL to run PostgreSQL Ops Command Center integration tests")
	}
	if !strings.Contains(strings.ToLower(dsn), "test") {
		t.Fatal("OPS_TEST_DATABASE_URL must name a disposable test database")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	if err := db.Exec("CREATE EXTENSION IF NOT EXISTS pgcrypto").Error; err != nil {
		t.Fatalf("enable pgcrypto in test database: %v", err)
	}
	// Every table the registry StatusSQL, attention adapters, and command
	// walker touch. A new roster entry whose tables are missing here will fail
	// the contract test below — that is the point.
	if err := db.AutoMigrate(
		&models.SystemAutopilotPolicy{}, &models.SystemIncidentEpisode{},
		&models.FeedIntegrityPolicy{}, &models.FeedIntegrityEpisode{}, &models.FeedIntegrityAction{},
		&models.ExperiencePolicy{}, &models.ExperienceEvaluationRun{}, &models.ExperienceIncident{},
		&models.PipelineAutopilotPolicy{}, &models.EnrichmentAutopilotPolicy{},
		&models.MediaCirculationPolicy{}, &models.MediaCirculationRecommendation{}, &models.MediaCirculationAction{},
		&models.MediaStudioAutopilotPolicy{}, &models.MediaStudioAction{},
		&models.RedundancyPolicy{}, &models.NewsCirculationPolicy{}, &models.PreferenceAutopilotPolicy{},
		&models.EmbeddingLifecyclePolicy{}, &models.EmbeddingCampaign{}, &models.EmbeddingCampaignException{},
		&models.AISpendPolicy{}, &models.AISpendEpisode{}, &models.TopicProposal{},
		&models.OpsAttentionState{}, &models.OpsBriefingCursor{}, &models.OpsFleetCommand{}, &models.OpsFleetCommandAction{},
		&models.AuditLog{},
	); err != nil {
		t.Fatalf("migrate ops test schema: %v", err)
	}
	return db
}

// The adapter contract: every lane's StatusSQL and every attention adapter SQL
// must execute without error against the live schema. Hand-written SQL strings
// only fail at runtime; this is the test that fails instead.
func TestOpsRegistryAdapterContract(t *testing.T) {
	db := opsTestDB(t)
	now := time.Now().UTC()
	for _, member := range opsFleetRegistry {
		for _, lane := range member.Lanes {
			statuses := opsStatusForLane(db, member, lane, now)
			for _, status := range statuses {
				if status.State == "errored" {
					t.Errorf("%s/%s status adapter errored: %s", member.Key, lane.Key, status.Error)
				}
			}
			if lane.PauseColumn != "" && lane.Table == "" {
				t.Errorf("%s/%s declares a pause column without a table", member.Key, lane.Key)
			}
		}
	}
	if _, errs := opsOpenAttention(db, now); len(errs) > 0 {
		t.Errorf("attention adapters errored: %v", errs)
	}
}

// Pause → resume ownership round-trip through real column types. If the
// timestamp/timestamptz precision or timezone handling ever breaks
// opsPauseValuesMatch, this fails rather than every production resume
// silently reporting skipped: foreign_pause.
func TestOpsPauseValueRoundTripThroughPolicyColumns(t *testing.T) {
	db := opsTestDB(t)
	member, lane, ok := opsFindLane("pipeline", "autopilot")
	if !ok {
		t.Fatal("pipeline/autopilot lane missing from registry")
	}
	_ = member
	tenant := "ops-roundtrip-test"
	db.Where("tenant_id = ?", tenant).Delete(&models.PipelineAutopilotPolicy{})
	policy := models.DefaultPipelineAutopilotPolicy(tenant)
	if err := db.Create(&policy).Error; err != nil {
		t.Fatalf("seed policy: %v", err)
	}
	defer db.Where("tenant_id = ?", tenant).Delete(&models.PipelineAutopilotPolicy{})

	until := time.Now().UTC().Add(45 * time.Minute)
	update := fmt.Sprintf("UPDATE %s SET %s=?, updated_at=? WHERE %s=?", lane.Table, lane.PauseColumn, lane.TenantColumn)
	if err := db.Exec(update, until, time.Now().UTC(), tenant).Error; err != nil {
		t.Fatalf("write pause: %v", err)
	}
	query := fmt.Sprintf("SELECT %s AS paused_until FROM %s WHERE %s=?", lane.PauseColumn, lane.Table, lane.TenantColumn)
	var current struct {
		PausedUntil *time.Time `gorm:"column:paused_until"`
	}
	if err := db.Raw(query, tenant).Scan(&current).Error; err != nil {
		t.Fatalf("read pause back: %v", err)
	}
	if !opsPauseValuesMatch(current.PausedUntil, &until) {
		t.Fatalf("resume ownership check does not recognize its own pause: wrote %v, read %v", until, current.PausedUntil)
	}
	foreign := until.Add(30 * time.Minute)
	if opsPauseValuesMatch(current.PausedUntil, &foreign) {
		t.Fatal("resume ownership check matched a foreign pause 30m away")
	}
}
