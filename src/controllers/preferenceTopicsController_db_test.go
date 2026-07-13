package controllers

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"content-management-system/src/models"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// preferenceTestDB is deliberately opt-in: it migrates and clears only a
// disposable database. It is the shared fixture for Preference plans 023–030.
func preferenceTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv("PREFERENCES_TEST_DATABASE_URL"))
	if dsn == "" {
		t.Skip("set PREFERENCES_TEST_DATABASE_URL to run Preferences PostgreSQL tests")
	}
	if err := validatePreferencesTestDSN(dsn); err != nil {
		t.Fatal(err)
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	for _, extension := range []string{"vector", "pgcrypto"} {
		if err := db.Exec("CREATE EXTENSION IF NOT EXISTS " + extension).Error; err != nil {
			t.Fatalf("enable %s extension: %v", extension, err)
		}
	}
	if err := db.AutoMigrate(
		&models.TopicCategory{}, &models.Topic{}, &models.TopicProposal{}, &models.ContentItemTopic{}, &models.StoryTopic{},
		&models.UserTopicPref{}, &models.UserTopicAffinity{}, &models.UserCategoryAffinity{}, &models.PreferenceSettings{}, &models.PreferenceStat{},
		&models.PreferenceAutopilotPolicy{}, &models.PreferenceAutopilotRun{}, &models.PreferenceAutopilotAction{}, &models.PreferenceAffinityRecomputeQueue{},
	); err != nil {
		t.Fatalf("migrate Preferences test schema: %v", err)
	}
	clear := func() {
		for _, table := range []string{
			"preference_autopilot_actions", "preference_autopilot_runs", "preference_affinity_recompute_queue", "preference_autopilot_policies",
			"user_category_affinity", "user_topic_affinity", "user_topic_prefs", "story_topics", "content_item_topics", "topic_proposals", "topics", "topic_categories", "preference_stats", "preference_settings",
		} {
			_ = db.Exec("DELETE FROM " + table).Error
		}
	}
	clear()
	t.Cleanup(clear)
	return db
}

func validatePreferencesTestDSN(dsn string) error {
	if !strings.Contains(strings.ToLower(dsn), "test") {
		return fmt.Errorf("PREFERENCES_TEST_DATABASE_URL must name a disposable test database")
	}
	return nil
}

func TestPreferenceDSNSafetyGuard(t *testing.T) {
	if err := validatePreferencesTestDSN("postgres://localhost/preferences"); err == nil {
		t.Fatal("non-test DSN must be rejected before opening a database")
	}
	if err := validatePreferencesTestDSN("postgres://localhost/preferences_test"); err != nil {
		t.Fatalf("test DSN rejected: %v", err)
	}
}

func TestPreferenceDB_HarnessSmoke(t *testing.T) {
	db := preferenceTestDB(t)
	policy := models.DefaultPreferenceAutopilotPolicy("default")
	if err := db.Create(&policy).Error; err != nil {
		t.Fatal(err)
	}
	var stored models.PreferenceAutopilotPolicy
	if err := db.Where("tenant_id = ?", "default").First(&stored).Error; err != nil {
		t.Fatal(err)
	}
}
