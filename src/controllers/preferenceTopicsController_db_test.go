package controllers

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"content-management-system/src/models"
	"content-management-system/src/tests/testdb"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// preferenceTestDB uses the shared disposable fixture for Preference plans 023–030.
func preferenceTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := os.Getenv("PREFERENCES_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set guarded PREFERENCES_TEST_DATABASE_URL to run Preferences DB tests")
	}
	if _, err := testdb.ValidateDisposableDSN(dsn, os.Getenv("DATABASE_URL"), os.Getenv("CMS_TEST_DISPOSABLE")); err != nil {
		t.Fatalf("refusing Preferences test database before opening a connection: %v", err)
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open guarded Preferences test database: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("access Preferences test connection: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := db.Exec("CREATE EXTENSION IF NOT EXISTS pgcrypto").Error; err != nil {
		t.Fatalf("enable pgcrypto in guarded Preferences test database: %v", err)
	}
	if err := db.Exec("CREATE EXTENSION IF NOT EXISTS vector").Error; err != nil {
		t.Fatalf("enable pgvector in guarded Preferences test database: %v", err)
	}
	if err := db.AutoMigrate(
		&models.TopicCategory{}, &models.Topic{}, &models.TopicProposal{}, &models.ContentItemTopic{}, &models.StoryTopic{},
		&models.ContentItem{}, &models.UserInteraction{}, &models.Story{},
		&models.UserTopicPref{}, &models.UserTopicAffinity{}, &models.UserCategoryAffinity{}, &models.PreferenceSettings{}, &models.PreferenceStat{},
		&models.PreferenceAutopilotPolicy{}, &models.PreferenceAutopilotRun{}, &models.PreferenceAutopilotAction{}, &models.PreferenceAffinityRecomputeQueue{},
	); err != nil {
		t.Fatalf("migrate Preferences test schema: %v", err)
	}
	clear := func() {
		for _, table := range []string{
			"preference_autopilot_actions", "preference_autopilot_runs", "preference_affinity_recompute_queue", "preference_autopilot_policies",
			"user_category_affinity", "user_topic_affinity", "user_topic_prefs", "story_topics", "content_item_topics", "topic_proposals", "topics", "topic_categories", "preference_stats", "preference_settings",
			"user_interactions", "content_items", "stories",
		} {
			_ = db.Exec("DELETE FROM " + table).Error
		}
	}
	clear()
	t.Cleanup(clear)
	return db
}

// preferenceTestDBConnection opens a second independent connection to the
// already-migrated disposable database. Race fixtures use it to model separate
// CMS replicas without wiping the first fixture's state.
func preferenceTestDBConnection(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := os.Getenv("PREFERENCES_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set guarded PREFERENCES_TEST_DATABASE_URL to run Preferences DB tests")
	}
	if _, err := testdb.ValidateDisposableDSN(dsn, os.Getenv("DATABASE_URL"), os.Getenv("CMS_TEST_DISPOSABLE")); err != nil {
		t.Fatalf("refusing Preferences test database before opening a second connection: %v", err)
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open second guarded Preferences test connection: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("access second Preferences test connection: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	return db
}

func preferenceRequest(db *gorm.DB, method, target, body string, userID uuid.UUID) (*gin.Context, *httptest.ResponseRecorder) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(method, target, bytes.NewBufferString(body))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Set("db", db)
	c.Set("user_id", userID.String())
	return c, w
}

func createPreferenceTopic(t *testing.T, db *gorm.DB, slug string, active, featured bool) models.Topic {
	t.Helper()
	topic := models.Topic{TenantID: "default", Slug: slug, LabelAR: slug, LabelEN: slug, CategorySlug: "general", Active: active, Featured: featured}
	if err := db.Create(&topic).Error; err != nil {
		t.Fatal(err)
	}
	return topic
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

func TestPreferenceDB_DSNGuardRejectsUnsafeTargetsBeforeOpen(t *testing.T) {
	marker := "I_UNDERSTAND_THIS_DATABASE_IS_DISPOSABLE"
	if _, err := testdb.ValidateDisposableDSN("postgres://user:pass@localhost:5432/postgres?sslmode=disable", "", marker); err == nil {
		t.Fatal("non-disposable Preferences database must be rejected")
	}
	if _, err := testdb.ValidateDisposableDSN("postgres://user:pass@db.neon.tech:5432/wahb_cms_test_12345678?sslmode=require", "", marker); err == nil {
		t.Fatal("managed Preferences database host must be rejected")
	}
	if _, err := testdb.ValidateDisposableDSN("postgres://user:pass@localhost:5432/wahb_cms_test_12345678?sslmode=disable", "", marker); err != nil {
		t.Fatalf("guarded disposable Preferences target rejected: %v", err)
	}
}

func TestPreferenceDB_VisibleReplacementPreservesHiddenDeclarations(t *testing.T) {
	db := preferenceTestDB(t)
	userID := uuid.New()
	oldVisible := createPreferenceTopic(t, db, "old-visible", true, true)
	newVisible := createPreferenceTopic(t, db, "new-visible", true, true)
	hidden := createPreferenceTopic(t, db, "hidden", false, true)
	for _, topic := range []models.Topic{oldVisible, hidden} {
		if err := db.Create(&models.UserTopicPref{TenantID: "default", UserID: userID, TopicID: topic.PublicID, State: "declared"}).Error; err != nil {
			t.Fatal(err)
		}
	}
	c, w := preferenceRequest(db, http.MethodPut, "/preferences/topics", `{"topic_ids":["`+newVisible.PublicID.String()+`"]}`, userID)
	PutPreferenceTopics(c)
	if w.Code != http.StatusOK {
		t.Fatalf("replace status=%d body=%s", w.Code, w.Body.String())
	}
	var prefs []models.UserTopicPref
	if err := db.Where("tenant_id = ? AND user_id = ?", "default", userID).Find(&prefs).Error; err != nil {
		t.Fatal(err)
	}
	states := map[uuid.UUID]string{}
	for _, pref := range prefs {
		states[pref.TopicID] = pref.State
	}
	if _, exists := states[oldVisible.PublicID]; exists {
		t.Fatal("visible declaration was not replaced")
	}
	if states[newVisible.PublicID] != "declared" || states[hidden.PublicID] != "declared" {
		t.Fatalf("replacement must add the requested visible declaration and preserve hidden state: %+v", states)
	}
}

func TestPreferenceDB_MuteWinsOverExistingAffinity(t *testing.T) {
	db := preferenceTestDB(t)
	userID := uuid.New()
	topic := createPreferenceTopic(t, db, "muted-topic", true, true)
	if err := db.Create(&models.UserTopicAffinity{TenantID: "default", UserID: userID, TopicID: topic.PublicID, Score: 0.9}).Error; err != nil {
		t.Fatal(err)
	}
	c, w := preferenceRequest(db, http.MethodPost, "/preferences/topics/"+topic.PublicID.String()+"/mute", "", userID)
	c.Params = gin.Params{{Key: "id", Value: topic.PublicID.String()}}
	MutePreferenceTopic(c)
	if w.Code != http.StatusOK {
		t.Fatalf("mute status=%d body=%s", w.Code, w.Body.String())
	}
	var pref models.UserTopicPref
	if err := db.Where("tenant_id = ? AND user_id = ? AND topic_id = ?", "default", userID, topic.PublicID).First(&pref).Error; err != nil || pref.State != "muted" {
		t.Fatalf("mute was not persisted: %+v err=%v", pref, err)
	}
	var affinityCount int64
	if err := db.Model(&models.UserTopicAffinity{}).Where("tenant_id = ? AND user_id = ? AND topic_id = ?", "default", userID, topic.PublicID).Count(&affinityCount).Error; err != nil {
		t.Fatal(err)
	}
	if affinityCount != 0 {
		t.Fatalf("muted topic retained %d positive affinity rows", affinityCount)
	}
}

func TestPreferenceDB_UnmuteRemovesOnlyMuteDeclaration(t *testing.T) {
	db := preferenceTestDB(t)
	userID := uuid.New()
	topic := createPreferenceTopic(t, db, "unmute-topic", true, true)
	if err := db.Create(&models.UserTopicPref{TenantID: "default", UserID: userID, TopicID: topic.PublicID, State: "muted"}).Error; err != nil {
		t.Fatal(err)
	}
	c, w := preferenceRequest(db, http.MethodDelete, "/preferences/topics/"+topic.PublicID.String()+"/mute", "", userID)
	c.Params = gin.Params{{Key: "id", Value: topic.PublicID.String()}}
	UnmutePreferenceTopic(c)
	if w.Code != http.StatusOK {
		t.Fatalf("unmute status=%d body=%s", w.Code, w.Body.String())
	}
	var count int64
	if err := db.Model(&models.UserTopicPref{}).Where("tenant_id = ? AND user_id = ? AND topic_id = ?", "default", userID, topic.PublicID).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("unmute retained %d preference row(s)", count)
	}
}

func TestPreferenceDB_ReplacementRejectsNonPickerTopic(t *testing.T) {
	db := preferenceTestDB(t)
	userID := uuid.New()
	hidden := createPreferenceTopic(t, db, "not-a-picker-topic", true, false)
	c, w := preferenceRequest(db, http.MethodPut, "/preferences/topics", `{"topic_ids":["`+hidden.PublicID.String()+`"]}`, userID)
	PutPreferenceTopics(c)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("non-picker replacement status=%d body=%s", w.Code, w.Body.String())
	}
	var count int64
	if err := db.Model(&models.UserTopicPref{}).Where("tenant_id = ? AND user_id = ?", "default", userID).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("invalid picker replacement wrote %d preference row(s)", count)
	}
}

func TestPreferenceDB_EmptyReplacementClearsVisibleDeclarationsOnly(t *testing.T) {
	db := preferenceTestDB(t)
	userID := uuid.New()
	visible := createPreferenceTopic(t, db, "visible-to-clear", true, true)
	hidden := createPreferenceTopic(t, db, "hidden-to-preserve", false, true)
	for _, topic := range []models.Topic{visible, hidden} {
		if err := db.Create(&models.UserTopicPref{TenantID: "default", UserID: userID, TopicID: topic.PublicID, State: "declared"}).Error; err != nil {
			t.Fatal(err)
		}
	}
	c, w := preferenceRequest(db, http.MethodPut, "/preferences/topics", `{"topic_ids":[]}`, userID)
	PutPreferenceTopics(c)
	if w.Code != http.StatusOK {
		t.Fatalf("empty replacement status=%d body=%s", w.Code, w.Body.String())
	}
	var prefs []models.UserTopicPref
	if err := db.Where("tenant_id = ? AND user_id = ?", "default", userID).Find(&prefs).Error; err != nil {
		t.Fatal(err)
	}
	if len(prefs) != 1 || prefs[0].TopicID != hidden.PublicID || prefs[0].State != "declared" {
		t.Fatalf("empty replacement must preserve only hidden declaration: %+v", prefs)
	}
}

func TestPreferenceDB_DeclaredPreferenceCreatesCategoryAffinity(t *testing.T) {
	db := preferenceTestDB(t)
	userID := uuid.New()
	topic := createPreferenceTopic(t, db, "category-affinity", true, true)
	topic.CategorySlug = "science"
	if err := db.Save(&topic).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&models.UserTopicPref{TenantID: "default", UserID: userID, TopicID: topic.PublicID, State: "declared"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := recomputeUserAffinity(db, userID, "default"); err != nil {
		t.Fatalf("recompute declared preference: %v", err)
	}
	var affinity models.UserCategoryAffinity
	if err := db.Where("tenant_id = ? AND user_id = ? AND category_slug = ?", "default", userID, "science").First(&affinity).Error; err != nil {
		t.Fatalf("load category affinity: %v", err)
	}
	if affinity.Score <= 0 {
		t.Fatalf("declared topic category affinity=%f, want positive", affinity.Score)
	}
}

// The fixtures below are intentionally disabled until the plans that correct
// their known failure modes land. Keeping them close to the live endpoint tests
// makes the required concurrency contract executable without normalizing the
// unsafe behavior as a passing assertion.

func TestPreferenceDB_AffinityRollbackOnDerivedWriteFailure(t *testing.T) {
	t.Skip("enabled by plan 024: derived affinity replacement must be atomic")
}

func TestPreferenceDB_AnonymousAndDisabledFeedHooksAvoidPreferenceQueries(t *testing.T) {
	t.Skip("enabled by plan 025: feed-off and anonymous preference query budget")
}

func TestPreferenceDB_ConcurrentRunClaimsOneTenant(t *testing.T) {
	t.Skip("enabled by plan 026: database-global scheduler claim fixture")
	db := preferenceTestDB(t)
	db2 := preferenceTestDBConnection(t)
	release1, acquired1 := tryAcquirePreferenceAutopilotLock(db, "default")
	defer release1()
	release2, acquired2 := tryAcquirePreferenceAutopilotLock(db2, "default")
	defer release2()
	if acquired1 == acquired2 {
		t.Fatalf("same-tenant race winners=%t,%t; want exactly one", acquired1, acquired2)
	}
}

func TestPreferenceDB_RecentUsersOverflowDrainsAcrossRuns(t *testing.T) {
	t.Skip("enabled by plan 026: capped recent-user cursor fixture")
}

func TestPreferenceDB_BaselineFailureKeepsCheckpointRetryable(t *testing.T) {
	t.Skip("enabled by plan 026: structured baseline failure fixture")
}

func TestPreferenceDB_ConcurrentHumanResolutionDoesNotAutoApprove(t *testing.T) {
	t.Skip("enabled by plan 027: human/automation approval race fixture")
}

func TestPreferenceDB_ConcurrentSlugCreationDoesNotOverwriteTopic(t *testing.T) {
	t.Skip("enabled by plan 027: machine approval must use conflict-do-nothing")
}
