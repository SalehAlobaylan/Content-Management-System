package integration

import (
	"content-management-system/src/models"
	"content-management-system/src/routes"
	"content-management-system/src/tests/testdb"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

var (
	testDB             *gorm.DB
	router             *gin.Engine
	integrationCleanup func() error
)

func TestMain(m *testing.M) {
	if err := setup(); err != nil {
		fmt.Fprintln(os.Stderr, "integration test setup failed:", err)
		cleanup()
		os.Exit(1)
	}
	code := m.Run()
	cleanup()
	os.Exit(code)
}

func setup() error {
	gin.SetMode(gin.TestMode)
	os.Setenv("JWT_SECRET", "test_secret")
	os.Setenv("CMS_SERVICE_TOKEN", "")
	os.Setenv("CMS_AGGREGATION_SERVICE_TOKEN", integrationAggregationToken)
	os.Setenv("CMS_ENRICHMENT_SERVICE_TOKEN", integrationEnrichmentToken)
	os.Setenv("CMS_MEDIA_SERVICE_TOKEN", integrationMediaToken)

	// testdb validates before opening a connection and creates a fresh database
	// whenever the CI/local admin URL is available. It never reads service .env.
	var cleanupFn func() error
	var err error
	testDB, cleanupFn, err = testdb.OpenForMain()
	if err != nil {
		return errors.New("failed to connect disposable integration database")
	}
	integrationCleanup = cleanupFn

	// Temporary test schema only. Plan 091 replaces this with canonical
	// migrations; this fixture does not claim migration coverage.
	if err := testDB.AutoMigrate(
		&models.Page{},
		&models.Media{},
		&models.Post{},
		// Wahb Platform models
		&models.ContentItem{},
		&models.Transcript{},
		&models.TranscriptQuality{},
		&models.UserInteraction{},
		&models.ContentSource{},
		// Phase 13 — story feed
		&models.RankingConfig{},
		&models.ContentFlag{},
		// Temporary fixture support for internal vector write fencing.
		&models.EmbeddingCampaign{},
		&models.Story{},
		&models.NewsSnapshot{},
		&models.NewsCirculationPolicy{},
		&models.NewsStoryOverride{},
		&models.SourceRunTelemetry{},
		&models.SourceCirculationRecommendation{},
		// Ranking/Intelligence (stage 4) — feed serve-side telemetry writes these
		&models.MediaIntelligenceScore{},
		&models.MediaDemandStat{},
	); err != nil {
		return errors.New("failed to prepare disposable integration schema")
	}

	router = gin.Default()
	router.Use(func(c *gin.Context) {
		c.Set("db", testDB)
		c.Next()
	})

	v1 := router.Group("/api/v1")
	routes.SetupPostRoutes(v1, testDB)
	routes.SetupMediaRoutes(v1, testDB)
	routes.SetupPageRoutes(v1, testDB)
	// Wahb Platform routes
	routes.SetupFeedRoutes(v1, testDB)
	routes.SetupInteractionRoutes(v1, testDB)
	routes.SetupContentRoutes(v1, testDB)
	routes.SetupAdminAuthRoutes(router, testDB)
	routes.SetupInternalRoutes(router, testDB)
	return nil
}

func cleanup() {
	if integrationCleanup != nil {
		if err := integrationCleanup(); err != nil {
			fmt.Fprintln(os.Stderr, "integration database cleanup failed:", err)
		}
	}
}

func setDefaultEnvIfEmpty(key, value string) {
	if os.Getenv(key) == "" {
		_ = os.Setenv(key, value)
	}
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func clearTables() {
	if testDB == nil {
		return
	}
	_ = testDB.Exec("DELETE FROM post_media").Error
	_ = testDB.Exec("DELETE FROM posts").Error
	_ = testDB.Exec("DELETE FROM media").Error
	_ = testDB.Exec("DELETE FROM pages").Error
}
