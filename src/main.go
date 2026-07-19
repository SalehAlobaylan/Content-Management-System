package main

import (
	"content-management-system/src/controllers"
	"content-management-system/src/intelligence"
	"content-management-system/src/models" // needs it for automigrate
	"content-management-system/src/routes"
	"content-management-system/src/utils"

	"log"
	"net/url"
	"os"
	"strings"
	"time"

	// "fmt"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	_ "github.com/joho/godotenv/autoload"
	"gorm.io/gorm"
)

func SetupRoutes(router *gin.Engine, db *gorm.DB) {
	// Welcome endpoint
	router.GET("/", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "Welcome to Content Management System API",
			"version": "1.1.0",
			"endpoints": gin.H{
				"health":        "/health",
				"api":           "/api/v1",
				"posts":         "/api/v1/posts",
				"media":         "/api/v1/media",
				"pages":         "/api/v1/pages",
				"feed_foryou":   "/api/v1/feed/foryou",
				"feed_news":     "/api/v1/feed/news",
				"content":       "/api/v1/content/:id",
				"interactions":  "/api/v1/interactions",
				"documentation": "/docs",
			},
		})
	})

	// Health check endpoint
	router.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"status": "ok",
		})
	})

	router.Use(func(c *gin.Context) {
		c.Set("db", db)
		c.Next()
	})

	v1 := router.Group("/api/v1")
	routes.SetupPostRoutes(v1, db)
	routes.SetupMediaRoutes(v1, db)
	routes.SetupPageRoutes(v1, db)

	// Wahb Platform routes
	routes.SetupFeedRoutes(v1, db)
	routes.SetupInteractionRoutes(v1, db)
	routes.SetupContentRoutes(v1, db)
	routes.SetupTranscriptRoutes(v1, db)
	routes.SetupPreferenceRoutes(v1, db)
	// Real User Experience — public RUX telemetry ingest (BFF-token guarded)
	routes.SetupExperienceRoutes(v1, db)

	// Internal service-to-service routes
	routes.SetupInternalRoutes(router, db)

}

func main() {
	log.Println("Starting Content Management System...")
	log.Printf("Environment: %s", os.Getenv("ENV"))
	logCMSConnectionTargets()

	if _, err := utils.GetJWTSecret(); err != nil {
		log.Fatalf("Refusing to start: %v. Set JWT_SECRET to the shared value used by IAM.", err)
	}

	db, err := utils.ConnectDB()
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	log.Println("Successfully connected to database")
	if err := utils.CheckSchemaReadiness(db); err != nil {
		log.Fatalf("CMS schema is not ready: %v", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		log.Fatalf("Failed to get database instance: %v", err)
	}

	defer sqlDB.Close()

	env := os.Getenv("ENV")
	if env == "" { //if env is not set, set it to development as default
		env = "development"
	}

	// Run migrations only in development environments — and only when
	// AUTO_MIGRATE != "false". GORM's AutoMigrate introspects every column of
	// every model (hundreds of information_schema round-trips); against a
	// remote DB like Neon (~0.3-0.6s RTT each) that turns boot into multiple
	// minutes and start.sh times out. Schema changes are tracked as SQL files
	// in migrations/ and applied directly, so day-to-day boots can
	// skip the sweep (set AUTO_MIGRATE=false in .env.local). Unset = migrate,
	// the safe default for fresh setups.
	// Note: In production, use manual migrations or migration tools to avoid conflicts
	if false { // Legacy AutoMigrate implementation retained temporarily while plan 091 moves every model to canonical SQL; it is never reachable at startup.
		log.Println("Migrating database...")
		if err := utils.AutoMigrate(db,
			&models.Page{},
			&models.Post{},
			&models.Media{},
			// Wahb Platform models
			&models.ContentItem{},
			&models.Transcript{},
			&models.UserInteraction{},
			&models.ContentSource{},
			// Intelligence / Ranking
			&models.RankingConfig{},
			&models.ContentFlag{},
			// Media — transcription/STT config (auto-STT toggle + budget)
			&models.TranscriptionConfig{},
			&models.TranscriptionJob{},
			&models.TranscriptionBatch{},
			&models.TranscriptionBatchItem{},
			&models.TranscriptQuality{},
			&models.TranscriptVersion{},
			// Media Studio — first-class editable chapters
			&models.Chapter{},
			&models.MediaAtomizationPolicy{},
			&models.MediaAtomizationRun{},
			// First-class topics (LLM-labeled + centroid embedding)
			&models.Story{},
			// Canonical preference topic catalog + derived user affinity
			&models.TopicCategory{},
			&models.Topic{},
			&models.TopicProposal{},
			&models.ContentItemTopic{},
			&models.StoryTopic{},
			&models.UserTopicPref{},
			&models.UserTopicAffinity{},
			&models.UserCategoryAffinity{},
			&models.PreferenceSettings{},
			&models.PreferenceStat{},
			// Preferences Autopilot (stage 7) — bounded catalog-maintenance + proposal advisor
			&models.PreferenceAutopilotPolicy{},
			&models.PreferenceAutopilotRun{},
			&models.PreferenceAutopilotAction{},
			&models.PreferenceAffinityRecomputeQueue{},
			// Feeds Finding — auto source discovery (profiles + suggestions + config)
			&models.DiscoveryProfile{},
			&models.SourceSuggestion{},
			&models.DiscoveryConfig{},
			// Slice 4 — Source Intelligence Graph (ledger + citation edges)
			&models.SourceCandidate{},
			&models.SourceEdge{},
			// Phase 13 — precomputed News-feed story-slide snapshot
			&models.NewsSnapshot{},
			// News Circulation Engine — story windows, overrides, source cadence
			&models.NewsCirculationPolicy{},
			&models.NewsStoryOverride{},
			&models.SourceRunTelemetry{},
			&models.SourceCirculationRecommendation{},
			&models.NewsAutopilotRun{},
			&models.NewsAutopilotAction{},
			// Saved syndication feeds (RSS/Atom/JSON)
			&models.RSSFeed{},
			// Storage management
			&models.StoragePolicy{},
			&models.StorageSweepRun{},
			&models.StorageOpMetric{},
			&models.MediaStorageArtifactEvent{},
			// Media Circulation Engine — advisory verdict/recommendation layer
			&models.MediaCirculationPolicy{},
			&models.MediaCirculationRecommendation{},
			&models.MediaCirculationOverride{},
			&models.MediaCirculationRun{},
			&models.MediaCirculationAction{},
			&models.RedundancyPolicy{},
			&models.RedundancyRun{},
			&models.RedundancyPair{},
			&models.RedundancyPairEvaluation{},
			&models.RedundancyFamily{},
			&models.RedundancyFamilyMember{},
			&models.RedundancyAction{},
			&models.RedundancyFingerprint{},
			// Enrichment Coverage Autopilot — scheduled gap-filling supervisor
			&models.EnrichmentAutopilotPolicy{},
			&models.EnrichmentAutopilotRun{},
			&models.EnrichmentAutopilotAction{},
			// Pipeline Repair Autopilot — bounded retry/repair supervisor
			&models.PipelineAutopilotPolicy{},
			&models.PipelineAutopilotRun{},
			&models.PipelineAutopilotAction{},
			// Media Studio Clearance Autopilot (stage 6) — editorial-clearance helper
			&models.MediaStudioAutopilotPolicy{},
			&models.MediaStudioRun{},
			&models.MediaStudioAction{},
			// System Health / Incident Autopilot — platform probe ledger + containment
			&models.SystemAutopilotPolicy{},
			&models.SystemIncidentEpisode{},
			&models.SystemAutopilotRun{},
			&models.SystemAutopilotAction{},
			// Ranking/Intelligence System (stage 4) — persisted value surface +
			// serve-side demand telemetry + per-tenant tuning overrides
			&models.MediaIntelligenceScore{},
			&models.MediaDemandStat{},
			&models.MediaIntelligenceConfig{},
			// Quality management — ingest configuration only.
			// quality_rules and quality_history were removed in Phase 7;
			// re-encoding is now driven by storage policies (archive_action='re_encode').
			// The two tables remain in the dev DB for now — drop manually after confirming.
			&models.QualityProfile{},
			// Audit log — admin actions executed from Platform-Console
			&models.AuditLog{},
			// Embedding & Model Lifecycle System (stage 10) — audit persistence
			&models.EmbeddingLifecyclePolicy{},
			&models.EmbeddingLifecycleRun{},
			&models.EmbeddingLifecycleFinding{},
			&models.EmbeddingCampaign{},
			&models.EmbeddingCampaignAction{},
			&models.EmbeddingCampaignException{},
			// AI Spend & Economics Governor (stage 11) — ledger and policy.
			&models.AISpendEvent{},
			&models.AIPriceBook{},
			&models.AISpendRollup{},
			&models.AISpendPolicy{},
			&models.AISpendBudget{},
			&models.AISpendRun{},
			&models.AISpendEpisode{},
			// Operations Command Center
			&models.OpsAttentionState{},
			&models.OpsBriefingCursor{},
			&models.OpsFleetCommand{},
			&models.OpsFleetCommandAction{},
		); err != nil {
			log.Fatalf("Failed to migrate database: %v", err)
		}
		// Seed default quality profiles if the table is empty.
		if err := utils.SeedDefaultQualityProfiles(db); err != nil {
			log.Fatalf("Failed to seed quality profiles: %v", err)
		}
		// Seed development data
		if env == "development" || env == "dev" {
			if err := utils.SeedData(db); err != nil {
				log.Fatalf("Failed to seed data: %v", err)
			}
			// Seed Wahb Platform content
			if err := utils.SeedWahbData(db); err != nil {
				log.Fatalf("Failed to seed Wahb data: %v", err)
			}
		}
	} else if env == "development" || env == "dev" {
		log.Println("Skipping GORM AutoMigrate because AUTO_MIGRATE=false")
		log.Println("SQL migrations are not run by CMS startup; apply them explicitly with: go run ./cmd/migrate <migration.sql>")
	}
	if env == "production" {
		gin.SetMode(gin.ReleaseMode)
	}

	controllers.ResumeTranscriptionBatches(db)

	router := gin.Default()

	// Configure CORS to allow all origins
	router.Use(cors.New(cors.Config{
		AllowAllOrigins:  true,
		AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Length", "Content-Type", "Authorization", "Idempotency-Key"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: false,
		MaxAge:           12 * time.Hour,
	}))

	SetupRoutes(router, db)
	routes.SetupAdminAuthRoutes(router, db)
	logCMSAuthConfig()

	// Self-heal classification drift: classify any embedded-but-unclassified
	// NEWS items (LLM outages, bulk re-embeds, taxonomy wipes) and rebuild the
	// precompute News snapshot when done. Runs in the background.
	controllers.StartClassificationBackfill(db)
	// Preferences Autopilot scheduler (stage 7) — REPLACES the bare topics
	// heartbeat. Disabled tenants get the incumbent catalog maintenance exactly;
	// enabled tenants get the bounded, ledgered runner with a health verdict.
	controllers.StartPreferenceAutopilotHeartbeat(db)
	// Precompute missing topics.related_ids (stories predating the write-time
	// related feature) so feed reads never fall back to per-slide centroid kNN.
	controllers.StartRelatedBackfill(db)
	// News Circulation automation heartbeat — periodically recompute source
	// cadence recommendations (and auto-apply inside guardrails) for tenants that
	// opted in, so the news pipeline self-tunes without manual admin triggers.
	controllers.StartCirculationAutomation(db)
	// Ranking/Intelligence refresh heartbeat — recomputes stale/nudged media
	// value scores in bounded batches (stage 4; scheduled + event-nudged
	// triggers in one pass, on-demand scoring happens inside circulation).
	intelligence.StartRefreshLoop(db)
	// Media Circulation Autopilot heartbeat (stage 5) — fires deterministic
	// runs for tenants whose autopilot interval has elapsed; Observe tenants
	// get shadow (dry-run) ledgers, Safe Auto tenants get bounded execution.
	controllers.StartMediaCirculationAutopilotHeartbeat(db)
	controllers.StartRedundancyHygieneHeartbeat(db)
	controllers.StartEnrichmentAutopilotHeartbeat(db)
	controllers.StartPipelineAutopilotHeartbeat(db)
	// Media Studio Clearance Autopilot (stage 6) — chain-first heartbeat: fires
	// after the lead executes atomize_now, plus a slower interval sweep-up.
	controllers.StartMediaStudioAutopilotHeartbeat(db)
	// System Health / Incident Autopilot — CMS-owned probes + incident ledger.
	controllers.StartSystemHealthAutopilotHeartbeat(db)
	// Feed Integrity base system — deterministic CMS-edge verification, not an Autopilot.
	controllers.StartFeedIntegrityHeartbeat(db)
	// Real User Experience — Observe scheduler: rolls up closed telemetry buckets
	// and evaluates deterministic surface verdicts for tenants that enabled it.
	controllers.StartExperienceHeartbeat(db)
	// Embedding & Model Lifecycle (stage 10) — vector-space audit scheduler.
	// Observation only; disabled by default until an admin enables it.
	controllers.StartEmbeddingLifecycleHeartbeat(db)
	controllers.StartAISpendGovernorHeartbeat(db)

	serverAddr := cmsServerAddress()
	log.Printf("Starting server on %s...", serverAddr)
	if err := router.Run(serverAddr); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

}

func logCMSAuthConfig() {
	jwtIssuer := strings.TrimSpace(os.Getenv("JWT_ISSUER"))
	if jwtIssuer == "" {
		jwtIssuer = "cms-service"
	}

	allowedIssuers := strings.TrimSpace(os.Getenv("JWT_ALLOWED_ISSUERS"))
	if allowedIssuers == "" {
		allowedIssuers = "cms-service,iam-authorization-service"
	}

	requireTenant := strings.TrimSpace(os.Getenv("JWT_REQUIRE_TENANT_ID"))
	if requireTenant == "" {
		requireTenant = "false"
	}

	defaultTenant := strings.TrimSpace(os.Getenv("DEFAULT_TENANT_ID"))
	if defaultTenant == "" {
		defaultTenant = "default"
	}

	normalizedAllowlist := strings.ToLower(allowedIssuers)
	mode := "compatibility (accepts CMS + IAM issuers)"
	if strings.Contains(normalizedAllowlist, "iam-authorization-service") && !strings.Contains(normalizedAllowlist, "cms-service") {
		mode = "IAM verifier-only"
	}

	log.Println("[CMS] Auth verifier config")
	log.Printf("[CMS] - JWT_ISSUER=%s", jwtIssuer)
	log.Printf("[CMS] - JWT_ALLOWED_ISSUERS=%s", allowedIssuers)
	log.Printf("[CMS] - JWT_REQUIRE_TENANT_ID=%s", requireTenant)
	log.Printf("[CMS] - DEFAULT_TENANT_ID=%s", defaultTenant)
	log.Printf("[CMS] - Verifier mode=%s", mode)
	log.Println("[CMS] - Auth mode: IAM-issued JWT verification only (no local login)")
}

func logCMSConnectionTargets() {
	dbURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	whisperURL := strings.TrimSpace(os.Getenv("WHISPER_API_URL"))
	storageEndpoint := strings.TrimSpace(os.Getenv("STORAGE_ENDPOINT"))
	storagePublicURL := strings.TrimSpace(os.Getenv("STORAGE_PUBLIC_URL"))
	aggregationBaseURL := strings.TrimSpace(os.Getenv("AGGREGATION_BASE_URL"))
	enrichmentBaseURL := strings.TrimSpace(os.Getenv("ENRICHMENT_BASE_URL"))

	log.Println("[CMS] Connection targets")
	log.Printf("[CMS] - Server bind: %s", cmsServerAddress())
	log.Printf("[CMS] - Database: %s", cmsDatabaseTarget(dbURL))
	log.Printf("[CMS] - Whisper API: %s", emptyOr(whisperURL, "(not set)"))
	log.Printf("[CMS] - Aggregation API: %s", emptyOr(aggregationBaseURL, "(not set)"))
	log.Printf("[CMS] - Enrichment API: %s", emptyOr(enrichmentBaseURL, "(not set)"))
	log.Printf("[CMS] - Storage endpoint: %s", emptyOr(storageEndpoint, "(not set)"))
	log.Printf("[CMS] - Storage public URL: %s", emptyOr(storagePublicURL, "(not set)"))
	log.Println("[CMS] - CORS mode: AllowAllOrigins=true")
}

func cmsDatabaseTarget(dsn string) string {
	if dsn == "" {
		return "(not set)"
	}
	parsed, err := url.Parse(dsn)
	if err == nil && parsed.Host != "" {
		dbName := strings.TrimPrefix(parsed.Path, "/")
		if dbName == "" {
			dbName = "(default)"
		}
		return parsed.Host + "/" + dbName
	}
	return "(unparsed DSN)"
}

func emptyOr(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func cmsServerAddress() string {
	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		port = "8080"
	}

	host := strings.TrimSpace(os.Getenv("HOST"))
	if host == "" || host == "0.0.0.0" {
		return ":" + port
	}

	return host + ":" + port
}
