package main

import (
	"content-management-system/src/controllers"
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

	if err := utils.EnsureTenantScopeColumns(db); err != nil {
		log.Fatalf("Failed to apply tenant scope schema patch: %v", err)
	}
	log.Println("Tenant scope schema patch verified")

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
	if (env == "development" || env == "dev") && os.Getenv("AUTO_MIGRATE") != "false" {
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
			&models.Topic{},
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
			// Quality management — ingest configuration only.
			// quality_rules and quality_history were removed in Phase 7;
			// re-encoding is now driven by storage policies (archive_action='re_encode').
			// The two tables remain in the dev DB for now — drop manually after confirming.
			&models.QualityProfile{},
			// Audit log — admin actions executed from Platform-Console
			&models.AuditLog{},
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
		AllowHeaders:     []string{"Origin", "Content-Length", "Content-Type", "Authorization"},
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
	// Precompute missing topics.related_ids (stories predating the write-time
	// related feature) so feed reads never fall back to per-slide centroid kNN.
	controllers.StartRelatedBackfill(db)
	// News Circulation automation heartbeat — periodically recompute source
	// cadence recommendations (and auto-apply inside guardrails) for tenants that
	// opted in, so the news pipeline self-tunes without manual admin triggers.
	controllers.StartCirculationAutomation(db)

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
