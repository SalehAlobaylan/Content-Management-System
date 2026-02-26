package main

import (
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

	// Internal service-to-service routes
	routes.SetupInternalRoutes(router, db)

}

func main() {
	log.Println("Starting Content Management System...")
	log.Printf("Environment: %s", os.Getenv("ENV"))
	logCMSConnectionTargets()

	db, err := utils.ConnectDB()
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	log.Println("Successfully connected to database")

	sqlDB, err := db.DB()
	if err != nil {
		log.Fatalf("Failed to get database instance: %v", err)
	}

	defer sqlDB.Close()

	env := os.Getenv("ENV")
	if env == "" { //if env is not set, set it to development as default
		env = "development"
	}

	// Run migrations only in development environments
	// Note: In production, use manual migrations or migration tools to avoid conflicts
	if env == "development" || env == "dev" {
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
			&models.AdminUser{},
		); err != nil {
			log.Fatalf("Failed to migrate database: %v", err)
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
			// Seed default admin user (dev only)
			if err := utils.SeedAdminUser(db); err != nil {
				log.Fatalf("Failed to seed admin user: %v", err)
			}
		}
	}
	if env == "production" {
		gin.SetMode(gin.ReleaseMode)
	}

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

	log.Println("Starting server on :8080...")
	if err := router.Run(":8080"); err != nil {
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
	log.Println("[CMS] - /admin/login route is currently enabled (legacy compatibility path)")
}

func logCMSConnectionTargets() {
	dbURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	whisperURL := strings.TrimSpace(os.Getenv("WHISPER_API_URL"))
	storageEndpoint := strings.TrimSpace(os.Getenv("STORAGE_ENDPOINT"))
	storagePublicURL := strings.TrimSpace(os.Getenv("STORAGE_PUBLIC_URL"))

	log.Println("[CMS] Connection targets")
	log.Printf("[CMS] - Database: %s", cmsDatabaseTarget(dbURL))
	log.Printf("[CMS] - Whisper API: %s", emptyOr(whisperURL, "(not set)"))
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
