package main

import (
	"content-management-system/src/models" // needs it for automigrate
	"content-management-system/src/routes"
	"content-management-system/src/utils"

	"log"
	"os"
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
			"version": "1.0.0",
			"endpoints": gin.H{
				"health":        "/health",
				"api":           "/api/v1",
				"posts":         "/api/v1/posts",
				"media":         "/api/v1/media",
				"pages":         "/api/v1/pages",
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

}

func main() {
	log.Println("Starting Content Management System...")
	log.Printf("Environment: %s", os.Getenv("ENV"))

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

	// Run migrations in development environments
	// Note: In production, after initial setup, use proper migration tools instead
	if env == "development" || env == "dev" || env == "production" {
		log.Println("Migrating database...")
		if err := utils.AutoMigrate(db, &models.Page{}, &models.Post{}, &models.Media{}); err != nil {
			log.Fatalf("Failed to migrate database: %v", err)
		}
		// Only seed in development
		if env == "development" || env == "dev" {
			if err := utils.SeedData(db); err != nil {
				log.Fatalf("Failed to seed data: %v", err)
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

	log.Println("Starting server on :8080...")
	if err := router.Run(":8080"); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

}
