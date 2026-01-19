package integration

import (
	"content-management-system/src/models"
	"content-management-system/src/routes"
	"content-management-system/src/utils"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/gin-gonic/gin"
	_ "github.com/joho/godotenv/autoload"
	"gorm.io/gorm"
)

var (
	testDB *gorm.DB
	router *gin.Engine
)

func TestMain(m *testing.M) {
	fmt.Println("🚀 Starting integration tests...")
	setup()
	code := m.Run()
	cleanup()
	fmt.Printf("✅ Integration tests completed with exit code: %d\n", code)
	os.Exit(code)
}

func setup() {
	fmt.Println("🔧 Setting up test environment...")
	gin.SetMode(gin.TestMode)

	// Set DATABASE_URL for tests (preferred method)
	// Falls back to building from individual vars if not set
	if os.Getenv("DATABASE_URL") == "" {
		dbUser := getEnvOrDefault("DB_USER", "postgres")
		dbPassword := getEnvOrDefault("DB_PASSWORD", "927319")
		dbName := getEnvOrDefault("DB_NAME", "cms_test")
		dbHost := getEnvOrDefault("DB_HOST", "localhost")
		dbPort := getEnvOrDefault("DB_PORT", "5433")

		databaseURL := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
			dbUser, dbPassword, dbHost, dbPort, dbName)
		os.Setenv("DATABASE_URL", databaseURL)
	}

	fmt.Printf("📊 Connecting to test database: %s\n", os.Getenv("DATABASE_URL"))

	var err error
	testDB, err = utils.ConnectDB()
	if err != nil {
		log.Fatalf("failed to connect test database: %v", err)
	}

	fmt.Println("🔄 Running database migrations...")
	if err := testDB.AutoMigrate(
		&models.Page{},
		&models.Media{},
		&models.Post{},
		// Lumen Platform models
		&models.ContentItem{},
		&models.Transcript{},
		&models.UserInteraction{},
		&models.ContentSource{},
	); err != nil {
		log.Fatalf("failed to migrate test database: %v", err)
	}

	fmt.Println("🌐 Setting up test router and routes...")
	router = gin.Default()
	router.Use(func(c *gin.Context) {
		c.Set("db", testDB)
		c.Next()
	})

	v1 := router.Group("/api/v1")
	routes.SetupPostRoutes(v1, testDB)
	routes.SetupMediaRoutes(v1, testDB)
	routes.SetupPageRoutes(v1, testDB)
	// Lumen Platform routes
	routes.SetupFeedRoutes(v1, testDB)
	routes.SetupInteractionRoutes(v1, testDB)
	routes.SetupContentRoutes(v1, testDB)
	fmt.Println("✅ Test environment setup complete!")
}

func cleanup() {
	fmt.Println("🧹 Cleaning up test environment...")
	if testDB == nil {
		return
	}
	m := testDB.Migrator()
	// Lumen Platform tables
	_ = m.DropTable(&models.UserInteraction{})
	_ = m.DropTable(&models.Transcript{})
	_ = m.DropTable(&models.ContentItem{})
	_ = m.DropTable(&models.ContentSource{})
	// Original CMS tables
	_ = m.DropTable("post_media")
	_ = m.DropTable(&models.Post{})
	_ = m.DropTable(&models.Media{})
	_ = m.DropTable(&models.Page{})

	if sqlDB, err := testDB.DB(); err == nil {
		_ = sqlDB.Close()
		fmt.Println("📊 Database connection closed")
	}
	fmt.Println("✅ Cleanup complete!")
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
	fmt.Println("🗑️  Clearing test tables...")
	if testDB == nil {
		return
	}
	_ = testDB.Exec("DELETE FROM post_media").Error
	_ = testDB.Exec("DELETE FROM posts").Error
	_ = testDB.Exec("DELETE FROM media").Error
	_ = testDB.Exec("DELETE FROM pages").Error
	fmt.Println("✅ Tables cleared")
}

/*
TESTING HINTS:
1. Database Connection:
   - Use a separate test database
   - Consider environment variables for credentials
   - Handle connection errors properly

2. Table Management:
   - Drop tables in correct order (foreign key constraints)
   - Clear data between tests
   - Consider using transactions for tests

3. Error Handling:
   - Log setup/cleanup errors
   - Ensure proper resource cleanup
   - Handle database operation errors

4. Best Practices:
   - Use constants for connection strings
   - Consider test helper functions
   - Add proper logging for debugging
   - Document any required setup steps
*/
