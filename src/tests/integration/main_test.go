package integration

import (
	"content-management-system/src/models"
	"content-management-system/src/routes"
	"content-management-system/src/utils"
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
	setup()
	code := m.Run()
	cleanup()
	os.Exit(code)
}

func setup() {
	gin.SetMode(gin.TestMode)

	// Provide sensible defaults for local testing if env vars are not set
	setDefaultEnvIfEmpty("DB_USER", "postgres")
	setDefaultEnvIfEmpty("DB_PASSWORD", "927319")
	setDefaultEnvIfEmpty("DB_NAME", "cms_test")
	setDefaultEnvIfEmpty("DB_HOST", "localhost")
	setDefaultEnvIfEmpty("DB_PORT", "5433")

	var err error
	testDB, err = utils.ConnectDB()
	if err != nil {
		log.Fatalf("failed to connect test database: %v", err)
	}

	if err := testDB.AutoMigrate(&models.Page{}, &models.Media{}, &models.Post{}); err != nil {
		log.Fatalf("failed to migrate test database: %v", err)
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
}

func cleanup() {
	if testDB == nil {
		return
	}
	m := testDB.Migrator()
	_ = m.DropTable("post_media")
	_ = m.DropTable(&models.Post{})
	_ = m.DropTable(&models.Media{})
	_ = m.DropTable(&models.Page{})

	if sqlDB, err := testDB.DB(); err == nil {
		_ = sqlDB.Close()
	}
}

func setDefaultEnvIfEmpty(key, value string) {
	if os.Getenv(key) == "" {
		_ = os.Setenv(key, value)
	}
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
