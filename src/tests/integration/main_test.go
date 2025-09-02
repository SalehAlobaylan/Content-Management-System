package integration

import (
	"content-management-system/src/models"
	"content-management-system/src/routes"
	"content-management-system/src/utils"
	"log"
	"os"
	"testing"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

var (
	testDB *gorm.DB
	router *gin.Engine
)

// TODO: Import required packages for:
// - Database (gorm, postgres driver)
// - Gin framework
// - Testing
// - Logging
// - OS operations
// - Your application packages (models, routes)

// TODO: Define package-level variables for:
// - Test database connection
// - Gin router instance

/*
INTEGRATION TEST SETUP GUIDE

This file sets up the integration test environment for your CMS backend.
It handles database connections, schema migrations, and cleanup.

Key Components:
1. Test database connection
2. Router setup
3. Schema migrations
4. Test cleanup
*/

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	cleanup()
	os.Exit(code)
}

func setup() {
	gin.SetMode(gin.TestMode)

	setDefaultEnvIfEmpty("DB_USER", "postgres")
	setDefaultEnvIfEmpty("DB_PASSWORD", "postgres")
	setDefaultEnvIfEmpty("DB_NAME", "cms_test")
	setDefaultEnvIfEmpty("DB_HOST", "localhost")
	setDefaultEnvIfEmpty("DB_PORT", "5432")

	var err error
	testDB, err = utils.ConnectDB()
	if err != nil {
		log.Fatalf("failed to connect test database: %v", err)
	}

	if err := utils.AutoMigrate(testDB, &models.Page{}, &models.Media{}, &models.Post{}); err != nil {
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

func clearTables() {
	if testDB == nil {
		return
	}
	_ = testDB.Exec("DELETE FROM post_media").Error
	_ = testDB.Exec("DELETE FROM posts").Error
	_ = testDB.Exec("DELETE FROM media").Error
	_ = testDB.Exec("DELETE FROM pages").Error
}

func setDefaultEnvIfEmpty(key, value string) {
	if os.Getenv(key) == "" {
		_ = os.Setenv(key, value)
	}
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
