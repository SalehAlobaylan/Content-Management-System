package utils

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// ConnectDB connects to PostgreSQL using DATABASE_URL environment variable
func ConnectDB() (*gorm.DB, error) {
	env := os.Getenv("ENV")

	// Get connection string from DATABASE_URL (required)
	dsn := getDatabaseURL()

	// In development, try to ensure the database exists
	if env == "development" || env == "dev" || env == "" {
		if err := ensureDatabaseExistsFromURL(dsn); err != nil {
			// Log but don't fail - database might already exist
			fmt.Printf("Warning: Could not ensure database exists: %v\n", err)
		}
	}

	db, err := gorm.Open(postgres.New(postgres.Config{
		DSN:                  dsn,
		PreferSimpleProtocol: true,
	}), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Ensure required PostgreSQL extensions
	_ = db.Exec("CREATE EXTENSION IF NOT EXISTS pgcrypto").Error
	_ = db.Exec("CREATE EXTENSION IF NOT EXISTS vector").Error

	return db, nil
}

// EnsureTenantScopeColumns applies an idempotent tenant-scope patch for legacy schemas.
// It keeps production instances aligned with tenant-aware query paths.
func EnsureTenantScopeColumns(db *gorm.DB) error {
	if err := db.Exec("ALTER TABLE IF EXISTS admin_users ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64)").Error; err != nil {
		return fmt.Errorf("tenant scope patch failed (admin_users add tenant_id): %w", err)
	}
	if err := db.Exec("ALTER TABLE IF EXISTS content_sources ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64)").Error; err != nil {
		return fmt.Errorf("tenant scope patch failed (content_sources add tenant_id): %w", err)
	}
	if err := db.Exec("ALTER TABLE IF EXISTS content_items ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64)").Error; err != nil {
		return fmt.Errorf("tenant scope patch failed (content_items add tenant_id): %w", err)
	}
	if err := db.Exec("ALTER TABLE IF EXISTS content_items ALTER COLUMN idempotency_key TYPE VARCHAR(512)").Error; err != nil {
		return fmt.Errorf("tenant scope patch failed (content_items idempotency_key): %w", err)
	}

	if db.Migrator().HasTable("admin_users") {
		statements := []string{
			"UPDATE admin_users SET tenant_id = 'default' WHERE tenant_id IS NULL OR tenant_id = ''",
			"ALTER TABLE admin_users ALTER COLUMN tenant_id SET DEFAULT 'default'",
			"ALTER TABLE admin_users ALTER COLUMN tenant_id SET NOT NULL",
			"CREATE INDEX IF NOT EXISTS idx_admin_users_tenant_id ON admin_users(tenant_id)",
		}
		for _, stmt := range statements {
			if err := db.Exec(stmt).Error; err != nil {
				return fmt.Errorf("tenant scope patch failed (%s): %w", stmt, err)
			}
		}
	}

	if db.Migrator().HasTable("content_sources") {
		statements := []string{
			"UPDATE content_sources SET tenant_id = 'default' WHERE tenant_id IS NULL OR tenant_id = ''",
			"ALTER TABLE content_sources ALTER COLUMN tenant_id SET DEFAULT 'default'",
			"ALTER TABLE content_sources ALTER COLUMN tenant_id SET NOT NULL",
			"CREATE INDEX IF NOT EXISTS idx_content_sources_tenant_id ON content_sources(tenant_id)",
		}
		for _, stmt := range statements {
			if err := db.Exec(stmt).Error; err != nil {
				return fmt.Errorf("tenant scope patch failed (%s): %w", stmt, err)
			}
		}
	}

	if db.Migrator().HasTable("content_items") {
		statements := []string{
			"UPDATE content_items SET tenant_id = 'default' WHERE tenant_id IS NULL OR tenant_id = ''",
			"ALTER TABLE content_items ALTER COLUMN tenant_id SET DEFAULT 'default'",
			"ALTER TABLE content_items ALTER COLUMN tenant_id SET NOT NULL",
			"CREATE INDEX IF NOT EXISTS idx_content_items_tenant_id ON content_items(tenant_id)",
		}
		for _, stmt := range statements {
			if err := db.Exec(stmt).Error; err != nil {
				return fmt.Errorf("tenant scope patch failed (%s): %w", stmt, err)
			}
		}
	}

	return nil
}

// getDatabaseURL returns the database connection string from DATABASE_URL
// This is the only supported method for database configuration
func getDatabaseURL() string {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		panic("DATABASE_URL environment variable is required but not set")
	}
	return databaseURL
}

// ensureDatabaseExistsFromURL tries to create the database if it doesn't exist
func ensureDatabaseExistsFromURL(dsn string) error {
	// Extract database name and create admin connection string
	dbName, adminDSN := parseAndModifyDSN(dsn)
	if dbName == "" || adminDSN == "" {
		return fmt.Errorf("could not parse database connection string")
	}

	adminDB, err := gorm.Open(postgres.New(postgres.Config{
		DSN:                  adminDSN,
		PreferSimpleProtocol: true,
	}), &gorm.Config{})
	if err != nil {
		return err
	}

	sqlDB, _ := adminDB.DB()
	defer sqlDB.Close()

	var exists bool
	if err := adminDB.Raw("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = ?)", dbName).Scan(&exists).Error; err != nil {
		return err
	}
	if !exists {
		if err := adminDB.Exec("CREATE DATABASE " + dbName).Error; err != nil {
			return err
		}
	}
	return nil
}

// parseAndModifyDSN extracts the database name and returns a connection string for 'postgres' database
func parseAndModifyDSN(dsn string) (dbName string, adminDSN string) {
	// Handle URL format: postgres://user:pass@host:port/dbname?sslmode=disable
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return "", ""
		}
		dbName = strings.TrimPrefix(u.Path, "/")
		u.Path = "/postgres"
		return dbName, u.String()
	}

	// Handle key=value format: host=... user=... dbname=...
	parts := strings.Fields(dsn)
	var newParts []string
	for _, part := range parts {
		if strings.HasPrefix(part, "dbname=") {
			dbName = strings.TrimPrefix(part, "dbname=")
			newParts = append(newParts, "dbname=postgres")
		} else {
			newParts = append(newParts, part)
		}
	}
	return dbName, strings.Join(newParts, " ")
}

func AutoMigrate(db *gorm.DB, models ...interface{}) error {
	return db.AutoMigrate(models...)
}

func SeedData(db *gorm.DB) error {
	// Placeholder for original CMS seed data
	return nil
}
