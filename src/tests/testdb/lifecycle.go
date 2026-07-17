// Package testdb provides the only destructive PostgreSQL test lifecycle used
// by CMS tests. It must be configured explicitly and never reads service .env.
package testdb

import (
	"crypto/rand"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"

	_ "github.com/lib/pq"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const disposableMarker = "I_UNDERSTAND_THIS_DATABASE_IS_DISPOSABLE"

// Open creates a fresh random database when CMS_TEST_ADMIN_URL is available.
// For restricted local environments it accepts an already-created, guarded
// CMS_TEST_DATABASE_URL, but never drops that fallback database.
func Open(t testing.TB) *gorm.DB {
	t.Helper()
	handle, err := openFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := handle.Close(); err != nil {
			t.Errorf("disposable database cleanup failed: %v", err)
		}
	})
	return handle.DB
}

// OpenForMain is the TestMain equivalent of Open. Callers must invoke the
// returned cleanup after all package tests have finished.
func OpenForMain() (*gorm.DB, func() error, error) {
	handle, err := openFromEnvironment()
	if err != nil {
		return nil, nil, err
	}
	return handle.DB, handle.Close, nil
}

type handle struct {
	DB       *gorm.DB
	dsn      string
	adminDSN string
	created  bool
}

func openFromEnvironment() (*handle, error) {
	marker := os.Getenv("CMS_TEST_DISPOSABLE")
	runtimeDSN := os.Getenv("DATABASE_URL")
	adminDSN := strings.TrimSpace(os.Getenv("CMS_TEST_ADMIN_URL"))
	if adminDSN != "" {
		if err := validateAdminDSN(adminDSN, marker); err != nil {
			return nil, err
		}
		dsn, err := createDatabase(adminDSN)
		if err != nil {
			return nil, err
		}
		handle, err := openHandle(dsn, adminDSN, true)
		if err != nil {
			_ = dropDatabase(adminDSN, dsn)
		}
		return handle, err
	}

	// The restricted-local path is safe only because the target guard runs
	// before gorm/sql opens a connection and cleanup never drops this database.
	dsn := os.Getenv("CMS_TEST_DATABASE_URL")
	if _, err := ValidateDisposableDSN(dsn, runtimeDSN, marker); err != nil {
		return nil, err
	}
	return openHandle(dsn, "", false)
}

func openHandle(dsn, adminDSN string, created bool) (*handle, error) {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		return nil, fmt.Errorf("connect disposable CMS test database: %w", err)
	}
	if err := db.Exec("CREATE EXTENSION IF NOT EXISTS pgcrypto").Error; err != nil {
		closeGormDB(db)
		return nil, fmt.Errorf("enable pgcrypto in disposable CMS test database: %w", err)
	}
	if err := db.Exec("CREATE EXTENSION IF NOT EXISTS vector").Error; err != nil {
		closeGormDB(db)
		return nil, fmt.Errorf("enable pgvector in disposable CMS test database: %w", err)
	}
	return &handle{DB: db, dsn: dsn, adminDSN: adminDSN, created: created}, nil
}

func (h *handle) Close() error {
	if h == nil || h.DB == nil {
		return nil
	}
	sqlDB, err := h.DB.DB()
	if err == nil {
		err = sqlDB.Close()
	}
	if err != nil {
		return err
	}
	if !h.created {
		return nil
	}
	return dropDatabase(h.adminDSN, h.dsn)
}

func closeGormDB(db *gorm.DB) {
	if sqlDB, err := db.DB(); err == nil {
		_ = sqlDB.Close()
	}
}

func dropDatabase(adminDSN, dsn string) error {
	name, err := databaseName(dsn)
	if err != nil {
		return err
	}
	admin, err := sql.Open("postgres", adminDSN)
	if err != nil {
		return fmt.Errorf("open disposable database admin connection: %w", err)
	}
	defer admin.Close()
	if _, err := admin.Exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()", name); err != nil {
		return fmt.Errorf("terminate disposable database connections: %w", err)
	}
	if _, err := admin.Exec(`DROP DATABASE "` + name + `"`); err != nil {
		return fmt.Errorf("drop disposable database: %w", err)
	}
	return nil
}

func validateAdminDSN(raw, marker string) error {
	if marker != disposableMarker {
		return fmt.Errorf("CMS_TEST_DISPOSABLE acknowledgement is required")
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Hostname() == "" {
		return fmt.Errorf("CMS_TEST_ADMIN_URL must be a PostgreSQL URL")
	}
	host := strings.ToLower(u.Hostname())
	if strings.Contains(host, "supabase") || strings.Contains(host, "neon") {
		return fmt.Errorf("managed production database hosts are forbidden for tests")
	}
	return nil
}

func createDatabase(adminDSN string) (string, error) {
	name, err := randomDatabaseName()
	if err != nil {
		return "", err
	}
	admin, err := sql.Open("postgres", adminDSN)
	if err != nil {
		return "", fmt.Errorf("open disposable database admin connection: %w", err)
	}
	defer admin.Close()
	if _, err := admin.Exec(`CREATE DATABASE "` + name + `"`); err != nil {
		return "", fmt.Errorf("create disposable CMS test database: %w", err)
	}
	u, err := url.Parse(adminDSN)
	if err != nil {
		return "", fmt.Errorf("parse disposable database admin URL: %w", err)
	}
	u.Path = "/" + name
	return u.String(), nil
}

func randomDatabaseName() (string, error) {
	bytes := make([]byte, 8)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("generate disposable database suffix: %w", err)
	}
	return fmt.Sprintf("wahb_cms_test_%x", bytes), nil
}

func databaseName(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parse disposable database URL: %w", err)
	}
	name := strings.TrimPrefix(u.Path, "/")
	if !disposableName.MatchString(name) {
		return "", fmt.Errorf("refusing to drop non-disposable database")
	}
	return name, nil
}
