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

// PrepareStoryTopicSplit adapts development databases created before the
// story/topic vocabulary split. Old schemas used `topics` for News event
// clusters; new schemas reserve `topics` for canonical user preferences and
// store event clusters in `stories`.
func PrepareStoryTopicSplit(db *gorm.DB) error {
	var legacyTopics bool
	if err := db.Raw(`
		SELECT to_regclass('public.topics') IS NOT NULL
		   AND NOT EXISTS (
		       SELECT 1
		       FROM information_schema.columns
		       WHERE table_schema = 'public'
		         AND table_name = 'topics'
		         AND column_name = 'slug'
		   )
	`).Scan(&legacyTopics).Error; err != nil {
		return fmt.Errorf("story/topic split check failed: %w", err)
	}
	if !legacyTopics {
		return nil
	}

	var storiesExists bool
	if err := db.Raw(`SELECT to_regclass('public.stories') IS NOT NULL`).Scan(&storiesExists).Error; err != nil {
		return fmt.Errorf("story/topic split stories check failed: %w", err)
	}
	if storiesExists {
		var storyRows int64
		if err := db.Raw(`SELECT COUNT(*) FROM stories`).Scan(&storyRows).Error; err != nil {
			return fmt.Errorf("story/topic split stories count failed: %w", err)
		}
		if storyRows > 0 {
			return mergeAndArchiveLegacyTopics(db)
		}
		if err := db.Exec(`DROP TABLE stories`).Error; err != nil {
			return fmt.Errorf("story/topic split failed to remove empty partial stories table: %w", err)
		}
	}

	statements := []string{
		`ALTER TABLE topics RENAME TO stories`,
		`DO $$
		BEGIN
		    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'topics_pkey')
		       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'stories_pkey') THEN
		        ALTER TABLE stories RENAME CONSTRAINT topics_pkey TO stories_pkey;
		    END IF;
		END $$`,
		`DO $$
		BEGIN
		    IF to_regclass('public.topics_id_seq') IS NOT NULL
		       AND to_regclass('public.stories_id_seq') IS NULL THEN
		        ALTER SEQUENCE topics_id_seq RENAME TO stories_id_seq;
		        ALTER TABLE stories ALTER COLUMN id SET DEFAULT nextval('stories_id_seq'::regclass);
		    END IF;
		END $$`,
		`DO $$
		BEGIN
		    IF EXISTS (
		        SELECT 1 FROM information_schema.columns
		        WHERE table_schema = 'public'
		          AND table_name = 'content_items'
		          AND column_name = 'topic_id'
		    ) AND NOT EXISTS (
		        SELECT 1 FROM information_schema.columns
		        WHERE table_schema = 'public'
		          AND table_name = 'content_items'
		          AND column_name = 'story_id'
		    ) THEN
		        ALTER TABLE content_items RENAME COLUMN topic_id TO story_id;
		    END IF;
		END $$`,
		`DO $$
		BEGIN
		    IF EXISTS (
		        SELECT 1 FROM information_schema.columns
		        WHERE table_schema = 'public'
		          AND table_name = 'rss_feeds'
		          AND column_name = 'topic_id'
		    ) AND NOT EXISTS (
		        SELECT 1 FROM information_schema.columns
		        WHERE table_schema = 'public'
		          AND table_name = 'rss_feeds'
		          AND column_name = 'story_id'
		    ) THEN
		        ALTER TABLE rss_feeds RENAME COLUMN topic_id TO story_id;
		    END IF;
		END $$`,
		renameIndexIfAvailableSQL("idx_topics_public_id", "idx_stories_public_id"),
		renameIndexIfAvailableSQL("idx_topics_tenant_label", "idx_stories_tenant_label"),
		renameIndexIfAvailableSQL("idx_topics_tenant", "idx_stories_tenant"),
		renameIndexIfAvailableSQL("idx_topics_last_member_at", "idx_stories_last_member_at"),
		renameIndexIfAvailableSQL("idx_topics_summary_built_at", "idx_stories_summary_built_at"),
		renameIndexIfAvailableSQL("topics_embedding_idx", "stories_embedding_idx"),
		renameIndexIfAvailableSQL("idx_content_items_topic_id", "idx_content_items_story_id"),
		renameIndexIfAvailableSQL("idx_rss_feeds_topic_id", "idx_rss_feeds_story_id"),
		`DO $$
		BEGIN
		    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'content_items_topic_id_fkey')
		       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'content_items_story_id_fkey') THEN
		        ALTER TABLE content_items
		            RENAME CONSTRAINT content_items_topic_id_fkey TO content_items_story_id_fkey;
		    END IF;

		    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'rss_feeds_topic_id_fkey')
		       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'rss_feeds_story_id_fkey') THEN
		        ALTER TABLE rss_feeds
		            RENAME CONSTRAINT rss_feeds_topic_id_fkey TO rss_feeds_story_id_fkey;
		    END IF;
		END $$`,
	}
	for _, stmt := range statements {
		if err := db.Exec(stmt).Error; err != nil {
			return fmt.Errorf("story/topic split failed (%s): %w", firstLine(stmt), err)
		}
	}
	return nil
}

func mergeAndArchiveLegacyTopics(db *gorm.DB) error {
	var archiveExists bool
	if err := db.Raw(`SELECT to_regclass('public.legacy_event_topics') IS NOT NULL`).Scan(&archiveExists).Error; err != nil {
		return fmt.Errorf("legacy event topics archive check failed: %w", err)
	}
	if archiveExists {
		return fmt.Errorf("legacy topics table, populated stories table, and legacy_event_topics archive all exist; manual schema reconciliation required")
	}

	statements := []string{
		`INSERT INTO stories (
		    public_id, tenant_id, label, embedding, article_count, last_member_at,
		    related_ids, labeled, summary, bullets, summary_built_at, category,
		    created_at, updated_at
		)
		SELECT
		    t.public_id, t.tenant_id, t.label, t.embedding, t.article_count, t.last_member_at,
		    t.related_ids, t.labeled, t.summary, t.bullets, t.summary_built_at, t.category,
		    t.created_at, t.updated_at
		FROM topics t
		WHERE NOT EXISTS (
		    SELECT 1 FROM stories s WHERE s.public_id = t.public_id
		)`,
		`ALTER TABLE topics RENAME TO legacy_event_topics`,
		`DO $$
		BEGIN
		    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'topics_pkey')
		       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'legacy_event_topics_pkey') THEN
		        ALTER TABLE legacy_event_topics RENAME CONSTRAINT topics_pkey TO legacy_event_topics_pkey;
		    END IF;
		END $$`,
		`DO $$
		BEGIN
		    IF to_regclass('public.topics_id_seq') IS NOT NULL
		       AND to_regclass('public.legacy_event_topics_id_seq') IS NULL THEN
		        ALTER SEQUENCE topics_id_seq RENAME TO legacy_event_topics_id_seq;
		        ALTER TABLE legacy_event_topics ALTER COLUMN id SET DEFAULT nextval('legacy_event_topics_id_seq'::regclass);
		    END IF;
		END $$`,
		renameIndexIfAvailableSQL("idx_topics_public_id", "idx_legacy_event_topics_public_id"),
		renameIndexIfAvailableSQL("idx_topics_tenant_label", "idx_legacy_event_topics_tenant_label"),
		renameIndexIfAvailableSQL("idx_topics_tenant", "idx_legacy_event_topics_tenant"),
		renameIndexIfAvailableSQL("idx_topics_last_member_at", "idx_legacy_event_topics_last_member_at"),
		renameIndexIfAvailableSQL("idx_topics_summary_built_at", "idx_legacy_event_topics_summary_built_at"),
		renameIndexIfAvailableSQL("topics_embedding_idx", "legacy_event_topics_embedding_idx"),
	}
	for _, stmt := range statements {
		if err := db.Exec(stmt).Error; err != nil {
			return fmt.Errorf("legacy topics reconciliation failed (%s): %w", firstLine(stmt), err)
		}
	}
	return nil
}

func renameIndexIfAvailableSQL(oldName, newName string) string {
	return fmt.Sprintf(`DO $$
	BEGIN
	    IF to_regclass('public.%[1]s') IS NOT NULL
	       AND to_regclass('public.%[2]s') IS NULL THEN
	        ALTER INDEX %[1]s RENAME TO %[2]s;
	    END IF;
	END $$`, oldName, newName)
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return strings.TrimSpace(s[:i])
	}
	return strings.TrimSpace(s)
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
