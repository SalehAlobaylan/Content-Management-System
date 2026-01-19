-- PostgreSQL initialization script for Lumen Platform
-- This runs automatically when the Docker container is first created

-- ===========================================
-- Extensions
-- ===========================================
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS vector;

-- ===========================================
-- Note: Tables are auto-created by GORM AutoMigrate
-- This script only ensures extensions are enabled
-- ===========================================

-- If you want to pre-create tables (optional), include the full schema here
-- Otherwise, the Go application will create them on startup via AutoMigrate

-- Grant permissions (if needed for specific users)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
