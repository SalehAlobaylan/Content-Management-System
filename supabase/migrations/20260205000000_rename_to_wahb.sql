-- Database Rename Migration for Wahb Rebranding
-- This migration renames existing databases and schemas from Lumen/Turfa to Wahb
-- Run this AFTER backing up your data!

-- NOTE: This script assumes you're connected to the database as a superuser
-- For DigitalOcean App Platform or similar, you may need to run these commands
-- via their dashboard or CLI tool

-- ====================================================================
-- Step 1: Rename existing databases (if they exist)
-- ====================================================================
-- These commands must be run OUTSIDE of any database connection
-- Uncomment and run via psql or admin interface:

-- ALTER DATABASE lumen_db RENAME TO wahb_db;
-- ALTER DATABASE turfa_platform RENAME TO wahb_platform;

-- ====================================================================
-- Step 2: Update schema references (within the database)
-- ====================================================================
-- Connect to wahb_db/wahb_platform and run:

-- Rename schema if it exists
-- ALTER SCHEMA IF EXISTS lumen RENAME TO wahb;

-- ====================================================================
-- Step 3: Update database URLs in configuration files
-- ====================================================================
-- After running this migration, update your .env files:
--
-- Content-Management-System/.env:
--   DATABASE_URL=postgres://.../wahb_db?sslmode=...
--
-- CRM-Service/.env:
--   DATABASE_URL=postgres://.../wahb_platform?sslmode=...
--
-- docker-compose.yaml files:
--   POSTGRES_DB: wahb_db (or wahb_platform)
--   DATABASE_URL defaults updated

-- ====================================================================
-- Step 4: Verify the changes
-- ====================================================================
-- Check current database: SELECT current_database();
-- List all databases: \l (in psql) or SELECT datname FROM pg_database;
-- Check schemas: SELECT schema_name FROM information_schema.schemata;

-- ====================================================================
-- Rollback Instructions (if needed)
-- ====================================================================
-- To revert these changes:
-- ALTER DATABASE wahb_db RENAME TO lumen_db;
-- ALTER DATABASE wahb_platform RENAME TO turfa_platform;
-- ALTER SCHEMA IF EXISTS wahb RENAME TO lumen;
