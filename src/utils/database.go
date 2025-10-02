package utils

import (
	// "content-management-system/src/models"

	"fmt"
	"os"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func ConnectDB() (*gorm.DB, error) {
	dbUser := os.Getenv("DB_USER")
	dbPassword := os.Getenv("DB_PASSWORD")
	dbName := os.Getenv("DB_NAME")
	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	sslMode := os.Getenv("DB_SSLMODE")
	env := os.Getenv("ENV")

	// Default to require for production (DigitalOcean)
	if sslMode == "" {
		sslMode = "require"
	}

	// Only try to ensure database exists in development
	// In production (DigitalOcean), the database should already exist
	if env == "development" || env == "dev" || env == "" {
		if err := ensureDatabaseExists(dbHost, dbPort, dbUser, dbPassword, dbName, sslMode); err != nil {
			// Log but don't fail - database might already exist
			fmt.Printf("Warning: Could not ensure database exists: %v\n", err)
		}
	}

	dsn := fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%s sslmode=%s TimeZone=UTC",
		dbHost, dbUser, dbPassword, dbName, dbPort, sslMode,
	)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Ensure pgcrypto extension for gen_random_uuid()
	_ = db.Exec("CREATE EXTENSION IF NOT EXISTS pgcrypto").Error

	return db, nil
}

func ensureDatabaseExists(host, port, user, password, dbName, sslMode string) error {
	adminDSN := fmt.Sprintf(
		"host=%s user=%s password=%s dbname=postgres port=%s sslmode=%s TimeZone=UTC",
		host, user, password, port, sslMode,
	)
	adminDB, err := gorm.Open(postgres.Open(adminDSN), &gorm.Config{})
	if err != nil {
		return err
	}

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

func AutoMigrate(db *gorm.DB, models ...interface{}) error {
	return db.AutoMigrate(models...)
}

func SeedData(db *gorm.DB) error { // todo
	// db.Create(&models.Post{
	// 	Title: "Hello World",
	// 	Content: "This is a test post",
	// })
	return nil
}
