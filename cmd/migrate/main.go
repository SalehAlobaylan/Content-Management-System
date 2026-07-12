package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"content-management-system/src/utils"

	_ "github.com/joho/godotenv/autoload"
	"gorm.io/gorm"
)

const ledgerTableDDL = `
CREATE TABLE IF NOT EXISTS cms_schema_migrations (
	version varchar(255) PRIMARY KEY,
	applied_at timestamp NOT NULL DEFAULT now()
)`

type migrationFile struct {
	Version string
	Path    string
}

var timestampedMigrationName = regexp.MustCompile(`^\d{14}_.+\.sql$`)

func main() {
	var (
		applyAll = flag.Bool("all", false, "apply every migration not recorded in schema_migrations")
		status   = flag.Bool("status", false, "print migration ledger status without applying files")
		baseline = flag.String("baseline-through", "", "record timestamped migrations through this version as already applied without executing them")
		dir      = flag.String("dir", "migrations", "directory containing CMS SQL migrations")
	)
	flag.Parse()

	if *status && (*applyAll || *baseline != "" || flag.NArg() > 0) {
		log.Fatal("--status cannot be combined with --all, --baseline-through, or explicit migration files")
	}
	if *baseline != "" && (*applyAll || flag.NArg() > 0) {
		log.Fatal("--baseline-through cannot be combined with --all or explicit migration files")
	}
	if !*status && !*applyAll && *baseline == "" && flag.NArg() == 0 {
		log.Fatal("no migrations selected. Use --status, --all, --baseline-through, or pass explicit migration filenames")
	}

	db, err := utils.ConnectDB()
	if err != nil {
		log.Fatalf("connect database: %v", err)
	}

	if err := ensureLedger(db); err != nil {
		log.Fatalf("ensure migration ledger: %v", err)
	}

	files, err := listMigrationFiles(*dir)
	if err != nil {
		log.Fatalf("list migrations: %v", err)
	}

	applied, err := appliedVersions(db)
	if err != nil {
		log.Fatalf("read migration ledger: %v", err)
	}

	if *status {
		printStatus(files, applied)
		return
	}
	if *baseline != "" {
		if err := baselineThrough(db, files, applied, *baseline); err != nil {
			log.Fatalf("baseline migrations: %v", err)
		}
		return
	}

	selected, err := selectMigrations(files, applied, *applyAll, flag.Args())
	if err != nil {
		log.Fatal(err)
	}
	if len(selected) == 0 {
		log.Println("No migrations to apply.")
		return
	}

	for _, file := range selected {
		if err := applyMigration(db, file); err != nil {
			log.Fatalf("apply %s: %v", file.Version, err)
		}
		log.Printf("Applied %s", file.Version)
	}
}

func ensureLedger(db *gorm.DB) error {
	return db.Exec(ledgerTableDDL).Error
}

func listMigrationFiles(dir string) ([]migrationFile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	files := make([]migrationFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !timestampedMigrationName.MatchString(entry.Name()) {
			continue
		}
		files = append(files, migrationFile{
			Version: entry.Name(),
			Path:    filepath.Join(dir, entry.Name()),
		})
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].Version < files[j].Version
	})
	return files, nil
}

func baselineThrough(db *gorm.DB, files []migrationFile, applied map[string]time.Time, through string) error {
	through = strings.TrimSuffix(through, ".sql")
	matched := false
	selected := make([]migrationFile, 0)
	for _, file := range files {
		version := strings.TrimSuffix(file.Version, ".sql")
		if version > through {
			break
		}
		selected = append(selected, file)
		if version == through {
			matched = true
		}
	}
	if !matched {
		return fmt.Errorf("baseline version %q not found in migrations/", through)
	}

	return db.Transaction(func(tx *gorm.DB) error {
		for _, file := range selected {
			if _, ok := applied[file.Version]; ok {
				continue
			}
			if err := tx.Exec(
				"INSERT INTO cms_schema_migrations (version, applied_at) VALUES (?, now()) ON CONFLICT (version) DO NOTHING",
				file.Version,
			).Error; err != nil {
				return err
			}
			log.Printf("Baselined %s", file.Version)
		}
		return nil
	})
}

func appliedVersions(db *gorm.DB) (map[string]time.Time, error) {
	rows, err := db.Raw("SELECT version, applied_at FROM cms_schema_migrations").Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	applied := make(map[string]time.Time)
	for rows.Next() {
		var version string
		var appliedAt time.Time
		if err := rows.Scan(&version, &appliedAt); err != nil {
			return nil, err
		}
		applied[version] = appliedAt
	}
	return applied, rows.Err()
}

func printStatus(files []migrationFile, applied map[string]time.Time) {
	for _, file := range files {
		if appliedAt, ok := applied[file.Version]; ok {
			fmt.Printf("applied  %s  %s\n", file.Version, appliedAt.Format(time.RFC3339))
			continue
		}
		fmt.Printf("pending  %s\n", file.Version)
	}
}

func selectMigrations(files []migrationFile, applied map[string]time.Time, applyAll bool, requested []string) ([]migrationFile, error) {
	if applyAll {
		selected := make([]migrationFile, 0)
		for _, file := range files {
			if _, ok := applied[file.Version]; !ok {
				selected = append(selected, file)
			}
		}
		return selected, nil
	}

	byVersion := make(map[string]migrationFile, len(files))
	for _, file := range files {
		byVersion[file.Version] = file
		byVersion[strings.TrimSuffix(file.Version, ".sql")] = file
	}

	selected := make([]migrationFile, 0, len(requested))
	for _, name := range requested {
		file, ok := byVersion[name]
		if !ok {
			return nil, fmt.Errorf("migration %q not found in migrations/", name)
		}
		selected = append(selected, file)
	}
	sort.Slice(selected, func(i, j int) bool {
		return selected[i].Version < selected[j].Version
	})
	return selected, nil
}

func applyMigration(db *gorm.DB, file migrationFile) error {
	sqlBytes, err := os.ReadFile(file.Path)
	if err != nil {
		return err
	}
	sql := strings.TrimSpace(string(sqlBytes))
	if sql == "" {
		return errors.New("migration file is empty")
	}

	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(sql).Error; err != nil {
			return err
		}
		return tx.Exec(
			"INSERT INTO cms_schema_migrations (version, applied_at) VALUES (?, now()) ON CONFLICT (version) DO UPDATE SET applied_at = EXCLUDED.applied_at",
			file.Version,
		).Error
	})
}
