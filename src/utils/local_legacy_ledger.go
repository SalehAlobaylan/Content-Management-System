package utils

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
)

// VerifyReviewedLegacyCMSHistory is an explicit adoption-only compatibility
// check. It never changes the ledger or claims unavailable historical hashes
// were verified. The normal runtime/relocation database contract stays strict.
// Call inside the adoption transaction, after target/fence authority checks.
func VerifyReviewedLegacyCMSHistory(tx *gorm.DB, dir, reviewed string) ([]string, error) {
	const introduction = "20260716000000_migration_ledger_checksums.sql"
	expected := strings.Split(reviewed, ",")
	for i := range expected {
		expected[i] = strings.TrimSpace(expected[i])
		if !cmsMigrationFilename.MatchString(expected[i]) {
			return nil, fmt.Errorf("invalid reviewed legacy checksum history")
		}
	}
	sort.Strings(expected)
	for i := 1; i < len(expected); i++ {
		if expected[i] == expected[i-1] {
			return nil, fmt.Errorf("duplicate reviewed legacy history entry")
		}
	}
	files, err := canonicalCMSMigrationChecksums(dir)
	if err != nil {
		return nil, err
	}
	if err := tx.Exec("LOCK TABLE cms_schema_migrations IN SHARE MODE").Error; err != nil {
		return nil, err
	}
	var rows []struct {
		Version       string
		Checksum      string
		AppliedAt     time.Time
		ExecutionMode string
	}
	if err := tx.Raw("SELECT version,COALESCE(checksum_sha256,'') AS checksum,applied_at,execution_mode FROM cms_schema_migrations ORDER BY version").Scan(&rows).Error; err != nil {
		return nil, err
	}
	if len(rows) != len(files) {
		return nil, fmt.Errorf("canonical CMS ledger is incomplete")
	}
	var introducedAt time.Time
	for _, row := range rows {
		if row.Version == introduction && row.Checksum != "" && row.Checksum == files[introduction] {
			introducedAt = row.AppliedAt
		}
	}
	if introducedAt.IsZero() {
		return nil, fmt.Errorf("checksum introduction is not verified")
	}
	historical := []string{}
	for _, row := range rows {
		hash, exists := files[row.Version]
		if !exists {
			return nil, fmt.Errorf("unknown CMS ledger entry: %s", row.Version)
		}
		if row.Checksum != "" {
			if row.Checksum != hash {
				return nil, fmt.Errorf("recorded CMS checksum drift: %s", row.Version)
			}
			continue
		}
		if row.Version >= introduction || row.ExecutionMode != "legacy" || row.AppliedAt.IsZero() || !row.AppliedAt.Before(introducedAt) {
			return nil, fmt.Errorf("missing CMS checksum outside reviewed historical window: %s", row.Version)
		}
		historical = append(historical, row.Version)
	}
	sort.Strings(historical)
	if strings.Join(historical, ",") != strings.Join(expected, ",") {
		return nil, fmt.Errorf("legacy checksum history changed after review")
	}
	return historical, nil
}
