package utils

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"gorm.io/gorm"
)

// DatabaseContract is non-secret runtime evidence. It is deliberately small
// enough for readiness and an independent migration coordinator to compare.
type DatabaseContract struct {
	DatabaseID      string `json:"database_id"`
	LineageRootID   string `json:"lineage_root_id"`
	Epoch           int64  `json:"epoch"`
	SchemaDigest    string `json:"schema_digest"`
	IdentityState   string `json:"identity_state"`
	LedgerState     string `json:"ledger_state"`
	EnforcementMode string `json:"enforcement_mode"`
}

type schemaMigrationRecord struct {
	Version  string
	Checksum string
}

type WriterFence struct {
	State      string `json:"state"`
	Epoch      int64  `json:"epoch"`
	FenceToken string `json:"fence_token"`
}

var cmsMigrationFilename = regexp.MustCompile(`^\d{14}_.+\.sql$`)

// ReadDatabaseContract never writes. Missing identity is reported as unknown
// so compatibility rollout can observe it without treating it as a valid ID.
func ReadDatabaseContract(db *gorm.DB, migrationsDir string) (DatabaseContract, error) {
	contract := DatabaseContract{IdentityState: "unknown", LedgerState: "unknown", EnforcementMode: schemaContractMode()}
	if strings.TrimSpace(migrationsDir) == "" {
		migrationsDir = "migrations"
	}

	var ledgerExists bool
	if err := db.Raw(`SELECT to_regclass('public.cms_schema_migrations') IS NOT NULL`).Scan(&ledgerExists).Error; err != nil {
		return contract, fmt.Errorf("read migration ledger state: %w", err)
	}
	if !ledgerExists {
		contract.LedgerState = "absent"
		return contract, nil
	}

	files, err := canonicalCMSMigrationChecksums(migrationsDir)
	if err != nil {
		return contract, err
	}
	var applied []schemaMigrationRecord
	if err := db.Raw(`SELECT version, COALESCE(checksum_sha256, '') AS checksum FROM cms_schema_migrations ORDER BY version`).Scan(&applied).Error; err != nil {
		return contract, fmt.Errorf("read CMS migration ledger: %w", err)
	}
	contract.SchemaDigest, contract.LedgerState = compareCMSMigrationLedger(files, applied)

	var identityTable bool
	if err := db.Raw(`SELECT to_regclass('public.wahb_database_identity') IS NOT NULL`).Scan(&identityTable).Error; err != nil {
		return contract, fmt.Errorf("read database identity state: %w", err)
	}
	if !identityTable {
		return contract, nil
	}
	if err := db.Raw(`SELECT database_id::text, lineage_root_id::text, database_epoch FROM wahb_database_identity WHERE singleton = TRUE`).Row().Scan(&contract.DatabaseID, &contract.LineageRootID, &contract.Epoch); err != nil {
		if err == sql.ErrNoRows || err == gorm.ErrRecordNotFound {
			return contract, nil
		}
		return contract, fmt.Errorf("read database identity: %w", err)
	}
	contract.IdentityState = "present"
	return contract, nil
}

func schemaContractMode() string {
	if strings.EqualFold(strings.TrimSpace(os.Getenv("CMS_DATABASE_CONTRACT_MODE")), "enforce") {
		return "enforce"
	}
	return "observe"
}

// ReadWriterFence is safe during compatibility rollout: an unapplied
// expansion migration is unknown-to-open for serving, but never a proof that a
// database is cutover-ready. The coordinator separately rejects absent fence
// evidence for any mutation it controls.
func ReadWriterFence(db *gorm.DB) (WriterFence, error) {
	var exists bool
	if err := db.Raw(`SELECT to_regclass('public.wahb_database_writer_fence') IS NOT NULL`).Scan(&exists).Error; err != nil {
		return WriterFence{}, fmt.Errorf("read writer-fence state: %w", err)
	}
	if !exists {
		return WriterFence{State: "compatibility_open"}, nil
	}
	var fence WriterFence
	if err := db.Raw(`SELECT state, epoch, fence_token::text FROM wahb_database_writer_fence WHERE singleton = TRUE`).Row().Scan(&fence.State, &fence.Epoch, &fence.FenceToken); err != nil {
		return WriterFence{}, fmt.Errorf("read writer fence: %w", err)
	}
	return fence, nil
}

// InstallWriterFenceCallbacks extends the HTTP fence to every ordinary GORM
// create/update/delete path, including CMS background workers. Raw SQL writes
// remain migration-owner-only and must carry their own explicit fence check.
func InstallWriterFenceCallbacks(db *gorm.DB) {
	check := func(tx *gorm.DB) {
		fence, err := ReadWriterFence(tx.Session(&gorm.Session{NewDB: true, SkipHooks: true}))
		if err != nil {
			tx.AddError(fmt.Errorf("database writer authority is unavailable: %w", err))
			return
		}
		if fence.State != "open" && fence.State != "successor_open" && fence.State != "compatibility_open" {
			tx.AddError(fmt.Errorf("database writes are fenced at epoch %d (%s)", fence.Epoch, fence.State))
		}
	}
	db.Callback().Create().Before("gorm:create").Register("wahb:database_writer_fence", check)
	db.Callback().Update().Before("gorm:update").Register("wahb:database_writer_fence", check)
	db.Callback().Delete().Before("gorm:delete").Register("wahb:database_writer_fence", check)
}

func canonicalCMSMigrationChecksums(dir string) (map[string]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read canonical CMS migrations from %q: %w", dir, err)
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && cmsMigrationFilename.MatchString(entry.Name()) {
			files = append(files, entry.Name())
		}
	}
	sort.Strings(files)
	checksums := make(map[string]string, len(files))
	for _, name := range files {
		contents, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, fmt.Errorf("read canonical CMS migration %q: %w", name, err)
		}
		sum := sha256.Sum256(contents)
		checksums[name] = hex.EncodeToString(sum[:])
	}
	return checksums, nil
}

func compareCMSMigrationLedger(files map[string]string, applied []schemaMigrationRecord) (string, string) {
	ledger := make(map[string]string, len(applied))
	for _, record := range applied {
		ledger[record.Version] = record.Checksum
	}
	if len(ledger) != len(files) {
		return "", "incomplete"
	}
	versions := make([]string, 0, len(files))
	for version, expected := range files {
		actual, ok := ledger[version]
		if !ok || actual == "" || actual != expected {
			return "", "drifted"
		}
		versions = append(versions, version+":"+expected)
	}
	sort.Strings(versions)
	sum := sha256.Sum256([]byte(strings.Join(versions, "\n")))
	return hex.EncodeToString(sum[:]), "verified"
}
