package utils

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"gorm.io/gorm"
)

// HealthDatabaseState is observation only. Write authority still uses the fresh
// ReadWriterFence path, including inside owner transactions and GORM callbacks.
type HealthDatabaseState struct {
	Contract      DatabaseContract
	ContractError error
	Fence         WriterFence
	FenceError    error
}

// ReadHealthDatabaseState keeps its catalog and data reads on one connection,
// without a transaction or extra SQL round trips. Connection releases it on exit.
func ReadHealthDatabaseState(ctx context.Context, db *gorm.DB, migrationsDir string) HealthDatabaseState {
	var state HealthDatabaseState
	err := db.WithContext(ctx).Connection(func(conn *gorm.DB) error {
		// Keep the pinned connection, but isolate statement errors so a failed
		// combined read cannot contaminate the independent fallback reads.
		state = readHealthDatabaseState(ctx, conn.Session(&gorm.Session{}), migrationsDir)
		return nil
	})
	if err != nil {
		state.Contract = DatabaseContract{IdentityState: "unknown", LedgerState: "unknown", EnforcementMode: schemaContractMode()}
		state.ContractError = fmt.Errorf("acquire health database connection: %w", err)
		state.FenceError = state.ContractError
	}
	return state
}

// Constant fragments preserve compatibility without parsing missing tables.
func readHealthDatabaseState(ctx context.Context, db *gorm.DB, migrationsDir string) HealthDatabaseState {
	state := HealthDatabaseState{Contract: DatabaseContract{IdentityState: "unknown", LedgerState: "unknown", EnforcementMode: schemaContractMode()}}
	var ledger, identity, fence bool
	err := db.Raw(`SELECT to_regclass('public.cms_schema_migrations') IS NOT NULL,
		to_regclass('public.wahb_database_identity') IS NOT NULL,
		to_regclass('public.wahb_database_writer_fence') IS NOT NULL`).Row().Scan(&ledger, &identity, &fence)
	if err != nil {
		state.ContractError = fmt.Errorf("read migration ledger state: %w", err)
		state.FenceError = fmt.Errorf("read writer-fence state: %w", err)
		return state
	}
	if !fence {
		state.Fence.State = "compatibility_open"
	}
	var expected map[string]string
	if !ledger {
		state.Contract.LedgerState = "absent"
	} else {
		if strings.TrimSpace(migrationsDir) == "" {
			migrationsDir = "migrations"
		}
		expected, state.ContractError = canonicalCMSMigrationChecksums(migrationsDir)
	}
	readContract := ledger && state.ContractError == nil
	columns := []string{"NULL::text", "NULL::text", "NULL::text"}
	if readContract {
		columns[0] = `(SELECT COALESCE(json_agg(m), '[]'::json)::text FROM
			(SELECT version, COALESCE(checksum_sha256, '') AS checksum FROM public.cms_schema_migrations ORDER BY version) m)`
		if identity {
			columns[1] = `(SELECT row_to_json(i)::text FROM
				(SELECT database_id::text AS database_id, lineage_root_id::text AS lineage_root_id, database_epoch AS epoch
				FROM public.wahb_database_identity WHERE singleton = TRUE) i)`
		}
	}
	if fence {
		columns[2] = `(SELECT row_to_json(f)::text FROM
			(SELECT state, epoch, fence_token::text FROM public.wahb_database_writer_fence WHERE singleton = TRUE) f)`
	}
	if !readContract && !fence {
		return state
	}
	var ledgerJSON, identityJSON, fenceJSON sql.NullString
	err = db.Raw("SELECT "+strings.Join(columns, ",\n")).Row().Scan(&ledgerJSON, &identityJSON, &fenceJSON)
	if err != nil {
		// A denied/disappeared relation must not erase readable evidence from
		// the other section. Only this error path uses the original readers.
		if readContract {
			state.ContractError = fmt.Errorf("read CMS migration ledger: %w", err)
			if ctx.Err() == nil {
				state.Contract, state.ContractError = ReadDatabaseContract(db, migrationsDir)
			}
		}
		if fence {
			state.FenceError = fmt.Errorf("read writer fence: %w", err)
			if ctx.Err() == nil {
				state.Fence, state.FenceError = ReadWriterFence(db)
			}
		}
		return state
	}
	if readContract {
		var applied []schemaMigrationRecord
		if err := json.Unmarshal([]byte(ledgerJSON.String), &applied); err != nil {
			state.ContractError = fmt.Errorf("read CMS migration ledger: %w", err)
		} else {
			state.Contract.SchemaDigest, state.Contract.LedgerState = compareCMSMigrationLedger(expected, applied)
			if identityJSON.Valid {
				if err := decodeHealthRow(identityJSON.String, &state.Contract, "database_id", "lineage_root_id", "epoch"); err != nil {
					state.ContractError = fmt.Errorf("read database identity: %w", err)
				} else {
					state.Contract.IdentityState = "present"
				}
			}
		}
	}
	if fence {
		if !fenceJSON.Valid {
			state.FenceError = fmt.Errorf("read writer fence: %w", sql.ErrNoRows)
		} else if err := decodeHealthRow(fenceJSON.String, &state.Fence, "state", "epoch", "fence_token"); err != nil {
			state.FenceError = fmt.Errorf("read writer fence: %w", err)
		}
	}
	return state
}

// SQL Scan rejects NULL for these scalar fields; JSON decoding alone would
// silently turn it into a valid-looking zero value during compatibility reads.
func decodeHealthRow(raw string, dest any, required ...string) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &fields); err != nil {
		return err
	}
	for _, name := range required {
		if len(fields[name]) == 0 || strings.TrimSpace(string(fields[name])) == "null" {
			return fmt.Errorf("required field %s is NULL or absent", name)
		}
	}
	return json.Unmarshal([]byte(raw), dest)
}
