package utils

import (
	"fmt"
	"gorm.io/gorm"
	"os"
	"strconv"
)

// CheckLocalPreparationAuthority runs inside the owner transaction. Managed
// preparation never releases a relocation fence or guesses an identity.
func CheckLocalPreparationAuthority(tx *gorm.DB) error {
	if err := CheckLocalPreparationIdentity(tx); err != nil {
		return err
	}
	if os.Getenv("WAHB_LOCAL_PREPARATION") != "1" {
		return nil
	}
	return CheckLocalMigrationWork(tx, nil)
}

// Owner repairs validate identity/fences without pretending the specific work
// they are reconciling has already been settled. Lifecycle claims still block.
func CheckLocalPreparationIdentity(tx *gorm.DB) error {
	if os.Getenv("WAHB_LOCAL_PREPARATION") != "1" {
		return nil
	}
	expected := os.Getenv("WAHB_PREPARATION_DATABASE_ID")
	var exists bool
	if err := tx.Raw("SELECT to_regclass('public.wahb_database_identity') IS NOT NULL").Scan(&exists).Error; err != nil {
		return err
	}
	if exists {
		if err := tx.Exec("LOCK TABLE wahb_database_identity IN SHARE MODE").Error; err != nil {
			return err
		}
		var rows []struct {
			ID    string
			Epoch int64
		}
		if err := tx.Raw("SELECT database_id::text AS id,database_epoch AS epoch FROM wahb_database_identity WHERE singleton=TRUE FOR SHARE").Scan(&rows).Error; err != nil {
			return err
		}
		if expected == "absent" {
			if len(rows) > 0 {
				return fmt.Errorf("identity appeared after preparation review")
			}
		} else {
			epoch, err := strconv.ParseInt(os.Getenv("WAHB_PREPARATION_DATABASE_EPOCH"), 10, 64)
			if err != nil || len(rows) != 1 || rows[0].ID != expected || rows[0].Epoch != epoch {
				return fmt.Errorf("identity or epoch changed after preparation review")
			}
		}
	} else if expected != "absent" {
		return fmt.Errorf("reviewed database identity is missing")
	}
	if err := tx.Raw("SELECT to_regclass('public.wahb_database_writer_fence') IS NOT NULL").Scan(&exists).Error; err != nil {
		return err
	}
	if exists {
		var f WriterFence
		if err := tx.Raw("SELECT state,epoch FROM wahb_database_writer_fence WHERE singleton=TRUE FOR SHARE").Scan(&f).Error; err != nil {
			return err
		}
		if f.State != "open" && f.State != "successor_open" {
			return fmt.Errorf("database writes are fenced")
		}
		if expected != "absent" && fmt.Sprint(f.Epoch) != os.Getenv("WAHB_PREPARATION_DATABASE_EPOCH") {
			return fmt.Errorf("writer fence epoch changed")
		}
	}
	if err := tx.Raw("SELECT to_regclass('public.database_migration_owner_control') IS NOT NULL").Scan(&exists).Error; err != nil {
		return err
	}
	if exists {
		var state string
		if err := tx.Raw("SELECT state FROM database_migration_owner_control WHERE singleton=TRUE FOR SHARE").Scan(&state).Error; err != nil {
			return err
		}
		if state != "running" {
			return fmt.Errorf("relocation owner has quiesced writes")
		}
	}
	if err := tx.Raw("SELECT to_regclass('public.lifecycle_operation_claims') IS NOT NULL").Scan(&exists).Error; err != nil {
		return err
	}
	if exists {
		var count int64
		if err := tx.Raw("SELECT count(*) FROM lifecycle_operation_claims WHERE state='active'").Scan(&count).Error; err != nil {
			return err
		}
		if count > 0 {
			return fmt.Errorf("active lifecycle claims must be resolved by their owner before preparation")
		}
	}
	return nil
}
