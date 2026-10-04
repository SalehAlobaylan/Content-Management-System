package main

import (
	"content-management-system/src/contentstage"
	"content-management-system/src/utils"
	"database/sql"
	"encoding/json"
	"flag"
	"gorm.io/gorm"
	"log"
	"os"
	"time"
)

func main() {
	apply := flag.Bool("apply", false, "apply only reviewed, proven attempt ledger repairs")
	digest := flag.String("digest", "", "exact owner preview digest")
	actor := flag.String("actor", "", "reviewing administrator")
	flag.Parse()
	if flag.NArg() != 0 || (!*apply && (*digest != "" || *actor != "")) {
		log.Fatal("unsupported repair arguments")
	}
	if *apply && os.Getenv("WAHB_LOCAL_PREPARATION") != "1" {
		log.Fatal("repair requires managed, quiescent local preparation")
	}
	db, err := utils.ConnectDB()
	if err != nil {
		log.Fatal("CMS repair database unavailable")
	}
	var preview contentstage.MigrationAttemptRepairPreview
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("SET LOCAL statement_timeout='30s'").Error; err != nil {
			return err
		}
		if !*apply {
			var err error
			preview, err = contentstage.PreviewMigrationAttemptRepair(tx, time.Now().UTC())
			return err
		}
		if err := utils.CheckLocalPreparationIdentity(tx); err != nil {
			return err
		}
		var err error
		preview, err = contentstage.ApplyMigrationAttemptRepair(tx, *digest, *actor, time.Now().UTC())
		return err
	}, &sql.TxOptions{ReadOnly: !*apply, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		log.Fatal(err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(preview); err != nil {
		log.Fatal(err)
	}
}
