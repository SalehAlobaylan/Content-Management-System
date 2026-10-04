package main

import (
	"content-management-system/src/utils"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"gorm.io/gorm"
	"log"
	"os"
	"regexp"
	"strings"
)

func main() {
	id := flag.String("id", "", "reviewed new database identity")
	schema := flag.String("schema", "", "exact schema fingerprint from offline review")
	actor := flag.String("actor", "", "local operator identity")
	dir := flag.String("dir", "migrations", "reviewed canonical migration directory")
	legacyHistory := flag.String("legacy-checksum-history", "", "exact comma-separated pre-checksum legacy filenames acknowledged in this adoption review")
	flag.Parse()
	if os.Getenv("WAHB_LOCAL_PREPARATION") != "1" || !regexp.MustCompile(`^[a-f0-9-]{36}$`).MatchString(*id) || *actor == "" {
		log.Fatal("identity adoption requires explicit managed preparation review")
	}
	db, err := utils.ConnectDB()
	if err != nil {
		log.Fatal(err)
	}
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := utils.CheckLocalPreparationAuthority(tx); err != nil {
			return err
		}
		var schemaRows []struct {
			TableName  string
			ColumnName string
			UDTName    string
		}
		if e := tx.Raw("SELECT cols.table_name,cols.column_name,pg_catalog.format_type(a.atttypid,a.atttypmod) AS udt_name FROM information_schema.columns cols JOIN pg_namespace n ON n.nspname=cols.table_schema JOIN pg_class c ON c.relnamespace=n.oid AND c.relname=cols.table_name JOIN pg_attribute a ON a.attrelid=c.oid AND a.attname=cols.column_name WHERE cols.table_schema='public' ORDER BY cols.table_name,cols.ordinal_position").Scan(&schemaRows).Error; e != nil {
			return e
		}
		values := []string{}
		for _, r := range schemaRows {
			values = append(values, r.TableName+"."+r.ColumnName+":"+r.UDTName)
		}
		b, _ := json.Marshal(values)
		sum := sha256.Sum256(b)
		if hex.EncodeToString(sum[:]) != *schema {
			return fmt.Errorf("schema changed after identity review")
		}
		var existing int64
		if e := tx.Exec("LOCK TABLE wahb_database_identity IN EXCLUSIVE MODE").Error; e != nil {
			return e
		}
		if e := tx.Table("wahb_database_identity").Count(&existing).Error; e != nil {
			return e
		}
		if existing != 0 {
			return fmt.Errorf("identity already assigned; automatic adoption is refused")
		}
		var fence string
		if e := tx.Raw("SELECT state FROM wahb_database_writer_fence WHERE singleton=TRUE FOR SHARE").Scan(&fence).Error; e != nil {
			return e
		}
		if fence != "open" {
			return fmt.Errorf("identity adoption requires an open epoch-zero fence")
		}
		var epoch int64
		if e := tx.Raw("SELECT epoch FROM wahb_database_writer_fence WHERE singleton=TRUE").Scan(&epoch).Error; e != nil {
			return e
		}
		if epoch != 0 {
			return fmt.Errorf("identity adoption requires epoch zero")
		}
		c, e := utils.ReadDatabaseContract(tx, *dir)
		if e != nil {
			return e
		}
		history := []string{}
		if c.LedgerState != "verified" {
			if c.EnforcementMode == "enforce" || strings.TrimSpace(*legacyHistory) == "" {
				return fmt.Errorf("canonical CMS ledger must be verified before adoption; pre-checksum history needs a separate explicit compatibility review in observe mode")
			}
			history, e = utils.VerifyReviewedLegacyCMSHistory(tx, *dir, *legacyHistory)
			if e != nil {
				return e
			}
		} else if *legacyHistory != "" {
			return fmt.Errorf("legacy history changed after review; the ledger is now fully verified")
		}
		if e := tx.Exec("INSERT INTO wahb_database_identity(singleton,database_id,lineage_root_id,database_epoch,schema_contract_version) VALUES(TRUE,?, ?,0,?)", *id, *id, "cms/v1").Error; e != nil {
			return e
		}
		evidence, _ := json.Marshal(map[string]any{"actor": *actor, "schema": *schema, "operation": "explicit_local_identity_adoption", "legacy_checksum_history": history, "historical_checksums_available": len(history) == 0})
		return tx.Exec("INSERT INTO wahb_database_identity_events(database_id,lineage_root_id,database_epoch,event_type,evidence) VALUES(?,?,0,'local_identity_adopted',?::jsonb)", *id, *id, string(evidence)).Error
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Reviewed identity adopted")
}
