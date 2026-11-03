// source-run is a read-first operator tool for the source-run migration
// cutover. Promotion is digest-bound and never happens implicitly.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"content-management-system/src/supply"
	"content-management-system/src/utils"
)

func main() {
	status := flag.Bool("status", false, "report current source-run authority")
	preflight := flag.Bool("preflight", false, "run the read-only cutover preflight")
	promote := flag.Bool("promote", false, "promote using the exact preflight digest")
	repairPreview := flag.Bool("repair-preview", false, "preview duplicate and stale compatibility source-run repair")
	repairApply := flag.Bool("repair-apply", false, "apply the exact compatibility repair preview")
	digest := flag.String("digest", "", "verification digest returned by --preflight")
	actor := flag.String("actor", "", "human/operator identity for promotion")
	lanePreview := flag.Bool("inactive-lane-preview", false, "review provisioning one inactive lane after durable cutover")
	laneApply := flag.Bool("inactive-lane-apply", false, "provision an inactive lane using its exact preview digest")
	tenant := flag.String("tenant", "", "explicit tenant for inactive lane provisioning")
	lane := flag.String("lane", "", "explicit news or media lane")
	flag.Parse()
	actions := 0
	for _, selected := range []bool{*status, *preflight, *promote, *repairPreview, *repairApply, *lanePreview, *laneApply} {
		if selected {
			actions++
		}
	}
	if actions != 1 {
		log.Fatal("choose exactly one of --status, --preflight, --promote, --repair-preview, --repair-apply, --inactive-lane-preview, or --inactive-lane-apply")
	}
	db, err := utils.ConnectDB()
	if err != nil {
		log.Fatalf("connect CMS database: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		log.Fatalf("get database connection: %v", err)
	}
	defer sqlDB.Close()
	if *lanePreview || *laneApply {
		if *laneApply {
			if err := supply.ProvisionInactiveLane(db, *tenant, *lane, *digest, *actor); err != nil {
				log.Fatal(err)
			}
			fmt.Println("inactive source lane provisioned")
			return
		}
		report, err := supply.PreviewInactiveLane(db, *tenant, *lane)
		if err != nil {
			log.Fatal(err)
		}
		if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
			log.Fatal(err)
		}
		return
	}

	if *promote {
		if err := supply.PromoteSourceRunCutover(db, strings.TrimSpace(*digest), strings.TrimSpace(*actor)); err != nil {
			log.Fatal(err)
		}
		fmt.Println("source-run cutover promoted")
		return
	}
	if *repairPreview || *repairApply {
		var report supply.LegacySourceRunRepairReport
		if *repairApply {
			report, err = supply.ApplyLegacySourceRunRepair(db, strings.TrimSpace(*digest), strings.TrimSpace(*actor), time.Now().UTC())
		} else {
			report, err = supply.PreviewLegacySourceRunRepair(db, time.Now().UTC())
		}
		if err != nil {
			log.Fatal(err)
		}
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetEscapeHTML(false)
		if err := encoder.Encode(report); err != nil {
			log.Fatalf("encode repair report: %v", err)
		}
		return
	}
	var report supply.CutoverReport
	if *status {
		report, err = supply.SourceRunCutoverStatus(db)
	} else {
		report, err = supply.SourceRunCutoverPreflight(db)
	}
	if err != nil {
		log.Fatal(err)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(report); err != nil {
		log.Fatalf("encode cutover report: %v", err)
	}
}
