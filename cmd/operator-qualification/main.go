// operator-qualification is the only supported report assembly path. It reads
// and writes the CMS qualification ledger directly; it never accepts a report
// JSON document, action plan, evidence body, browser credential, or prompt.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	operatorpkg "content-management-system/src/operator"
	"content-management-system/src/utils"
)

func main() {
	var (
		create      = flag.Bool("create", false, "snapshot immutable CMS qualification rows into a draft report")
		record      = flag.Bool("record", false, "record one immutable assessment for a completed CMS shadow run")
		signoff     = flag.Bool("signoff", false, "record one required report signoff")
		seal        = flag.Bool("seal", false, "validate and seal a signed-off CMS report")
		export      = flag.Bool("export", false, "emit a verified sealed CMS report envelope")
		reportID    = flag.String("report", "", "CMS shadow report UUID")
		runID       = flag.String("run", "", "CMS shadow run UUID for --record")
		caseID      = flag.String("case", "", "unique evaluation case ID for --record")
		cohort      = flag.String("cohort", "", "normal, briefing, or fault")
		grounded    = flag.Bool("grounded", false, "assessment grounding result")
		usefulness  = flag.Int("usefulness", 0, "reviewer usefulness rating 1..5")
		toolCorrect = flag.Bool("tool-correct", false, "domain/tool selection result")
		unsupported = flag.Int("unsupported-critical", 0, "critical unsupported-certainty count")
		faultCase   = flag.String("fault", "", "registered fault case for fault cohort")
		outcome     = flag.String("outcome", "", "passed, failed, or degraded")
		reviewer    = flag.String("reviewer", "", "reviewer identity")
		provenance  = flag.String("provenance", "", "production_snapshot or isolated_fixture")
		fingerprint = flag.String("result-fingerprint", "", "SHA-256 fingerprint of redacted result")
		role        = flag.String("role", "", "product, engineering, operations, or security")
		actor       = flag.String("actor", "", "signoff actor identity")
	)
	flag.Parse()
	actions := 0
	for _, action := range []bool{*create, *record, *signoff, *seal, *export} {
		if action {
			actions++
		}
	}
	if actions != 1 {
		log.Fatal("choose exactly one of --create, --record, --signoff, --seal, or --export")
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
	ctx := context.Background()

	// Qualification is explicitly enrolled and read-only by its CMS workflow.
	// Legacy launch-mode configuration must not control whether evidence can be
	// recorded or a report can be assembled.
	if *create {
		report, err := operatorpkg.CreateShadowQualificationReport(ctx, db)
		if err != nil {
			log.Fatalf("create CMS shadow report: %v", err)
		}
		fmt.Println(report.PublicID.String())
		return
	}
	if *record {
		assessment, err := operatorpkg.RecordShadowQualificationAssessment(ctx, db, operatorpkg.QualificationAssessmentInput{ShadowRunPublicID: strings.TrimSpace(*runID), EvaluationCaseID: strings.TrimSpace(*caseID), Cohort: strings.TrimSpace(*cohort), Grounded: *grounded, UsefulRating: *usefulness, DomainToolSelectionCorrect: *toolCorrect, UnsupportedCertaintyCritical: *unsupported, FaultCase: strings.TrimSpace(*faultCase), Outcome: strings.TrimSpace(*outcome), ReviewerID: strings.TrimSpace(*reviewer), Provenance: strings.TrimSpace(*provenance), ResultFingerprint: strings.TrimSpace(*fingerprint)})
		if err != nil {
			log.Fatalf("record immutable shadow assessment: %v", err)
		}
		fmt.Println(assessment.PublicID.String())
		return
	}
	if *signoff {
		signed, err := operatorpkg.AddShadowReportSignoff(ctx, db, strings.TrimSpace(*reportID), strings.TrimSpace(*role), strings.TrimSpace(*actor))
		if err != nil {
			log.Fatalf("record report signoff: %v", err)
		}
		fmt.Println(signed.PublicID.String())
		return
	}
	key, err := operatorpkg.PlanSigningKeyFromEnv()
	if err != nil {
		log.Fatalf("load Operator signing key: %v", err)
	}
	if *seal {
		report, err := operatorpkg.SealShadowQualificationReport(ctx, db, strings.TrimSpace(*reportID), key)
		if err != nil {
			log.Fatalf("seal CMS shadow report: %v", err)
		}
		fmt.Println(report.PublicID.String())
		return
	}
	envelope, err := operatorpkg.ExportSealedShadowQualificationReport(ctx, db, strings.TrimSpace(*reportID), key)
	if err != nil {
		log.Fatalf("export CMS shadow report: %v", err)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(envelope); err != nil {
		log.Fatalf("encode sealed report: %v", err)
	}
}
