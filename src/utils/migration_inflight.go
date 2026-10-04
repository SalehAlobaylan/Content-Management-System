package utils

import (
	"fmt"

	"gorm.io/gorm"
)

// MigrationInflightProbe is a CMS-owned inventory of possibly effecting work.
type MigrationInflightProbe struct{ Subject, Query string }

var CMSMigrationInflightQueries = []MigrationInflightProbe{
	{"pipeline_stage_leases", `SELECT count(*) FROM pipeline_stage_leases WHERE state IN ('claimed','running','verifying','uncertain') AND (lease_expires_at IS NULL OR lease_expires_at > now())`},
	{"source_run_attempts", `SELECT count(*) FROM source_run_attempts WHERE state IN ('claimed','running','verifying','uncertain','reconciling')`},
	{"content_stage_attempts", `SELECT count(*) FROM content_stage_attempts WHERE state IN ('claimed','running','verifying','uncertain','reconciling')`},
	{"pipeline_repair_requests", `SELECT count(*) FROM pipeline_repair_requests WHERE state IN ('claimed','running','verifying','uncertain')`},
	{"artifact_coverage_requests", `SELECT count(*) FROM artifact_coverage_requests WHERE state IN ('claimed','running','verifying','uncertain')`},
	{"atomization_work_requests", `SELECT count(*) FROM atomization_work_requests WHERE state IN ('claimed','running','verifying','uncertain')`},
	{"studio_clearance_requests", `SELECT count(*) FROM studio_clearance_requests WHERE state IN ('claimed','running','verifying','uncertain')`},
	{"media_supply_actions", `SELECT count(*) FROM media_supply_actions WHERE state IN ('claimed','running','verifying','uncertain')`},
}

type MigrationWorkCheck struct {
	Subject          string `json:"subject"`
	Count            int64  `json:"count"`
	Expired          int64  `json:"expired,omitempty"`
	TerminalRequests int64  `json:"terminal_requests,omitempty"`
}

type MigrationWorkBlocker struct {
	Reason    string `json:"reason"`
	Migration string `json:"migration,omitempty"`
	Subject   string `json:"subject,omitempty"`
}

type MigrationWorkBlockedError struct{ Blocker MigrationWorkBlocker }

func (e *MigrationWorkBlockedError) Error() string {
	switch e.Blocker.Reason {
	case "migration_not_reviewed":
		return fmt.Sprintf("%s has no matching tested local policy for %s; review its exact SQL and classification before retrying", e.Blocker.Migration, e.Blocker.Subject)
	case "live_work":
		return fmt.Sprintf("%s has live owner work; let the owning workflow finish or reconcile it before retrying migrations", e.Blocker.Subject)
	default:
		message := fmt.Sprintf("%s contains possibly effecting work; reconcile it through its owner before preparation", e.Blocker.Subject)
		if e.Blocker.Migration != "" {
			message += " (migration: " + e.Blocker.Migration + ")"
		}
		return message
	}
}

type MigrationWorkReport struct {
	BlockedBy          *MigrationWorkBlocker `json:"blocked_by,omitempty"`
	SchemaUpgradeReady bool                  `json:"schema_upgrade_ready,omitempty"`
	State              string                `json:"state"`
	Count              int64                 `json:"count"`
	Checks             []MigrationWorkCheck  `json:"checks"`
}

// InspectCMSMigrationWork is read-only. These are the same possibly effecting
// records checked by local preparation, including leases that have expired.
// A terminal request is diagnostic evidence, never authority to settle an
// attempt: the owner must prove that attempt's effect before changing it.
func InspectCMSMigrationWork(tx *gorm.DB) (MigrationWorkReport, error) {
	r := MigrationWorkReport{State: "ready", Checks: []MigrationWorkCheck{}}
	probes := append([]MigrationInflightProbe{{"lifecycle_operation_claims", `SELECT count(*) FROM lifecycle_operation_claims WHERE state='active'`}}, CMSMigrationInflightQueries...)
	for _, probe := range probes {
		var exists bool
		if err := tx.Raw("SELECT to_regclass(?) IS NOT NULL", "public."+probe.Subject).Scan(&exists).Error; err != nil {
			return r, fmt.Errorf("cannot inspect migration work %s", probe.Subject)
		}
		if !exists {
			continue
		}
		check := MigrationWorkCheck{Subject: probe.Subject}
		if err := tx.Raw(probe.Query).Scan(&check.Count).Error; err != nil {
			return r, fmt.Errorf("migration work %s is unknown", probe.Subject)
		}
		if probe.Subject == "content_stage_attempts" && check.Count > 0 {
			if err := tx.Raw(`SELECT count(*) FROM content_stage_attempts WHERE state IN ('claimed','running','verifying','uncertain','reconciling') AND lease_expires_at<=now()`).Scan(&check.Expired).Error; err != nil {
				return r, fmt.Errorf("cannot inspect content-stage lease evidence")
			}
			if err := tx.Raw(`SELECT count(*) FROM content_stage_attempts a
				JOIN content_stage_requests r ON r.tenant_id=a.tenant_id AND r.public_id=a.request_id
				WHERE a.state IN ('claimed','running','verifying','uncertain','reconciling')
				AND r.state IN ('verified','failed','cancelled','superseded')`).Scan(&check.TerminalRequests).Error; err != nil {
				return r, fmt.Errorf("cannot inspect content-stage request evidence")
			}
		}
		r.Count += check.Count
		r.Checks = append(r.Checks, check)
	}
	if r.Count > 0 {
		r.State = "blocked"
	}
	return r, nil
}
