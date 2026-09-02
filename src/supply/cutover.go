package supply

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"content-management-system/src/models"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	admissionProtocolKey        = "source-run/v1"
	admissionEpochCompatibility = "compatibility"
	admissionEpochDurable       = "durable_required"
	admissionModeDurable        = "durable"
)

// CutoverCheck is intentionally serializable: operators can bind a promote
// request to the exact read-only evidence they reviewed.
type CutoverCheck struct {
	Name   string `json:"name"`
	Status string `json:"status"` // pass, fail, or unknown
	Detail string `json:"detail"`
}

type CutoverReport struct {
	SchemaState        string                     `json:"schema_state"`
	Epoch              string                     `json:"epoch"`
	Ready              bool                       `json:"ready"`
	VerificationDigest string                     `json:"verification_digest"`
	ActiveLanes        []string                   `json:"active_lanes"`
	Checks             []CutoverCheck             `json:"checks"`
	GeneratedAt        time.Time                  `json:"generated_at"`
	Admission          SourceAdmissionDiagnostics `json:"admission,omitempty"`
	CandidateOutcomes  map[string]int64           `json:"candidate_outcomes,omitempty"`
}

// SourceAdmissionDiagnostics makes a scheduler incident distinguishable from
// a healthy run that simply found no legal candidate. Counts are read-only and
// scoped to the active News/Media inventory.
type SourceAdmissionDiagnostics struct {
	Active                int64 `json:"active"`
	Due                   int64 `json:"due"`
	Scheduled             int64 `json:"scheduled"`
	InFlight              int64 `json:"in_flight"`
	Terminal              int64 `json:"terminal"`
	CircuitOpen           int64 `json:"circuit_open"`
	Unscheduled           int64 `json:"unscheduled"`
	DuplicateActiveGroups int64 `json:"duplicate_active_groups"`
}

type AdmissionMode string

const (
	AdmissionModeCompatibility AdmissionMode = "compatibility"
	AdmissionModeDurable       AdmissionMode = "durable"
)

// ResolveAdmissionMode reports which normal-operation source admission path
// owns one tenant lane. Missing pre-cutover schema/authority is compatibility;
// once the global epoch is durable, the lane must also be explicitly
// provisioned or the result is unknown and returned as an error.
func ResolveAdmissionMode(db *gorm.DB, tenantID, lane string) (AdmissionMode, error) {
	if db == nil || strings.TrimSpace(tenantID) == "" || (lane != "news" && lane != "media") {
		return "", fmt.Errorf("source-run admission mode requires explicit database, tenant, and lane")
	}
	if !db.Migrator().HasTable(&models.SourceRunAdmissionProtocol{}) {
		return AdmissionModeCompatibility, nil
	}
	var protocol models.SourceRunAdmissionProtocol
	if err := db.Where("protocol_key = ?", admissionProtocolKey).First(&protocol).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return AdmissionModeCompatibility, nil
		}
		return "", fmt.Errorf("source-run admission protocol is unavailable: %w", err)
	}
	mode, requiresLaneProof, err := classifyAdmissionEpoch(protocol.Epoch)
	if err != nil {
		return "", err
	}
	if requiresLaneProof {
		if err := RequireDurableAdmission(db, tenantID, lane); err != nil {
			return "", err
		}
	}
	return mode, nil
}

func classifyAdmissionEpoch(epoch string) (AdmissionMode, bool, error) {
	switch epoch {
	case admissionEpochCompatibility:
		return AdmissionModeCompatibility, false, nil
	case admissionEpochDurable:
		return AdmissionModeDurable, true, nil
	default:
		return "", false, fmt.Errorf("source-run admission protocol has an invalid epoch")
	}
}

// RequireDurableAdmission makes the cutover contract explicit at every new
// CMS admission/claim boundary. In compatibility the durable writer remains
// off; after the epoch moves to durable_required every tenant/lane must have
// an independently provisioned durable row. A missing table/row is never
// interpreted as an implicit default tenant or a successful cutover.
func RequireDurableAdmission(db *gorm.DB, tenantID, lane string) error {
	if db == nil || strings.TrimSpace(tenantID) == "" || strings.TrimSpace(lane) == "" {
		return fmt.Errorf("durable source-run admission requires explicit tenant and lane")
	}
	if lane != "media" && lane != "news" {
		return fmt.Errorf("source-run lane is not admitted")
	}
	var protocol models.SourceRunAdmissionProtocol
	if err := db.Where("protocol_key = ?", admissionProtocolKey).First(&protocol).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return fmt.Errorf("source-run durable admission is not activated")
		}
		return fmt.Errorf("source-run admission protocol is unavailable: %w", err)
	}
	if protocol.Epoch != admissionEpochDurable {
		return fmt.Errorf("source-run durable admission is not activated")
	}
	var cutover models.SourceRunAdmissionCutover
	if err := db.Where("tenant_id = ? AND lane = ?", tenantID, lane).First(&cutover).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return fmt.Errorf("source-run durable admission is not provisioned for tenant lane")
		}
		return fmt.Errorf("source-run admission cutover is unavailable: %w", err)
	}
	if cutover.Mode != admissionModeDurable || cutover.Protocol != ContractVersion {
		return fmt.Errorf("source-run tenant lane is not durable")
	}
	return nil
}

// RequireLegacyAdmission is the reciprocal compatibility guard used by old
// direct handlers. It preserves migration compatibility until the durable
// epoch is explicitly activated, then denies new legacy work.
func RequireLegacyAdmission(db *gorm.DB, tenantID, lane string) error {
	if db == nil || strings.TrimSpace(tenantID) == "" || strings.TrimSpace(lane) == "" {
		return fmt.Errorf("legacy source-run admission requires explicit tenant and lane")
	}
	// Deployment remains mixed-version compatible: before the forward migration
	// exists, the established legacy writer remains available. Once the table
	// exists, an unavailable/invalid protocol fails closed instead.
	if !db.Migrator().HasTable(&models.SourceRunAdmissionProtocol{}) {
		return nil
	}
	var protocol models.SourceRunAdmissionProtocol
	if err := db.Where("protocol_key = ?", admissionProtocolKey).First(&protocol).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil // Migration absent: preserve the pre-cutover compatibility path.
		}
		return fmt.Errorf("source-run admission protocol is unavailable: %w", err)
	}
	if protocol.Epoch == admissionEpochDurable {
		return fmt.Errorf("legacy source-run admission is retired")
	}
	if protocol.Epoch != admissionEpochCompatibility {
		return fmt.Errorf("source-run admission protocol has an invalid epoch")
	}
	return nil
}

// SourceRunCutoverStatus reports current authority without creating missing
// tables or rows. It is safe to run against an unapplied migration.
func SourceRunCutoverStatus(db *gorm.DB) (CutoverReport, error) {
	report := CutoverReport{SchemaState: "absent", Epoch: admissionEpochCompatibility, GeneratedAt: time.Now().UTC()}
	if db == nil {
		return report, fmt.Errorf("source-run cutover status requires a database")
	}
	if !db.Migrator().HasTable(&models.SourceRunAdmissionProtocol{}) {
		report.Checks = []CutoverCheck{{Name: "admission_schema", Status: "unknown", Detail: "source-run admission migration is not applied"}}
		report.VerificationDigest = digestCutoverReport(report)
		return report, nil
	}
	report.SchemaState = "present"
	var protocol models.SourceRunAdmissionProtocol
	if err := db.Where("protocol_key = ?", admissionProtocolKey).First(&protocol).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			report.Checks = []CutoverCheck{{Name: "admission_protocol", Status: "unknown", Detail: "protocol row is absent"}}
			report.VerificationDigest = digestCutoverReport(report)
			return report, nil
		}
		return report, err
	}
	report.Epoch = protocol.Epoch
	report.Checks = []CutoverCheck{{Name: "admission_protocol", Status: "pass", Detail: protocol.Epoch}}
	var lanes []string
	if err := db.Model(&models.SourceRunAdmissionCutover{}).Where("mode = ?", admissionModeDurable).Select("tenant_id || ':' || lane").Order("tenant_id, lane").Find(&lanes).Error; err != nil {
		return report, err
	}
	report.ActiveLanes = lanes
	report.Ready = protocol.Epoch == admissionEpochDurable
	report.VerificationDigest = digestCutoverReport(report)
	return report, nil
}

// SourceRunCutoverPreflight is read-only. Every check is explicit so a
// missing operational proof remains visible instead of being interpreted as
// a successful compatibility default.
func SourceRunCutoverPreflight(db *gorm.DB) (CutoverReport, error) {
	report := CutoverReport{SchemaState: "absent", Epoch: admissionEpochCompatibility, GeneratedAt: time.Now().UTC()}
	if db == nil {
		return report, fmt.Errorf("source-run cutover preflight requires a database")
	}
	report.Checks = append(report.Checks, CutoverCheck{Name: "admission_schema", Status: "fail", Detail: "source-run admission migration is not applied"})
	if !db.Migrator().HasTable(&models.SourceRunAdmissionProtocol{}) {
		return finalizeCutoverReport(report), nil
	}
	report.SchemaState = "present"
	var protocol models.SourceRunAdmissionProtocol
	if err := db.Where("protocol_key = ?", admissionProtocolKey).First(&protocol).Error; err != nil {
		return report, err
	}
	report.Epoch = protocol.Epoch
	report.Checks[0] = CutoverCheck{Name: "admission_schema", Status: "pass", Detail: "canonical admission tables are present"}
	report.Checks = append(report.Checks, CutoverCheck{Name: "epoch", Status: ternary(protocol.Epoch == admissionEpochCompatibility, "pass", "fail"), Detail: protocol.Epoch})

	var sources []models.ContentSource
	if err := db.Where("is_active = TRUE AND category IN ?", []string{models.SourceCategoryNews, models.SourceCategoryMedia}).Order("tenant_id, category, public_id").Find(&sources).Error; err != nil {
		return report, err
	}
	report.Admission = readSourceAdmissionDiagnostics(db, sources, time.Now().UTC())
	laneSet := map[string]bool{}
	for _, source := range sources {
		laneSet[source.TenantID+":"+source.Category] = true
	}
	lanes := make([]string, 0, len(laneSet))
	for lane := range laneSet {
		lanes = append(lanes, lane)
	}
	sort.Strings(lanes)
	report.ActiveLanes = lanes
	if len(lanes) == 0 {
		report.Checks = append(report.Checks, CutoverCheck{Name: "active_tenant_lane_coverage", Status: "fail", Detail: "no active source lane exists"})
	} else {
		report.Checks = append(report.Checks, CutoverCheck{Name: "active_tenant_lane_coverage", Status: "pass", Detail: fmt.Sprintf("%d active tenant/lane combinations", len(lanes))})
	}
	report.Checks = append(report.Checks, CutoverCheck{Name: "explicit_schedules", Status: ternary(report.Admission.Unscheduled == 0, "pass", "fail"), Detail: fmt.Sprintf("%d active sources lack next_due_at", report.Admission.Unscheduled)})
	duplicateCount := int64(0)
	if db.Migrator().HasTable(&models.SourceRunRequest{}) {
		if err := db.Raw(`SELECT COUNT(*) FROM (SELECT tenant_id, content_source_id FROM source_run_requests WHERE state IN ('requested','accepted','running','verification_required') GROUP BY tenant_id, content_source_id HAVING COUNT(*) > 1) duplicates`).Scan(&duplicateCount).Error; err != nil {
			return report, err
		}
	}
	report.Admission.DuplicateActiveGroups = duplicateCount
	report.Checks = append(report.Checks, CutoverCheck{Name: "duplicate_active_source_runs", Status: ternary(duplicateCount == 0, "pass", "fail"), Detail: fmt.Sprintf("%d source(s) have duplicate active runs", duplicateCount)})
	schedulerStatus := "unknown"
	if !protocol.UpdatedAt.IsZero() && time.Since(protocol.UpdatedAt.UTC()) <= sourceRunSchedulerHeartbeatGrace {
		schedulerStatus = "pass"
	} else {
		schedulerStatus = "fail"
	}
	report.Checks = append(report.Checks, CutoverCheck{Name: "cms_scheduler_readiness", Status: schedulerStatus, Detail: "durable CMS source-run scheduler heartbeat"})
	readyLaneSnapshots, expectedLaneSnapshots := 0, len(lanes)
	if db.Migrator().HasTable(&models.PipelineLaneHealthSnapshot{}) {
		for _, tenantLane := range lanes {
			parts := strings.SplitN(tenantLane, ":", 2)
			if len(parts) != 2 {
				continue
			}
			contentLane := parts[1]
			if contentLane == models.SourceCategoryMedia {
				contentLane = models.ContentStageLanePods
			}
			var count int64
			if err := db.Model(&models.PipelineLaneHealthSnapshot{}).
				Where("tenant_id=? AND lane=? AND owner_principal=? AND captured_at>=?", parts[0], contentLane, "aggregation", time.Now().UTC().Add(-45*time.Second)).
				Count(&count).Error; err != nil {
				return report, err
			}
			if count > 0 {
				readyLaneSnapshots++
			}
		}
	}
	report.Checks = append(report.Checks, CutoverCheck{
		Name:   "aggregation_lane_readiness",
		Status: ternary(expectedLaneSnapshots > 0 && readyLaneSnapshots == expectedLaneSnapshots, "pass", "fail"),
		Detail: fmt.Sprintf("%d/%d active tenant lanes have a current Aggregation snapshot", readyLaneSnapshots, expectedLaneSnapshots),
	})
	retained := int64(0)
	if db.Migrator().HasTable(&models.SourceRunRetainedReceipt{}) {
		if err := db.Model(&models.SourceRunRetainedReceipt{}).Where("state = ?", "retained").Count(&retained).Error; err != nil {
			return report, err
		}
	}
	report.Checks = append(report.Checks, CutoverCheck{Name: "retained_receipts", Status: ternary(retained == 0, "pass", "fail"), Detail: fmt.Sprintf("%d retained receipt(s) await delivery", retained)})
	legacyRequired := int64(0)
	if db.Migrator().HasTable(&models.SourceRunRequest{}) {
		_ = db.Model(&models.SourceRunRequest{}).Where("lane = 'legacy' AND state IN ?", models.SourceRunActiveStates).Count(&legacyRequired)
	}
	RefreshSupplyOwnerReadiness(time.Now().UTC())
	legacyEvidence := SupplyOwnerReadinessAt(time.Now().UTC())["legacy_drain"]
	legacyReady := legacyRequired == 0 || legacyEvidence.State == "ready"
	legacyDetail := "aggregate topology legacy-drain capability"
	if legacyRequired == 0 {
		legacyDetail = "not required: no active legacy source-run work"
	} else if legacyEvidence.Detail != "" {
		legacyDetail = legacyEvidence.Detail
	}
	report.Checks = append(report.Checks, CutoverCheck{Name: "legacy_drain", Status: ternary(legacyReady, "pass", "fail"), Detail: legacyDetail})
	report.CandidateOutcomes = readCandidateOutcomes(db)
	return finalizeCutoverReport(report), nil
}

func readSourceAdmissionDiagnostics(db *gorm.DB, sources []models.ContentSource, now time.Time) SourceAdmissionDiagnostics {
	d := SourceAdmissionDiagnostics{Active: int64(len(sources))}
	for _, source := range sources {
		if source.NextDueAt == nil {
			d.Unscheduled++
		} else {
			d.Scheduled++
			if !source.NextDueAt.After(now) {
				d.Due++
			}
		}
		if source.IntakeCircuitUntil != nil && source.IntakeCircuitUntil.After(now) {
			d.CircuitOpen++
		}
	}
	if db.Migrator().HasTable(&models.SourceRunRequest{}) {
		_ = db.Model(&models.SourceRunRequest{}).Where("state IN ?", models.SourceRunActiveStates).Count(&d.InFlight)
		_ = db.Model(&models.SourceRunRequest{}).Where("state NOT IN ? AND updated_at >= ?", models.SourceRunActiveStates, now.Add(-30*24*time.Hour)).Count(&d.Terminal)
	}
	return d
}

func readCandidateOutcomes(db *gorm.DB) map[string]int64 {
	result := map[string]int64{"legal": 0, "filtered_short": 0, "no_change": 0, "provider_failure": 0, "budget_truncated": 0}
	if db == nil || !db.Migrator().HasTable(&models.SourceRunTelemetry{}) {
		return result
	}
	var rows []struct {
		Outcome string `gorm:"column:outcome"`
		Count   int64  `gorm:"column:count"`
	}
	if db.Raw(`SELECT COALESCE(metadata->>'outcome','unknown') AS outcome, COUNT(*) AS count FROM source_run_telemetry WHERE updated_at >= ? GROUP BY COALESCE(metadata->>'outcome','unknown')`, time.Now().UTC().Add(-30*24*time.Hour)).Scan(&rows).Error != nil {
		return result
	}
	for _, row := range rows {
		switch row.Outcome {
		case "legal_candidates", "new_items":
			result["legal"] += row.Count
		case "filtered_short_or_invalid", "filtered_short":
			result["filtered_short"] += row.Count
		case "provider_failure", "provider_failed":
			result["provider_failure"] += row.Count
		case "budget_truncated":
			result["budget_truncated"] += row.Count
		case "no_change":
			result["no_change"] += row.Count
		}
	}
	return result
}

// PromoteSourceRunCutover is the only forward mutation. It requires the
// exact current preflight digest and performs the lane provisioning plus epoch
// change in one transaction. This implementation does not invoke it.
func PromoteSourceRunCutover(db *gorm.DB, expectedDigest, actor string) error {
	if strings.TrimSpace(expectedDigest) == "" || strings.TrimSpace(actor) == "" {
		return fmt.Errorf("promotion requires preflight digest and actor")
	}
	report, err := SourceRunCutoverPreflight(db)
	if err != nil {
		return err
	}
	if report.VerificationDigest != expectedDigest {
		return fmt.Errorf("preflight digest changed; run preflight again")
	}
	if !report.ReadyForPromotion() {
		return fmt.Errorf("source-run cutover preflight is not ready")
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var protocol models.SourceRunAdmissionProtocol
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("protocol_key = ?", admissionProtocolKey).First(&protocol).Error; err != nil {
			return err
		}
		if protocol.Epoch != admissionEpochCompatibility {
			return fmt.Errorf("source-run epoch is no longer compatibility")
		}
		now := time.Now().UTC()
		for _, lane := range report.ActiveLanes {
			parts := strings.SplitN(lane, ":", 2)
			if len(parts) != 2 {
				return fmt.Errorf("invalid active lane %q", lane)
			}
			cutover := models.SourceRunAdmissionCutover{TenantID: parts[0], Lane: parts[1]}
			if err := tx.Where("tenant_id = ? AND lane = ?", parts[0], parts[1]).Assign(models.SourceRunAdmissionCutover{Mode: admissionModeDurable, Protocol: ContractVersion, Version: protocol.Version + 1, ActivatedAt: &now, ActivatedBy: actor}).FirstOrCreate(&cutover).Error; err != nil {
				return err
			}
		}
		protocol.Epoch, protocol.Version, protocol.ActivatedAt, protocol.ActivatedBy = admissionEpochDurable, protocol.Version+1, &now, actor
		if err := tx.Save(&protocol).Error; err != nil {
			return err
		}
		if tx.Migrator().HasTable(&models.SourceRunCutoverAuditEvent{}) {
			payload, _ := json.Marshal(map[string]any{"schema_version": ContractVersion, "active_lanes": report.ActiveLanes})
			if err := tx.Create(&models.SourceRunCutoverAuditEvent{EventType: "epoch_promoted", FromEpoch: admissionEpochCompatibility, ToEpoch: admissionEpochDurable, VerificationDigest: report.VerificationDigest, Actor: actor, Payload: payload, OccurredAt: now}).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func (r CutoverReport) ReadyForPromotion() bool {
	if len(r.ActiveLanes) == 0 {
		return false
	}
	for _, check := range r.Checks {
		if check.Status != "pass" {
			return false
		}
	}
	return r.Epoch == admissionEpochCompatibility
}

func finalizeCutoverReport(report CutoverReport) CutoverReport {
	report.Ready = report.ReadyForPromotion()
	report.VerificationDigest = digestCutoverReport(report)
	return report
}

func digestCutoverReport(report CutoverReport) string {
	copyReport := report
	copyReport.VerificationDigest = ""
	copyReport.GeneratedAt = time.Time{}
	encoded, _ := json.Marshal(copyReport)
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func ternary(condition bool, yes, no string) string {
	if condition {
		return yes
	}
	return no
}
