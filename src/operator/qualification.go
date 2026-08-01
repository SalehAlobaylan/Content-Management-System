package operator

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

const (
	ShadowReportSchemaVersion = "wahb-operator/shadow-report/v1"
	shadowReportSealNamespace = "wahb-operator-shadow-report/v1:"
)

var (
	QualificationLocales    = []string{"ar", "en"}
	QualificationFaultCases = []string{
		"stale_evidence", "conflicting_evidence", "cross_tenant", "cancel_before_start", "rollback_window", "worker_crash", "llm_outage", "iam_outage", "provider_outage", "read_kill_switch", "llm_kill_switch", "execution_kill_switch", "schedule_kill_switch", "adapter_kill_switch", "tool_kill_switch",
	}
	QualificationSignoffRoles = []string{"product", "engineering", "operations", "security"}
)

type QualificationAssessmentInput struct {
	ShadowRunPublicID            string
	EvaluationCaseID             string
	Cohort                       string
	Grounded                     bool
	UsefulRating                 int
	DomainToolSelectionCorrect   bool
	UnsupportedCertaintyCritical int
	FaultCase                    string
	Outcome                      string
	ReviewerID                   string
	Provenance                   string
	ResultFingerprint            string
}

// QualificationRun is the report-safe join of an immutable shadow snapshot
// and its immutable rubric assessment. It intentionally omits all evidence
// content and user identity beyond the reviewer ID required for accountability.
type QualificationRun struct {
	RealSnapshotID               string `json:"real_snapshot_id"`
	EvaluationCaseID             string `json:"evaluation_case_id"`
	TenantID                     string `json:"tenant_id"`
	Domain                       string `json:"domain"`
	Locale                       string `json:"locale"`
	Kind                         string `json:"kind"`
	Grounded                     bool   `json:"grounded"`
	UsefulRating                 int    `json:"useful_rating"`
	DomainToolSelectionCorrect   bool   `json:"domain_tool_selection_correct"`
	UnsupportedCertaintyCritical int    `json:"unsupported_certainty_critical"`
	LatencyMS                    int64  `json:"latency_ms"`
	FaultCase                    string `json:"fault_case,omitempty"`
	Outcome                      string `json:"outcome"`
	AccessVersionHash            string `json:"access_version_hash"`
	PacketFingerprint            string `json:"packet_fingerprint"`
	ResultFingerprint            string `json:"result_fingerprint"`
	ReviewerID                   string `json:"reviewer_id"`
	Provenance                   string `json:"provenance"`
}

type ShadowQualificationPayload struct {
	SchemaVersion       string             `json:"schema_version"`
	Source              string             `json:"source"`
	LaunchMode          string             `json:"launch_mode"`
	EnvironmentIdentity string             `json:"environment_identity"`
	Runs                []QualificationRun `json:"runs"`
}

type ShadowQualificationEnvelope struct {
	SchemaVersion string                               `json:"schema_version"`
	Source        string                               `json:"source"`
	ReportID      string                               `json:"report_id"`
	Digest        string                               `json:"digest"`
	Seal          string                               `json:"seal"`
	Signoffs      []models.OperatorShadowReportSignoff `json:"signoffs"`
	Report        ShadowQualificationPayload           `json:"report"`
}

func qualificationEnvironmentIdentity() string {
	environment := strings.ToLower(strings.TrimSpace(os.Getenv("ENV")))
	if environment == "" {
		return "development"
	}
	return environment
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func validateQualificationAssessment(input QualificationAssessmentInput, environment string) error {
	if _, err := uuid.Parse(strings.TrimSpace(input.ShadowRunPublicID)); err != nil {
		return fmt.Errorf("%w: qualification shadow run ID is invalid", ErrInvalidContract)
	}
	if strings.TrimSpace(input.EvaluationCaseID) == "" || len(input.EvaluationCaseID) > 160 || input.UsefulRating < 1 || input.UsefulRating > 5 || input.UnsupportedCertaintyCritical < 0 || strings.TrimSpace(input.ReviewerID) == "" || len(input.ResultFingerprint) != 64 {
		return fmt.Errorf("%w: qualification assessment fields are invalid", ErrInvalidContract)
	}
	if input.Cohort != "normal" && input.Cohort != "briefing" && input.Cohort != "fault" {
		return fmt.Errorf("%w: qualification cohort is invalid", ErrInvalidContract)
	}
	if input.Outcome != "passed" && input.Outcome != "failed" && input.Outcome != "degraded" {
		return fmt.Errorf("%w: qualification outcome is invalid", ErrInvalidContract)
	}
	if input.Provenance != "production_snapshot" && input.Provenance != "isolated_fixture" {
		return fmt.Errorf("%w: qualification provenance is invalid", ErrInvalidContract)
	}
	if (input.Cohort == "fault") != (strings.TrimSpace(input.FaultCase) != "") {
		return fmt.Errorf("%w: fault cohort and fault case must agree", ErrInvalidContract)
	}
	if input.FaultCase != "" && !containsString(QualificationFaultCases, input.FaultCase) {
		return fmt.Errorf("%w: qualification fault case is not registered", ErrInvalidContract)
	}
	if environment == "production" && (input.Cohort == "fault" || input.Provenance != "production_snapshot") {
		return fmt.Errorf("%w: production qualification accepts real snapshot assessments only", ErrInvalidContract)
	}
	if _, err := hex.DecodeString(input.ResultFingerprint); err != nil {
		return fmt.Errorf("%w: result fingerprint is invalid", ErrInvalidContract)
	}
	return nil
}

// RecordShadowQualificationAssessment is intentionally service/CLI-only: no
// admin route can inject fixture verdicts into a production shadow run.
func RecordShadowQualificationAssessment(ctx context.Context, db *gorm.DB, input QualificationAssessmentInput) (models.OperatorShadowAssessment, error) {
	environment := qualificationEnvironmentIdentity()
	if err := validateQualificationAssessment(input, environment); err != nil {
		return models.OperatorShadowAssessment{}, err
	}
	var run models.OperatorShadowRun
	if err := db.WithContext(ctx).Where("public_id=?", input.ShadowRunPublicID).First(&run).Error; err != nil {
		return models.OperatorShadowAssessment{}, fmt.Errorf("load qualification shadow run: %w", err)
	}
	if run.State != "completed" || strings.TrimSpace(run.PacketFingerprint) == "" || strings.TrimSpace(run.AccessVersionHash) == "" {
		return models.OperatorShadowAssessment{}, fmt.Errorf("%w: only completed shadow snapshots with access provenance can be assessed", ErrInvalidContract)
	}
	assessment := models.OperatorShadowAssessment{
		ShadowRunID: run.ID, EvaluationCaseID: input.EvaluationCaseID, Cohort: input.Cohort, Grounded: input.Grounded,
		UsefulRating: input.UsefulRating, DomainToolSelectionCorrect: input.DomainToolSelectionCorrect,
		UnsupportedCertaintyCritical: input.UnsupportedCertaintyCritical, FaultCase: input.FaultCase, Outcome: input.Outcome,
		ReviewerID: input.ReviewerID, Provenance: input.Provenance, ResultFingerprint: input.ResultFingerprint,
	}
	if err := db.WithContext(ctx).Create(&assessment).Error; err != nil {
		return models.OperatorShadowAssessment{}, fmt.Errorf("persist immutable qualification assessment: %w", err)
	}
	return assessment, nil
}

func qualificationRuns(ctx context.Context, db *gorm.DB) ([]QualificationRun, []uint, error) {
	type joined struct {
		RunPublicID                  string
		RunID                        uint
		TenantID                     string
		Domain                       string
		Locale                       string
		LatencyMS                    int64
		AccessVersionHash            string
		PacketFingerprint            string
		EvaluationCaseID             string
		Cohort                       string
		Grounded                     bool
		UsefulRating                 int
		DomainToolSelectionCorrect   bool
		UnsupportedCertaintyCritical int
		FaultCase                    string
		Outcome                      string
		ReviewerID                   string
		Provenance                   string
		ResultFingerprint            string
	}
	var rows []joined
	err := db.WithContext(ctx).Table("operator_shadow_assessments AS a").
		Select("r.public_id AS run_public_id, r.id AS run_id, r.tenant_id, r.domain, r.locale, r.latency_ms, r.access_version_hash, r.packet_fingerprint, a.evaluation_case_id, a.cohort, a.grounded, a.useful_rating, a.domain_tool_selection_correct, a.unsupported_certainty_critical, a.fault_case, a.outcome, a.reviewer_id, a.provenance, a.result_fingerprint").
		Joins("JOIN operator_shadow_runs AS r ON r.id = a.shadow_run_id").
		Where("r.state=?", "completed").Order("a.evaluation_case_id ASC").Find(&rows).Error
	if err != nil {
		return nil, nil, fmt.Errorf("load immutable qualification rows: %w", err)
	}
	runs := make([]QualificationRun, 0, len(rows))
	runIDs := make([]uint, 0, len(rows))
	for _, row := range rows {
		runs = append(runs, QualificationRun{RealSnapshotID: row.RunPublicID, EvaluationCaseID: row.EvaluationCaseID, TenantID: row.TenantID, Domain: row.Domain, Locale: row.Locale, Kind: row.Cohort, Grounded: row.Grounded, UsefulRating: row.UsefulRating, DomainToolSelectionCorrect: row.DomainToolSelectionCorrect, UnsupportedCertaintyCritical: row.UnsupportedCertaintyCritical, LatencyMS: row.LatencyMS, FaultCase: row.FaultCase, Outcome: row.Outcome, AccessVersionHash: row.AccessVersionHash, PacketFingerprint: row.PacketFingerprint, ResultFingerprint: row.ResultFingerprint, ReviewerID: row.ReviewerID, Provenance: row.Provenance})
		runIDs = append(runIDs, row.RunID)
	}
	return runs, runIDs, nil
}

func reportDigest(payload ShadowQualificationPayload) (string, []byte, error) {
	// json.Marshal preserves struct field order, whereas the offline verifier
	// deliberately uses lexical key order. Normalize through a map so CMS and
	// every verifier sign the same stable JSON bytes.
	initial, err := json.Marshal(payload)
	if err != nil {
		return "", nil, fmt.Errorf("serialize qualification payload: %w", err)
	}
	var canonical any
	if err := json.Unmarshal(initial, &canonical); err != nil {
		return "", nil, fmt.Errorf("normalize qualification payload: %w", err)
	}
	serialized, err := json.Marshal(canonical)
	if err != nil {
		return "", nil, fmt.Errorf("serialize normalized qualification payload: %w", err)
	}
	sum := sha256.Sum256(serialized)
	return hex.EncodeToString(sum[:]), serialized, nil
}

func shadowReportSeal(key []byte, digest string) (string, error) {
	if len(key) < 32 || len(digest) != 64 {
		return "", fmt.Errorf("%w: shadow report signing material is invalid", ErrInvalidContract)
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(shadowReportSealNamespace + digest))
	return hex.EncodeToString(mac.Sum(nil)), nil
}

// CreateShadowQualificationReport snapshots all currently recorded immutable
// assessments. It is intentionally allowed to create an incomplete draft; the
// strict coverage gates apply before sign/seal and therefore fail closed.
func CreateShadowQualificationReport(ctx context.Context, db *gorm.DB) (models.OperatorShadowReport, error) {
	runs, runIDs, err := qualificationRuns(ctx, db)
	if err != nil {
		return models.OperatorShadowReport{}, err
	}
	payload := ShadowQualificationPayload{SchemaVersion: ShadowReportSchemaVersion, Source: "cms_shadow_ledger", LaunchMode: string(LaunchModeShadow), EnvironmentIdentity: qualificationEnvironmentIdentity(), Runs: runs}
	digest, serialized, err := reportDigest(payload)
	if err != nil {
		return models.OperatorShadowReport{}, err
	}
	report := models.OperatorShadowReport{SchemaVersion: ShadowReportSchemaVersion, EnvironmentIdentity: payload.EnvironmentIdentity, LaunchMode: string(LaunchModeShadow), State: "draft", Payload: datatypes.JSON(serialized), ReportDigest: digest}
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&report).Error; err != nil {
			return err
		}
		for _, runID := range runIDs {
			if err := tx.Create(&models.OperatorShadowReportRun{ReportID: report.ID, RunID: runID}).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return models.OperatorShadowReport{}, fmt.Errorf("persist CMS shadow report: %w", err)
	}
	return report, nil
}

func parseReportPayload(report models.OperatorShadowReport) (ShadowQualificationPayload, error) {
	var payload ShadowQualificationPayload
	if err := json.Unmarshal(report.Payload, &payload); err != nil {
		return payload, fmt.Errorf("decode stored shadow report: %w", err)
	}
	if payload.SchemaVersion != ShadowReportSchemaVersion || payload.Source != "cms_shadow_ledger" || payload.LaunchMode != string(LaunchModeShadow) || payload.EnvironmentIdentity != report.EnvironmentIdentity {
		return payload, fmt.Errorf("%w: stored shadow report contract is invalid", ErrInvalidContract)
	}
	digest, _, err := reportDigest(payload)
	if err != nil || !hmac.Equal([]byte(digest), []byte(report.ReportDigest)) {
		return payload, fmt.Errorf("%w: stored shadow report digest is invalid", ErrInvalidContract)
	}
	return payload, nil
}

func requiredCoverage(runs []QualificationRun) error {
	pairs, faults, cases := map[string]bool{}, map[string]bool{}, map[string]bool{}
	normal, briefing, fault := []QualificationRun{}, []QualificationRun{}, []QualificationRun{}
	unsupported := 0
	for _, run := range runs {
		if run.RealSnapshotID == "" || run.EvaluationCaseID == "" || cases[run.EvaluationCaseID] || !containsString(ShadowDomains, run.Domain) || !containsString(QualificationLocales, run.Locale) || len(run.AccessVersionHash) != 64 || len(run.PacketFingerprint) != 64 || len(run.ResultFingerprint) != 64 {
			return fmt.Errorf("%w: qualification run identity/provenance is invalid", ErrInvalidContract)
		}
		cases[run.EvaluationCaseID] = true
		pairs[run.Domain+":"+run.Locale] = true
		unsupported += run.UnsupportedCertaintyCritical
		switch run.Kind {
		case "normal":
			normal = append(normal, run)
		case "briefing":
			briefing = append(briefing, run)
		case "fault":
			if !containsString(QualificationFaultCases, run.FaultCase) {
				return fmt.Errorf("%w: fault qualification case is invalid", ErrInvalidContract)
			}
			faults[run.FaultCase] = true
			fault = append(fault, run)
		default:
			return fmt.Errorf("%w: qualification cohort is invalid", ErrInvalidContract)
		}
	}
	if len(normal) == 0 || len(briefing) == 0 || len(fault) == 0 {
		return fmt.Errorf("%w: qualification cohorts are incomplete", ErrInvalidContract)
	}
	for _, domain := range ShadowDomains {
		for _, locale := range QualificationLocales {
			if !pairs[domain+":"+locale] {
				return fmt.Errorf("%w: missing domain/locale qualification %s/%s", ErrInvalidContract, domain, locale)
			}
		}
	}
	for _, faultCase := range QualificationFaultCases {
		if !faults[faultCase] {
			return fmt.Errorf("%w: missing qualification fault %s", ErrInvalidContract, faultCase)
		}
	}
	if unsupported != 0 {
		return fmt.Errorf("%w: unsupported certainty critical gate failed", ErrInvalidContract)
	}
	if scorePercent(normal, func(run QualificationRun) bool { return run.Grounded }) < 100 || scorePercent(normal, func(run QualificationRun) bool { return run.UsefulRating >= 4 }) < 90 || scorePercent(normal, func(run QualificationRun) bool { return run.DomainToolSelectionCorrect }) < 95 || scorePercent(fault, func(run QualificationRun) bool { return run.Outcome == "passed" }) < 100 {
		return fmt.Errorf("%w: qualification score gate failed", ErrInvalidContract)
	}
	if percentileLatency(normal) > 12000 || percentileLatency(briefing) > 2000 {
		return fmt.Errorf("%w: qualification p95 latency gate failed", ErrInvalidContract)
	}
	return nil
}

func scorePercent(runs []QualificationRun, predicate func(QualificationRun) bool) float64 {
	if len(runs) == 0 {
		return 0
	}
	matched := 0
	for _, run := range runs {
		if predicate(run) {
			matched++
		}
	}
	return float64(matched) * 100 / float64(len(runs))
}

func percentileLatency(runs []QualificationRun) int64 {
	latencies := make([]int64, 0, len(runs))
	for _, run := range runs {
		latencies = append(latencies, run.LatencyMS)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	if len(latencies) == 0 {
		return 0
	}
	index := int(float64(len(latencies)-1) * 0.95)
	return latencies[index]
}

func loadShadowReport(ctx context.Context, db *gorm.DB, publicID string) (models.OperatorShadowReport, ShadowQualificationPayload, error) {
	if _, err := uuid.Parse(strings.TrimSpace(publicID)); err != nil {
		return models.OperatorShadowReport{}, ShadowQualificationPayload{}, fmt.Errorf("%w: report ID is invalid", ErrInvalidContract)
	}
	var report models.OperatorShadowReport
	if err := db.WithContext(ctx).Where("public_id=?", publicID).First(&report).Error; err != nil {
		return report, ShadowQualificationPayload{}, fmt.Errorf("load shadow report: %w", err)
	}
	payload, err := parseReportPayload(report)
	return report, payload, err
}

func AddShadowReportSignoff(ctx context.Context, db *gorm.DB, reportPublicID, role, actorID string) (models.OperatorShadowReportSignoff, error) {
	if !containsString(QualificationSignoffRoles, role) || strings.TrimSpace(actorID) == "" {
		return models.OperatorShadowReportSignoff{}, fmt.Errorf("%w: report signoff is invalid", ErrInvalidContract)
	}
	var created models.OperatorShadowReportSignoff
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		report, _, err := loadShadowReport(ctx, tx, reportPublicID)
		if err != nil {
			return err
		}
		if report.State != "draft" {
			return fmt.Errorf("%w: only a draft report can receive signoffs", ErrInvalidContract)
		}
		created = models.OperatorShadowReportSignoff{ReportID: report.ID, Role: role, ActorID: actorID, ReportDigest: report.ReportDigest, SignedAt: time.Now().UTC()}
		return tx.Create(&created).Error
	})
	if err != nil {
		return models.OperatorShadowReportSignoff{}, fmt.Errorf("persist report signoff: %w", err)
	}
	return created, nil
}

func SealShadowQualificationReport(ctx context.Context, db *gorm.DB, reportPublicID string, signingKey []byte) (models.OperatorShadowReport, error) {
	var sealed models.OperatorShadowReport
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		report, payload, err := loadShadowReport(ctx, tx, reportPublicID)
		if err != nil {
			return err
		}
		if report.State != "draft" {
			return fmt.Errorf("%w: report is not a draft", ErrInvalidContract)
		}
		if err := requiredCoverage(payload.Runs); err != nil {
			return err
		}
		var signoffs []models.OperatorShadowReportSignoff
		if err := tx.Where("report_id=?", report.ID).Order("role ASC").Find(&signoffs).Error; err != nil {
			return err
		}
		if len(signoffs) != len(QualificationSignoffRoles) {
			return fmt.Errorf("%w: required report signoffs are incomplete", ErrInvalidContract)
		}
		for _, role := range QualificationSignoffRoles {
			found := false
			for _, signoff := range signoffs {
				if signoff.Role == role && signoff.ReportDigest == report.ReportDigest && signoff.ActorID != "" {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("%w: missing valid %s report signoff", ErrInvalidContract, role)
			}
		}
		seal, err := shadowReportSeal(signingKey, report.ReportDigest)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		if err := tx.Model(&models.OperatorShadowReport{}).Where("id=? AND state=?", report.ID, "draft").Updates(map[string]any{"state": "sealed", "seal": seal, "sealed_at": now}).Error; err != nil {
			return err
		}
		report.State, report.Seal, report.SealedAt = "sealed", seal, &now
		sealed = report
		return nil
	})
	if err != nil {
		return models.OperatorShadowReport{}, fmt.Errorf("seal CMS shadow report: %w", err)
	}
	return sealed, nil
}

func ExportSealedShadowQualificationReport(ctx context.Context, db *gorm.DB, reportPublicID string, signingKey []byte) (ShadowQualificationEnvelope, error) {
	report, payload, err := loadShadowReport(ctx, db, reportPublicID)
	if err != nil {
		return ShadowQualificationEnvelope{}, err
	}
	if report.State != "sealed" || report.SealedAt == nil {
		return ShadowQualificationEnvelope{}, fmt.Errorf("%w: report is not sealed", ErrInvalidContract)
	}
	if err := requiredCoverage(payload.Runs); err != nil {
		return ShadowQualificationEnvelope{}, err
	}
	expectedSeal, err := shadowReportSeal(signingKey, report.ReportDigest)
	if err != nil || !hmac.Equal([]byte(expectedSeal), []byte(report.Seal)) {
		return ShadowQualificationEnvelope{}, fmt.Errorf("%w: report seal verification failed", ErrInvalidContract)
	}
	var signoffs []models.OperatorShadowReportSignoff
	if err := db.WithContext(ctx).Where("report_id=?", report.ID).Order("role ASC").Find(&signoffs).Error; err != nil {
		return ShadowQualificationEnvelope{}, fmt.Errorf("load report signoffs: %w", err)
	}
	return ShadowQualificationEnvelope{SchemaVersion: "wahb-operator/shadow-report-envelope/v2", Source: "cms_shadow_ledger", ReportID: report.PublicID.String(), Digest: report.ReportDigest, Seal: report.Seal, Signoffs: signoffs, Report: payload}, nil
}
