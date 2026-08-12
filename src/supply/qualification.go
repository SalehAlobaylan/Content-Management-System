package supply

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
	"gorm.io/gorm/clause"
)

const (
	MediaSupplyQualificationRubricVersion = "media-supply-qualification/v1"
	mediaSupplyQualificationSealNamespace = "wahb-media-supply-qualification/v1:"
	qualificationMinimumTerminalCases     = 30
	qualificationMinimumHumanDecisions    = 10
)

const (
	qualificationPrincipalCMSObserve   = "cms:supply-observe"
	qualificationPrincipalVerifier     = "cms:supply-owner-verifier"
	qualificationPrincipalFaultHarness = "cms:isolated-fault-harness"
)

var MediaSupplyQualificationFaults = []string{
	"dependency_outage", "authorization_loss", "cancel_before_effect", "cancel_after_effect",
	"stale_target", "control_disabled", "worker_crash_pre_effect", "worker_crash_post_effect",
	"duplicate_delivery", "fence_rejection", "rollback_conflict", "manual_only_denial",
}

var mediaSupplyQualificationSignoffRoles = []string{"product", "engineering", "operations", "security"}

type QualificationOrigin string

const (
	QualificationOriginCMSObserve           QualificationOrigin = "cms_observe"
	QualificationOriginOwnerVerifier        QualificationOrigin = "owner_verifier"
	QualificationOriginIsolatedFaultHarness QualificationOrigin = "isolated_fault_harness"
)

type RecordQualificationCaseInput struct {
	CaseKey, TenantID, ActionKey, ActionVersion, AdapterVersion, VerifierVersion string
	SchemaVersion, PolicyVersion, Cohort, FaultCase, OriginPrincipal             string
	Recommendation, EffectVerdict, CorrelationDigest                             string
	Origin                                                                       QualificationOrigin
	VerifiedSuccess, IndependentVerifier, ReversalOrConflict                     bool
	Violations                                                                   []string
}

type qualificationReportCase struct {
	ID                  string   `json:"id"`
	CaseKey             string   `json:"case_key"`
	TenantID            string   `json:"tenant_id"`
	Cohort              string   `json:"cohort"`
	FaultCase           string   `json:"fault_case,omitempty"`
	Origin              string   `json:"origin"`
	Recommendation      string   `json:"recommendation"`
	VerifiedSuccess     bool     `json:"verified_success"`
	IndependentVerifier bool     `json:"independent_verifier"`
	EffectVerdict       string   `json:"effect_verdict"`
	ReversalOrConflict  bool     `json:"reversal_or_conflict"`
	Violations          []string `json:"violations"`
	PayloadDigest       string   `json:"payload_digest"`
	HumanDecision       string   `json:"human_decision,omitempty"`
}

type QualificationReportPayload struct {
	RubricVersion       string                    `json:"rubric_version"`
	TenantID            string                    `json:"tenant_id"`
	ActionKey           string                    `json:"action_key"`
	ActionVersion       string                    `json:"action_version"`
	AdapterVersion      string                    `json:"adapter_version"`
	VerifierVersion     string                    `json:"verifier_version"`
	SchemaVersion       string                    `json:"schema_version"`
	PolicyVersion       string                    `json:"policy_version"`
	EnvironmentIdentity string                    `json:"environment_identity"`
	BuildIdentity       string                    `json:"build_identity"`
	Cases               []qualificationReportCase `json:"cases"`
}

type QualificationVersionSet struct {
	ActionVersion, AdapterVersion, VerifierVersion, SchemaVersion, PolicyVersion string
}

// QualificationVersions is code-owned so neither a browser nor a report
// author can select an easier adapter/verifier/policy cohort.
func QualificationVersions(actionKey string) (QualificationVersionSet, bool) {
	if _, ok := SupplyAction(actionKey); !ok {
		return QualificationVersionSet{}, false
	}
	versions := QualificationVersionSet{ActionVersion: "v1", AdapterVersion: "cms-supply-action/v1", VerifierVersion: "cms-supply-verifier/v1", SchemaVersion: "media-supply-actions/v1", PolicyVersion: "media-supply-policy/v1"}
	switch {
	case strings.HasPrefix(actionKey, "source_run."):
		versions.AdapterVersion, versions.VerifierVersion, versions.SchemaVersion = "source-run-adapter/v1", "source-run-verifier/v1", ContractVersion
	case actionKey == SupplyActionPipelineResumeExactStage:
		versions.AdapterVersion, versions.VerifierVersion, versions.SchemaVersion = "pipeline-repair/v1", "pipeline-stage-proof/v1", "pipeline-repair/v1"
	case strings.HasPrefix(actionKey, "artifact."):
		versions.AdapterVersion, versions.VerifierVersion, versions.SchemaVersion = "artifact-coverage/v1", "artifact-provenance/v1", "artifact-coverage/v1"
	case actionKey == SupplyActionAtomizationExecuteExactParent:
		versions.AdapterVersion, versions.VerifierVersion, versions.SchemaVersion = "atomization-work/v1", "atomization-child-set/v1", "atomization-work/v1"
	case actionKey == SupplyActionFeedGenerationAttachVerifiedMember:
		versions.AdapterVersion, versions.VerifierVersion, versions.SchemaVersion = "feed-membership-repair/v1", "active-generation-membership/v1", "feed-membership-repair/v1"
	case actionKey == SupplyActionStudioClearExactChildren:
		versions.AdapterVersion, versions.VerifierVersion, versions.SchemaVersion = "studio-clearance/v1", "studio-clearance-proof/v1", "studio-clearance/v1"
	}
	return versions, true
}

func qualificationRuntimeIdentity() (string, string) {
	environment := strings.ToLower(strings.TrimSpace(os.Getenv("ENV")))
	if environment == "" {
		environment = "development"
	}
	build := strings.TrimSpace(os.Getenv("RELEASE_SHA"))
	if build == "" {
		build = strings.TrimSpace(os.Getenv("BUILD_ID"))
	}
	if build == "" {
		build = "local-unreleased"
	}
	return environment, build
}

func RecordTrustedQualificationCase(ctx context.Context, db *gorm.DB, input RecordQualificationCaseInput) (models.MediaSupplyQualificationCase, error) {
	if db == nil || strings.TrimSpace(input.CaseKey) == "" || len(input.CaseKey) > 200 || strings.TrimSpace(input.TenantID) == "" || strings.TrimSpace(input.OriginPrincipal) == "" || !isDigest(input.CorrelationDigest) {
		return models.MediaSupplyQualificationCase{}, fmt.Errorf("qualification case identity is invalid")
	}
	versions, ok := QualificationVersions(input.ActionKey)
	if !ok || input.ActionVersion != versions.ActionVersion || input.AdapterVersion != versions.AdapterVersion || input.VerifierVersion != versions.VerifierVersion || input.SchemaVersion != versions.SchemaVersion || input.PolicyVersion != versions.PolicyVersion {
		return models.MediaSupplyQualificationCase{}, fmt.Errorf("qualification action/version is not registered")
	}
	if input.Cohort != "terminal" && input.Cohort != "human_decision" && input.Cohort != "fault" && input.Cohort != "tri_state" {
		return models.MediaSupplyQualificationCase{}, fmt.Errorf("qualification cohort is invalid")
	}
	if (input.Cohort == "fault") != (input.FaultCase != "") || (input.FaultCase != "" && !qualificationContains(MediaSupplyQualificationFaults, input.FaultCase)) {
		return models.MediaSupplyQualificationCase{}, fmt.Errorf("qualification fault is invalid")
	}
	if !isTrustedQualificationOrigin(input.Origin, input.OriginPrincipal) {
		return models.MediaSupplyQualificationCase{}, fmt.Errorf("qualification origin is not trusted")
	}
	if input.Recommendation != "would_request" && input.Recommendation != "would_skip" {
		return models.MediaSupplyQualificationCase{}, fmt.Errorf("qualification recommendation is invalid")
	}
	if input.EffectVerdict != string(VerdictPresent) && input.EffectVerdict != string(VerdictAbsent) && input.EffectVerdict != string(VerdictUnknown) {
		return models.MediaSupplyQualificationCase{}, fmt.Errorf("qualification effect verdict is invalid")
	}
	environment, build := qualificationRuntimeIdentity()
	if environment == "production" && input.Origin == QualificationOriginIsolatedFaultHarness {
		return models.MediaSupplyQualificationCase{}, fmt.Errorf("production cannot ingest injected fault qualification")
	}
	sort.Strings(input.Violations)
	payload := map[string]any{"case_key": input.CaseKey, "tenant_id": input.TenantID, "action_key": input.ActionKey, "action_version": input.ActionVersion, "adapter_version": input.AdapterVersion, "verifier_version": input.VerifierVersion, "schema_version": input.SchemaVersion, "policy_version": input.PolicyVersion, "environment": environment, "build": build, "cohort": input.Cohort, "fault": input.FaultCase, "origin": input.Origin, "origin_principal": input.OriginPrincipal, "recommendation": input.Recommendation, "verified_success": input.VerifiedSuccess, "independent_verifier": input.IndependentVerifier, "effect_verdict": input.EffectVerdict, "reversal_or_conflict": input.ReversalOrConflict, "violations": input.Violations, "correlation_digest": strings.ToLower(input.CorrelationDigest)}
	payloadBytes, _ := json.Marshal(payload)
	payloadHash := sha256.Sum256(payloadBytes)
	violations, _ := json.Marshal(input.Violations)
	fault := (*string)(nil)
	if input.FaultCase != "" {
		fault = &input.FaultCase
	}
	record := models.MediaSupplyQualificationCase{PublicID: uuid.New(), CaseKey: input.CaseKey, TenantID: input.TenantID, ActionKey: input.ActionKey, ActionVersion: input.ActionVersion, AdapterVersion: input.AdapterVersion, VerifierVersion: input.VerifierVersion, RubricVersion: MediaSupplyQualificationRubricVersion, SchemaVersion: input.SchemaVersion, PolicyVersion: input.PolicyVersion, EnvironmentIdentity: environment, BuildIdentity: build, Cohort: input.Cohort, FaultCase: fault, Origin: string(input.Origin), OriginPrincipal: input.OriginPrincipal, Recommendation: input.Recommendation, VerifiedSuccess: input.VerifiedSuccess, IndependentVerifier: input.IndependentVerifier, EffectVerdict: input.EffectVerdict, ReversalOrConflict: input.ReversalOrConflict, Violations: datatypes.JSON(violations), CorrelationDigest: strings.ToLower(input.CorrelationDigest), PayloadDigest: hex.EncodeToString(payloadHash[:])}
	result := db.WithContext(ctx).Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "case_key"}}, DoNothing: true}).Create(&record)
	if result.Error != nil {
		return models.MediaSupplyQualificationCase{}, result.Error
	}
	if result.RowsAffected == 0 {
		var existing models.MediaSupplyQualificationCase
		if err := db.WithContext(ctx).Where("case_key=?", input.CaseKey).First(&existing).Error; err != nil {
			return models.MediaSupplyQualificationCase{}, err
		}
		if existing.TenantID != record.TenantID || existing.ActionKey != record.ActionKey || existing.PayloadDigest != record.PayloadDigest {
			return models.MediaSupplyQualificationCase{}, fmt.Errorf("qualification case identity conflicts with immutable evidence")
		}
		return existing, nil
	}
	return record, nil
}

func RecordSupplyObserveQualificationCase(ctx context.Context, db *gorm.DB, tenant, actionKey, caseKey, correlationDigest string, wouldRequest bool, verdict VerificationVerdict) (models.MediaSupplyQualificationCase, error) {
	versions, ok := QualificationVersions(actionKey)
	if !ok {
		return models.MediaSupplyQualificationCase{}, fmt.Errorf("qualification observe action is not registered")
	}
	recommendation := "would_skip"
	if wouldRequest {
		recommendation = "would_request"
	}
	return RecordTrustedQualificationCase(ctx, db, RecordQualificationCaseInput{
		CaseKey: caseKey, TenantID: tenant, ActionKey: actionKey,
		ActionVersion: versions.ActionVersion, AdapterVersion: versions.AdapterVersion, VerifierVersion: versions.VerifierVersion,
		SchemaVersion: versions.SchemaVersion, PolicyVersion: versions.PolicyVersion,
		Cohort: "tri_state", Origin: QualificationOriginCMSObserve, OriginPrincipal: qualificationPrincipalCMSObserve,
		Recommendation: recommendation, EffectVerdict: string(verdict), CorrelationDigest: correlationDigest,
	})
}

// RecordSupplyFaultQualificationCase is the only isolated-fixture writer. It
// cannot be used in production and cannot create a preview, action, owner
// request, or effect; it appends one registered fault outcome only.
func RecordSupplyFaultQualificationCase(ctx context.Context, db *gorm.DB, tenant, actionKey, caseKey, faultCase, correlationDigest string, safelyHandled bool, verdict VerificationVerdict, conflict bool) (models.MediaSupplyQualificationCase, error) {
	versions, ok := QualificationVersions(actionKey)
	if !ok || !qualificationContains(MediaSupplyQualificationFaults, faultCase) {
		return models.MediaSupplyQualificationCase{}, fmt.Errorf("qualification fault action or case is not registered")
	}
	return RecordTrustedQualificationCase(ctx, db, RecordQualificationCaseInput{
		CaseKey: caseKey, TenantID: tenant, ActionKey: actionKey,
		ActionVersion: versions.ActionVersion, AdapterVersion: versions.AdapterVersion, VerifierVersion: versions.VerifierVersion,
		SchemaVersion: versions.SchemaVersion, PolicyVersion: versions.PolicyVersion,
		Cohort: "fault", FaultCase: faultCase, Origin: QualificationOriginIsolatedFaultHarness, OriginPrincipal: qualificationPrincipalFaultHarness,
		Recommendation: "would_skip", EffectVerdict: string(verdict), CorrelationDigest: correlationDigest,
		VerifiedSuccess: safelyHandled, IndependentVerifier: true, ReversalOrConflict: conflict,
	})
}

func DigestForQualificationObservation(evaluationDigest, actionKey string) string {
	hash := sha256.Sum256([]byte("supply-observe/v1\n" + evaluationDigest + "\n" + actionKey))
	return hex.EncodeToString(hash[:])
}

func RecordSupplyVerifierQualificationCase(ctx context.Context, db *gorm.DB, request models.MediaSupplyActionRequest, succeeded bool, verdict VerificationVerdict, conflict bool) (models.MediaSupplyQualificationCase, error) {
	versions, ok := QualificationVersions(request.ActionKey)
	if !ok {
		return models.MediaSupplyQualificationCase{}, fmt.Errorf("qualification verifier action is not registered")
	}
	correlation := sha256.Sum256([]byte("supply-verifier/v1\n" + request.TenantID + "\n" + request.PublicID.String() + "\n" + string(verdict)))
	return RecordTrustedQualificationCase(ctx, db, RecordQualificationCaseInput{
		CaseKey: "supply-verifier:" + request.PublicID.String(), TenantID: request.TenantID, ActionKey: request.ActionKey,
		ActionVersion: versions.ActionVersion, AdapterVersion: versions.AdapterVersion, VerifierVersion: versions.VerifierVersion,
		SchemaVersion: versions.SchemaVersion, PolicyVersion: versions.PolicyVersion,
		Cohort: "terminal", Origin: QualificationOriginOwnerVerifier, OriginPrincipal: qualificationPrincipalVerifier,
		Recommendation: "would_request", EffectVerdict: string(verdict), CorrelationDigest: hex.EncodeToString(correlation[:]),
		VerifiedSuccess: succeeded, IndependentVerifier: true, ReversalOrConflict: conflict,
	})
}

// RecordSupplyVerifierQualificationCaseBestEffort isolates qualification from
// the operational action ledger with a savepoint. A qualification writer or
// schema fault must never roll back an independently verified manual recovery.
// Safe Auto safety remains fail-closed through promotion checks and demotion.
func RecordSupplyVerifierQualificationCaseBestEffort(tx *gorm.DB, request models.MediaSupplyActionRequest, succeeded bool, verdict VerificationVerdict, conflict bool) {
	if tx == nil {
		return
	}
	_ = tx.Transaction(func(qualificationTx *gorm.DB) error {
		_, err := RecordSupplyVerifierQualificationCase(context.Background(), qualificationTx, request, succeeded, verdict, conflict)
		return err
	})
}

func DemoteBoundSupplyPromotion(tx *gorm.DB, request models.MediaSupplyActionRequest, reason string) error {
	if request.ExecutionMode != SupplyExecutionSafeAuto || request.PromotionID == nil {
		return nil
	}
	var promotion models.MediaSupplyActionPromotion
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=? AND state=?", request.TenantID, *request.PromotionID, "active").First(&promotion).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil
		}
		return err
	}
	now := time.Now().UTC()
	if err := tx.Model(&promotion).Updates(map[string]any{"state": "demoted", "demoted_at": now, "demotion_reason": reason}).Error; err != nil {
		return err
	}
	return appendPromotionEvent(tx, promotion, "trust_reset", map[string]any{"actor_id": "cms:live-safety-guard", "reason": reason})
}

// isTrustedQualificationOrigin keeps authority purpose-scoped. Ingestion is
// intentionally not an HTTP/admin capability: callers must be one of the
// three checked-in writers and CMS derives all report identity fields from the
// registered action plus its own environment/build identity.
func isTrustedQualificationOrigin(origin QualificationOrigin, principal string) bool {
	switch origin {
	case QualificationOriginCMSObserve:
		return principal == qualificationPrincipalCMSObserve
	case QualificationOriginOwnerVerifier:
		return principal == qualificationPrincipalVerifier
	case QualificationOriginIsolatedFaultHarness:
		return principal == qualificationPrincipalFaultHarness
	default:
		return false
	}
}

func RecordQualificationHumanDecision(ctx context.Context, db *gorm.DB, tenant, caseID, decision, actorID, accessVersion string) (models.MediaSupplyQualificationHumanDecision, error) {
	if decision != "agreed" && decision != "disagreed" && decision != "not_required" {
		return models.MediaSupplyQualificationHumanDecision{}, fmt.Errorf("qualification human decision is invalid")
	}
	publicID, err := uuid.Parse(caseID)
	if err != nil || tenant == "" || actorID == "" || accessVersion == "" {
		return models.MediaSupplyQualificationHumanDecision{}, fmt.Errorf("qualification human identity is invalid")
	}
	var created models.MediaSupplyQualificationHumanDecision
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var qualificationCase models.MediaSupplyQualificationCase
		if err := tx.Where("public_id=? AND tenant_id=?", publicID, tenant).First(&qualificationCase).Error; err != nil {
			return err
		}
		if qualificationCase.Cohort != "human_decision" {
			return fmt.Errorf("qualification case does not require a human decision")
		}
		created = models.MediaSupplyQualificationHumanDecision{PublicID: uuid.New(), CaseID: qualificationCase.ID, Decision: decision, ActorID: actorID, AccessVersion: accessVersion, DecidedAt: time.Now().UTC()}
		return tx.Create(&created).Error
	})
	return created, err
}

func CreateQualificationReport(ctx context.Context, db *gorm.DB, tenant, actionKey, actionVersion, adapterVersion, verifierVersion, schemaVersion, policyVersion string) (models.MediaSupplyQualificationReport, error) {
	if strings.TrimSpace(tenant) == "" || actionVersion != "v1" || adapterVersion == "" || verifierVersion == "" || schemaVersion == "" || policyVersion == "" {
		return models.MediaSupplyQualificationReport{}, fmt.Errorf("qualification report version set is invalid")
	}
	registered, ok := QualificationVersions(actionKey)
	if !ok || registered.ActionVersion != actionVersion || registered.AdapterVersion != adapterVersion || registered.VerifierVersion != verifierVersion || registered.SchemaVersion != schemaVersion || registered.PolicyVersion != policyVersion {
		return models.MediaSupplyQualificationReport{}, fmt.Errorf("qualification report version set is not registered")
	}
	environment, build := qualificationRuntimeIdentity()
	var cases []models.MediaSupplyQualificationCase
	if err := db.WithContext(ctx).Where("tenant_id=? AND action_key=? AND action_version=? AND adapter_version=? AND verifier_version=? AND rubric_version=? AND schema_version=? AND policy_version=? AND environment_identity=? AND build_identity=?", tenant, actionKey, actionVersion, adapterVersion, verifierVersion, MediaSupplyQualificationRubricVersion, schemaVersion, policyVersion, environment, build).Order("case_key").Find(&cases).Error; err != nil {
		return models.MediaSupplyQualificationReport{}, err
	}
	payloadCases := make([]qualificationReportCase, 0, len(cases))
	for _, item := range cases {
		var violations []string
		_ = json.Unmarshal(item.Violations, &violations)
		row := qualificationReportCase{ID: item.PublicID.String(), CaseKey: item.CaseKey, TenantID: item.TenantID, Cohort: item.Cohort, Origin: item.Origin, Recommendation: item.Recommendation, VerifiedSuccess: item.VerifiedSuccess, IndependentVerifier: item.IndependentVerifier, EffectVerdict: item.EffectVerdict, ReversalOrConflict: item.ReversalOrConflict, Violations: violations, PayloadDigest: item.PayloadDigest}
		if item.FaultCase != nil {
			row.FaultCase = *item.FaultCase
		}
		var human models.MediaSupplyQualificationHumanDecision
		if err := db.WithContext(ctx).Where("case_id=?", item.ID).First(&human).Error; err == nil {
			row.HumanDecision = human.Decision
		} else if err != gorm.ErrRecordNotFound {
			return models.MediaSupplyQualificationReport{}, err
		}
		payloadCases = append(payloadCases, row)
	}
	payload := QualificationReportPayload{RubricVersion: MediaSupplyQualificationRubricVersion, TenantID: tenant, ActionKey: actionKey, ActionVersion: actionVersion, AdapterVersion: adapterVersion, VerifierVersion: verifierVersion, SchemaVersion: schemaVersion, PolicyVersion: policyVersion, EnvironmentIdentity: environment, BuildIdentity: build, Cases: payloadCases}
	serialized, _ := json.Marshal(payload)
	digest := sha256.Sum256(serialized)
	report := models.MediaSupplyQualificationReport{PublicID: uuid.New(), TenantID: tenant, RubricVersion: MediaSupplyQualificationRubricVersion, ActionKey: actionKey, ActionVersion: actionVersion, AdapterVersion: adapterVersion, VerifierVersion: verifierVersion, SchemaVersion: schemaVersion, PolicyVersion: policyVersion, EnvironmentIdentity: environment, BuildIdentity: build, State: "draft", Payload: datatypes.JSON(serialized), ReportDigest: hex.EncodeToString(digest[:])}
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&report).Error; err != nil {
			return err
		}
		for _, item := range cases {
			if err := tx.Create(&models.MediaSupplyQualificationReportCase{ReportID: report.ID, CaseID: item.ID}).Error; err != nil {
				return err
			}
		}
		return nil
	})
	return report, err
}

func validateQualificationReportPayload(payload QualificationReportPayload) error {
	if payload.RubricVersion != MediaSupplyQualificationRubricVersion || payload.TenantID == "" || payload.ActionVersion != "v1" || payload.ActionKey == "" || payload.AdapterVersion == "" || payload.VerifierVersion == "" || payload.SchemaVersion == "" || payload.PolicyVersion == "" || payload.EnvironmentIdentity == "" || payload.BuildIdentity == "" {
		return fmt.Errorf("qualification report identity is invalid")
	}
	ids, faults, verdicts := map[string]bool{}, map[string]bool{}, map[string]bool{}
	terminal, verified, conflicts, human, agreement := 0, 0, 0, 0, 0
	for _, item := range payload.Cases {
		if item.TenantID != payload.TenantID {
			return fmt.Errorf("qualification report contains a cross-tenant case")
		}
		if item.ID == "" || item.CaseKey == "" || ids[item.ID] || len(item.PayloadDigest) != 64 || len(item.Violations) != 0 || ((item.Cohort == "terminal" || item.Cohort == "fault") && !item.IndependentVerifier) {
			return fmt.Errorf("qualification case provenance or violation gate failed")
		}
		ids[item.ID] = true
		verdicts[item.EffectVerdict] = true
		if item.Cohort == "terminal" || item.Cohort == "human_decision" {
			terminal++
			if item.VerifiedSuccess {
				verified++
			}
		}
		if item.Cohort == "fault" {
			if item.FaultCase == "" || !item.VerifiedSuccess {
				return fmt.Errorf("qualification fault case failed")
			}
			faults[item.FaultCase] = true
		}
		if item.ReversalOrConflict {
			conflicts++
		}
		if item.Cohort == "human_decision" {
			human++
			if item.HumanDecision == "agreed" {
				agreement++
			} else if item.HumanDecision == "" {
				return fmt.Errorf("qualification human decision is missing")
			}
		}
	}
	if terminal < qualificationMinimumTerminalCases || human < qualificationMinimumHumanDecisions || verified*100 < terminal*98 || agreement*100 < human*95 || conflicts*100 > len(payload.Cases)*2 {
		return fmt.Errorf("qualification denominator or score gate failed")
	}
	for _, fault := range MediaSupplyQualificationFaults {
		if !faults[fault] {
			return fmt.Errorf("qualification fault coverage is incomplete: %s", fault)
		}
	}
	for _, verdict := range []string{string(VerdictPresent), string(VerdictAbsent), string(VerdictUnknown)} {
		if !verdicts[verdict] {
			return fmt.Errorf("qualification tri-state coverage is incomplete")
		}
	}
	return nil
}

func AddQualificationSignoff(ctx context.Context, db *gorm.DB, tenant, reportID, role, actorID, accessVersion string) (models.MediaSupplyQualificationSignoff, error) {
	if !qualificationContains(mediaSupplyQualificationSignoffRoles, role) || actorID == "" || accessVersion == "" {
		return models.MediaSupplyQualificationSignoff{}, fmt.Errorf("qualification signoff is invalid")
	}
	var report models.MediaSupplyQualificationReport
	if err := db.WithContext(ctx).Where("public_id=? AND tenant_id=? AND state=?", reportID, tenant, "draft").First(&report).Error; err != nil {
		return models.MediaSupplyQualificationSignoff{}, err
	}
	signoff := models.MediaSupplyQualificationSignoff{PublicID: uuid.New(), ReportID: report.ID, Role: role, ActorID: actorID, AccessVersion: accessVersion, ReportDigest: report.ReportDigest, SignedAt: time.Now().UTC()}
	return signoff, db.WithContext(ctx).Create(&signoff).Error
}

func SealQualificationReport(ctx context.Context, db *gorm.DB, tenant, reportID string, signingKey []byte) (models.MediaSupplyQualificationReport, error) {
	var sealed models.MediaSupplyQualificationReport
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var report models.MediaSupplyQualificationReport
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=? AND tenant_id=? AND state=?", reportID, tenant, "draft").First(&report).Error; err != nil {
			return err
		}
		var payload QualificationReportPayload
		if err := json.Unmarshal(report.Payload, &payload); err != nil {
			return err
		}
		if payload.TenantID != report.TenantID {
			return fmt.Errorf("qualification report tenant binding changed")
		}
		serialized, _ := json.Marshal(payload)
		digest := sha256.Sum256(serialized)
		if !hmac.Equal([]byte(hex.EncodeToString(digest[:])), []byte(report.ReportDigest)) {
			return fmt.Errorf("qualification report digest changed")
		}
		if err := validateQualificationReportPayload(payload); err != nil {
			return err
		}
		var signoffs []models.MediaSupplyQualificationSignoff
		if err := tx.Where("report_id=?", report.ID).Find(&signoffs).Error; err != nil {
			return err
		}
		for _, role := range mediaSupplyQualificationSignoffRoles {
			valid := false
			for _, s := range signoffs {
				if s.Role == role && s.ActorID != "" && s.AccessVersion != "" && s.ReportDigest == report.ReportDigest {
					valid = true
				}
			}
			if !valid {
				return fmt.Errorf("missing qualification signoff: %s", role)
			}
		}
		seal, err := qualificationReportSeal(signingKey, report.ReportDigest)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		if result := tx.Model(&report).Where("state=?", "draft").Updates(map[string]any{"state": "sealed", "seal": seal, "sealed_at": now}); result.Error != nil || result.RowsAffected != 1 {
			if result.Error != nil {
				return result.Error
			}
			return fmt.Errorf("qualification report changed while sealing")
		}
		report.State, report.Seal, report.SealedAt = "sealed", seal, &now
		sealed = report
		return nil
	})
	return sealed, err
}

func PromoteQualifiedSupplyAction(ctx context.Context, db *gorm.DB, tenant, reportID, actorID, accessVersion string, signingKey []byte) (models.MediaSupplyActionPromotion, error) {
	var promoted models.MediaSupplyActionPromotion
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var report models.MediaSupplyQualificationReport
		if err := tx.Where("public_id=? AND tenant_id=? AND state=?", reportID, tenant, "sealed").First(&report).Error; err != nil {
			return err
		}
		expectedSeal, err := qualificationReportSeal(signingKey, report.ReportDigest)
		if err != nil || !hmac.Equal([]byte(expectedSeal), []byte(report.Seal)) {
			return fmt.Errorf("qualification report seal is invalid")
		}
		var payload QualificationReportPayload
		if err := json.Unmarshal(report.Payload, &payload); err != nil {
			return err
		}
		if err := validateQualificationReportPayload(payload); err != nil {
			return err
		}
		allowed, _, err := MayExecuteSupplyAction(tx, tenant, report.ActionKey)
		if err != nil || !allowed {
			return fmt.Errorf("supply action is disabled")
		}
		var active int64
		if err := tx.Model(&models.MediaSupplyActionPromotion{}).Where("tenant_id=? AND action_key=? AND state=?", tenant, report.ActionKey, "active").Count(&active).Error; err != nil {
			return err
		}
		if active != 0 {
			return fmt.Errorf("an active promotion already exists")
		}
		now := time.Now().UTC()
		promoted = models.MediaSupplyActionPromotion{PublicID: uuid.New(), TenantID: tenant, ActionKey: report.ActionKey, ActionVersion: report.ActionVersion, AdapterVersion: report.AdapterVersion, VerifierVersion: report.VerifierVersion, SchemaVersion: report.SchemaVersion, PolicyVersion: report.PolicyVersion, EnvironmentIdentity: report.EnvironmentIdentity, BuildIdentity: report.BuildIdentity, ReportID: report.ID, ReportDigest: report.ReportDigest, State: "active", PromotionEpoch: 1, PromotedBy: actorID, PromotedAccessVersion: accessVersion, PromotedAt: now}
		if err := tx.Create(&promoted).Error; err != nil {
			return err
		}
		return appendPromotionEvent(tx, promoted, "promoted", map[string]any{"report_digest": report.ReportDigest})
	})
	return promoted, err
}

// RecheckSupplyActionExecutionAuthority separates approved human recovery from
// autonomous authority. A client cannot promote an approval-required request
// by changing its mode; only CMS-created safe_auto rows may carry a promotion.
func RecheckSupplyActionExecutionAuthority(db *gorm.DB, request models.MediaSupplyActionRequest) error {
	mode := strings.TrimSpace(request.ExecutionMode)
	if mode == "" || mode == SupplyExecutionApprovalRequired {
		if request.PromotionID != nil || request.QualificationReportID != nil {
			return fmt.Errorf("approval-required action carries autonomous authority")
		}
		return nil
	}
	if mode != SupplyExecutionSafeAuto || request.PromotionID == nil || request.QualificationReportID == nil {
		return fmt.Errorf("supply action execution mode is invalid")
	}
	versions, ok := QualificationVersions(request.ActionKey)
	if !ok {
		return fmt.Errorf("supply action qualification versions are unavailable")
	}
	promotion, err := RequireActiveSupplyPromotion(db, request.TenantID, request.ActionKey, versions.ActionVersion, versions.AdapterVersion, versions.VerifierVersion, versions.SchemaVersion, versions.PolicyVersion)
	if err != nil {
		return err
	}
	if promotion.PublicID != *request.PromotionID || promotion.ReportID != *request.QualificationReportID {
		return fmt.Errorf("supply action promotion binding changed")
	}
	return nil
}

func RequireActiveSupplyPromotion(db *gorm.DB, tenant, actionKey, actionVersion, adapterVersion, verifierVersion, schemaVersion, policyVersion string) (models.MediaSupplyActionPromotion, error) {
	allowed, disabledControl, err := MayExecuteSupplyAction(db, tenant, actionKey)
	if err != nil || !allowed {
		if err == nil && !allowed {
			_ = DemoteSupplyActionPromotion(context.Background(), db, tenant, actionKey, "cms:qualification-control-guard", "control_disabled:"+disabledControl, true)
		}
		return models.MediaSupplyActionPromotion{}, fmt.Errorf("supply action is disabled")
	}
	var promotion models.MediaSupplyActionPromotion
	if err := db.Where("tenant_id=? AND action_key=? AND state=?", tenant, actionKey, "active").First(&promotion).Error; err != nil {
		return promotion, fmt.Errorf("matching active qualification is unavailable: %w", err)
	}
	if promotion.ActionVersion != actionVersion || promotion.AdapterVersion != adapterVersion || promotion.VerifierVersion != verifierVersion || promotion.SchemaVersion != schemaVersion || promotion.PolicyVersion != policyVersion {
		// Version drift is a trust reset, not merely a negative lookup. Persisting
		// the demotion prevents the stale row from blocking a newly qualified
		// version and leaves an immutable reason in the promotion ledger.
		_ = DemoteSupplyActionPromotion(context.Background(), db, tenant, actionKey, "cms:qualification-version-guard", "adapter_verifier_schema_or_policy_version_changed", true)
		return models.MediaSupplyActionPromotion{}, fmt.Errorf("active qualification versions no longer match")
	}
	environment, build := qualificationRuntimeIdentity()
	if promotion.EnvironmentIdentity != environment || promotion.BuildIdentity != build {
		_ = DemoteSupplyActionPromotion(context.Background(), db, tenant, actionKey, "cms:qualification-runtime-guard", "environment_or_build_identity_changed", true)
		return models.MediaSupplyActionPromotion{}, fmt.Errorf("active qualification runtime identity no longer matches")
	}
	// Live safety is bounded and deliberately coarse: inspect only the latest
	// 50 autonomous terminals for this exact tenant/action. IDs never become
	// metric labels, and an excessive unknown/conflict/safety-failure rate is a
	// trust reset before another external effect can begin.
	var recent []models.MediaSupplyActionRequest
	if err := db.Select("state", "failure_class").Where("tenant_id=? AND action_key=? AND execution_mode=? AND state IN ?", tenant, actionKey, SupplyExecutionSafeAuto, []string{models.MediaSupplyActionRequestSucceeded, models.MediaSupplyActionRequestFailed, models.MediaSupplyActionRequestCancelled}).Order("finished_at DESC").Limit(50).Find(&recent).Error; err != nil {
		return models.MediaSupplyActionPromotion{}, fmt.Errorf("live safety evidence is unavailable: %w", err)
	}
	bad := 0
	for _, action := range recent {
		failure := strings.ToLower(action.FailureClass)
		if strings.Contains(failure, "unknown") || strings.Contains(failure, "conflict") || strings.Contains(failure, "verifier") || strings.Contains(failure, "authorization") || strings.Contains(failure, "budget") {
			bad++
		}
	}
	if len(recent) >= 20 && bad >= 3 && bad*10 >= len(recent) {
		_ = DemoteSupplyActionPromotion(context.Background(), db, tenant, actionKey, "cms:qualification-live-safety", "excessive_unknown_conflict_or_safety_failure_rate", true)
		return models.MediaSupplyActionPromotion{}, fmt.Errorf("active qualification failed live safety thresholds")
	}
	return promotion, nil
}

func DemoteSupplyActionPromotion(ctx context.Context, db *gorm.DB, tenant, actionKey, actorID, reason string, trustReset bool) error {
	if tenant == "" || actionKey == "" || actorID == "" || reason == "" {
		return fmt.Errorf("promotion demotion identity is invalid")
	}
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var promotion models.MediaSupplyActionPromotion
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND action_key=? AND state=?", tenant, actionKey, "active").First(&promotion).Error; err != nil {
			return err
		}
		now := time.Now().UTC()
		if err := tx.Model(&promotion).Updates(map[string]any{"state": "demoted", "demoted_at": now, "demotion_reason": reason}).Error; err != nil {
			return err
		}
		event := "demoted"
		if trustReset {
			event = "trust_reset"
		}
		return appendPromotionEvent(tx, promotion, event, map[string]any{"actor_id": actorID, "reason": reason})
	})
}

func QualificationSigningKeyFromEnv() ([]byte, error) {
	key := strings.TrimSpace(os.Getenv("OPERATOR_PLAN_SIGNING_KEY"))
	if key != "" {
		if len(key) < 32 {
			return nil, fmt.Errorf("Operator signing key is too short")
		}
		return []byte(key), nil
	}
	token := strings.TrimSpace(os.Getenv("CMS_SERVICE_TOKEN"))
	if token == "" {
		return nil, fmt.Errorf("CMS signing identity is unavailable")
	}
	mac := hmac.New(sha256.New, []byte(token))
	_, _ = mac.Write([]byte("wahb-operator/plan-signing/v1"))
	return mac.Sum(nil), nil
}

func qualificationReportSeal(key []byte, digest string) (string, error) {
	if len(key) < 32 || !isDigest(digest) {
		return "", fmt.Errorf("qualification seal material is invalid")
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(mediaSupplyQualificationSealNamespace + digest))
	return hex.EncodeToString(mac.Sum(nil)), nil
}
func qualificationContains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
func appendPromotionEvent(tx *gorm.DB, promotion models.MediaSupplyActionPromotion, kind string, payload any) error {
	bytes, _ := json.Marshal(payload)
	return tx.Create(&models.MediaSupplyPromotionEvent{PublicID: uuid.New(), TenantID: promotion.TenantID, ActionKey: promotion.ActionKey, PromotionID: promotion.PublicID, EventType: kind, Payload: datatypes.JSON(bytes), OccurredAt: time.Now().UTC()}).Error
}
