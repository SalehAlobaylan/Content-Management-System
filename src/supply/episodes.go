package supply

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// EpisodeFingerprint identifies the same tenant-scoped observed boundary over
// time. It intentionally omits observation timestamps and counts so renewed
// evidence updates one episode instead of creating alert noise.
func EpisodeFingerprint(tenantID string, evaluation SupplyEvaluation) (string, error) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return "", fmt.Errorf("media supply episode requires an explicit tenant")
	}
	payload := struct {
		SchemaVersion    string        `json:"schema_version"`
		TenantID         string        `json:"tenant_id"`
		Verdict          SupplyVerdict `json:"verdict"`
		HeadlineBoundary string        `json:"headline_boundary"`
		Owner            string        `json:"owner"`
		SourceIDs        []string      `json:"source_ids"`
		RequestIDs       []string      `json:"request_ids"`
	}{
		SchemaVersion: SupplyEvaluationSchemaVersion, TenantID: tenantID,
		Verdict: evaluation.Verdict, HeadlineBoundary: evaluation.HeadlineBoundary, Owner: evaluation.Owner,
		SourceIDs: uniqueSortedIDs(evaluation.AffectedSourceIDs), RequestIDs: uniqueSortedIDs(evaluation.AffectedRequestIDs),
	}
	bytes, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(bytes)
	return hex.EncodeToString(digest[:]), nil
}

// EvaluationEvidenceDigest changes whenever the persisted, non-secret
// operational conclusion changes. It is used for immutable event dedupe, not
// authorization, and excludes only the volatile evaluation timestamp.
func EvaluationEvidenceDigest(evaluation SupplyEvaluation) (string, error) {
	payload := struct {
		SchemaVersion        string                 `json:"schema_version"`
		Verdict              SupplyVerdict          `json:"verdict"`
		HeadlineBoundary     string                 `json:"headline_boundary"`
		Owner                string                 `json:"owner"`
		EvidenceCompleteness string                 `json:"evidence_completeness"`
		Summary              string                 `json:"summary"`
		Counts               SupplyEvaluationCounts `json:"counts"`
		AffectedSourceIDs    []string               `json:"affected_source_ids"`
		AffectedRequestIDs   []string               `json:"affected_request_ids"`
		Unknowns             []string               `json:"unknowns"`
	}{
		SchemaVersion: evaluation.SchemaVersion, Verdict: evaluation.Verdict,
		HeadlineBoundary: evaluation.HeadlineBoundary, Owner: evaluation.Owner,
		EvidenceCompleteness: evaluation.EvidenceCompleteness, Summary: evaluation.Summary, Counts: evaluation.Counts,
		AffectedSourceIDs: uniqueSortedIDs(evaluation.AffectedSourceIDs), AffectedRequestIDs: uniqueSortedIDs(evaluation.AffectedRequestIDs),
		Unknowns: evaluation.Unknowns,
	}
	bytes, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(bytes)
	return hex.EncodeToString(digest[:]), nil
}

// IsEpisodeWorthy keeps normal scheduled/in-flight observations in the
// read-model without turning them into incidents. Unknown evidence is kept as
// an attention episode because it must stay visible and blocks unsafe action.
func IsEpisodeWorthy(verdict SupplyVerdict) bool {
	switch verdict {
	case SupplyVerdictNoActiveMediaSources, SupplyVerdictSourceDueNotAdmitted,
		SupplyVerdictSourceRunWithoutIngest, SupplyVerdictPodsDeliveryDegraded,
		SupplyVerdictNoEligibleInventory, SupplyVerdictGenerationOmission,
		SupplyVerdictServingOmission, SupplyVerdictPodsInventoryStale,
		SupplyVerdictUpstreamDeferred, SupplyVerdictIntakeObservationBlocked, SupplyVerdictEvidenceUnavailable:
		return true
	default:
		return false
	}
}

func EpisodeSeverity(verdict SupplyVerdict) string {
	switch verdict {
	case SupplyVerdictEvidenceUnavailable:
		return "warning"
	case SupplyVerdictNoActiveMediaSources, SupplyVerdictSourceDueNotAdmitted,
		SupplyVerdictSourceRunWithoutIngest, SupplyVerdictPodsDeliveryDegraded,
		SupplyVerdictNoEligibleInventory, SupplyVerdictGenerationOmission,
		SupplyVerdictServingOmission, SupplyVerdictPodsInventoryStale,
		SupplyVerdictUpstreamDeferred, SupplyVerdictIntakeObservationBlocked:
		return "major"
	default:
		return "info"
	}
}
