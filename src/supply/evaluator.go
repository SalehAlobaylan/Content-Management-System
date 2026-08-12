package supply

import (
	"sort"
	"strings"
	"time"
)

// SupplyEvaluationSchemaVersion is the public, read-only contract for the
// Media Circulation supply headline. It deliberately contains no retry or
// action descriptor: this evaluator only names what CMS has observed.
const SupplyEvaluationSchemaVersion = "media-supply/v1"

type SupplyVerdict string

const (
	SupplyVerdictNoCurrentBreak           SupplyVerdict = "no_current_break_observed"
	SupplyVerdictNoActiveMediaSources     SupplyVerdict = "no_active_media_sources"
	SupplyVerdictObservationPending       SupplyVerdict = "observation_pending"
	SupplyVerdictSourceDueNotAdmitted     SupplyVerdict = "source_due_not_admitted"
	SupplyVerdictSourceRunWithoutIngest   SupplyVerdict = "source_run_without_ingest_proof"
	SupplyVerdictHealthyNoUpstreamChange  SupplyVerdict = "healthy_no_upstream_change"
	SupplyVerdictUpstreamDeferred         SupplyVerdict = "upstream_change_deferred"
	SupplyVerdictIntakeObservationBlocked SupplyVerdict = "observation_blocked_by_intake"
	SupplyVerdictPodsDeliveryDegraded     SupplyVerdict = "pods_delivery_evidence_degraded"
	SupplyVerdictNoEligibleInventory      SupplyVerdict = "no_base_eligible_inventory"
	SupplyVerdictGenerationOmission       SupplyVerdict = "eligible_not_generation_reachable"
	SupplyVerdictServingOmission          SupplyVerdict = "eligible_not_returned"
	SupplyVerdictPodsInventoryStale       SupplyVerdict = "pods_inventory_stale"
	SupplyVerdictEvidenceUnavailable      SupplyVerdict = "evidence_unavailable"
)

// SupplyScheduleObservation and SupplyDeliveryObservation are intentionally
// small projections of CMS-owned read models. Queue state, provider details,
// and browser-supplied evidence cannot enter the evaluator.
type SupplyScheduleObservation struct {
	SourceID string
	State    string
}

type SupplyDeliveryObservation struct {
	RequestID       string
	SourceID        string
	RequestState    string
	State           string
	TerminalOutcome string
}

type SupplyEvaluationInput struct {
	TenantID          string
	EvaluatedAt       time.Time
	ScheduleAvailable bool
	ScheduleTruncated bool
	Schedule          []SupplyScheduleObservation
	Delivery          []SupplyDeliveryObservation
	Exposure          *PodsExposureProof
}

type SupplyEvaluationCounts struct {
	DueUnadmitted    int `json:"due_unadmitted"`
	InFlight         int `json:"in_flight"`
	Scheduled        int `json:"scheduled"`
	Paused           int `json:"paused"`
	ScheduleUnknown  int `json:"schedule_unknown"`
	DeliveryVerified int `json:"delivery_verified"`
	DeliveryPending  int `json:"delivery_pending"`
	DeliveryDegraded int `json:"delivery_degraded"`
	DeliveryUnknown  int `json:"delivery_unknown"`
	NoUpstreamChange int `json:"no_upstream_change"`
	UpstreamDeferred int `json:"upstream_deferred"`
	IntakeBlocked    int `json:"intake_blocked"`
}

type SupplyEvaluation struct {
	SchemaVersion        string                 `json:"schema_version"`
	EvaluatedAt          time.Time              `json:"evaluated_at"`
	Verdict              SupplyVerdict          `json:"verdict"`
	HeadlineBoundary     string                 `json:"headline_boundary"`
	Owner                string                 `json:"owner"`
	EvidenceCompleteness string                 `json:"evidence_completeness"`
	ReadOnly             bool                   `json:"read_only"`
	Summary              string                 `json:"summary"`
	Counts               SupplyEvaluationCounts `json:"counts"`
	AffectedSourceIDs    []string               `json:"affected_source_ids"`
	AffectedRequestIDs   []string               `json:"affected_request_ids"`
	Unknowns             []string               `json:"unknowns"`
}

// EvaluateMediaSupply is a deterministic, read-only headline evaluator. A
// result describes the strongest *observed* boundary in a bounded sample; it
// never turns a missing fact into provider, queue, or retry authority.
func EvaluateMediaSupply(input SupplyEvaluationInput) SupplyEvaluation {
	now := input.EvaluatedAt.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	result := SupplyEvaluation{
		SchemaVersion:        SupplyEvaluationSchemaVersion,
		EvaluatedAt:          now,
		ReadOnly:             true,
		EvidenceCompleteness: "complete",
		AffectedSourceIDs:    []string{},
		AffectedRequestIDs:   []string{},
		Unknowns:             []string{},
	}

	if strings.TrimSpace(input.TenantID) == "" {
		return unavailableSupplyEvaluation(result, "CMS cannot evaluate supply without an explicit tenant.")
	}
	if !input.ScheduleAvailable {
		return unavailableSupplyEvaluation(result, "CMS could not read the Media source scheduling proof.")
	}

	dueSources := []string{}
	unknownSources := []string{}
	for _, observation := range input.Schedule {
		switch observation.State {
		case "due_unadmitted":
			result.Counts.DueUnadmitted++
			dueSources = append(dueSources, observation.SourceID)
		case "in_flight":
			result.Counts.InFlight++
		case "scheduled":
			result.Counts.Scheduled++
		case "paused":
			result.Counts.Paused++
		default:
			result.Counts.ScheduleUnknown++
			unknownSources = append(unknownSources, observation.SourceID)
		}
	}

	degradedSources, degradedRequests := []string{}, []string{}
	failedSources, failedRequests := []string{}, []string{}
	blockedSources, blockedRequests := []string{}, []string{}
	deferredSources, deferredRequests := []string{}, []string{}
	unknownDeliverySources, unknownDeliveryRequests := []string{}, []string{}
	for _, observation := range input.Delivery {
		switch SourceRunOutcome(observation.TerminalOutcome) {
		case OutcomeNoChange:
			result.Counts.NoUpstreamChange++
			continue
		case OutcomeUpstreamChangeDeferred:
			result.Counts.UpstreamDeferred++
			deferredSources = append(deferredSources, observation.SourceID)
			deferredRequests = append(deferredRequests, observation.RequestID)
			continue
		case OutcomeObservationBlockedByIntake, OutcomeConfigurationBlocked:
			result.Counts.IntakeBlocked++
			blockedSources = append(blockedSources, observation.SourceID)
			blockedRequests = append(blockedRequests, observation.RequestID)
			continue
		}
		switch observation.State {
		case "verified":
			result.Counts.DeliveryVerified++
		case "pending":
			result.Counts.DeliveryPending++
		case "degraded":
			result.Counts.DeliveryDegraded++
			degradedSources = append(degradedSources, observation.SourceID)
			degradedRequests = append(degradedRequests, observation.RequestID)
		case "not_observed":
			if isTerminalSourceRunFailure(observation.RequestState) {
				failedSources = append(failedSources, observation.SourceID)
				failedRequests = append(failedRequests, observation.RequestID)
			} else {
				result.Counts.DeliveryUnknown++
				unknownDeliverySources = append(unknownDeliverySources, observation.SourceID)
				unknownDeliveryRequests = append(unknownDeliveryRequests, observation.RequestID)
			}
		default:
			result.Counts.DeliveryUnknown++
			unknownDeliverySources = append(unknownDeliverySources, observation.SourceID)
			unknownDeliveryRequests = append(unknownDeliveryRequests, observation.RequestID)
		}
	}

	if input.ScheduleTruncated {
		result.EvidenceCompleteness = "partial"
		result.Unknowns = append(result.Unknowns, "The registered source-schedule sample reached its read limit; additional source state is unknown.")
	}
	if len(unknownSources) > 0 || len(unknownDeliverySources) > 0 {
		result.EvidenceCompleteness = "partial"
		result.Unknowns = append(result.Unknowns, "Some source schedule or Pods-delivery observations are unknown.")
	}

	// This precedence orders observed lifecycle boundaries only. It does not
	// claim that an unrelated source in the bounded sample shares the same root
	// cause, hence the name headline_boundary rather than a global incident.
	switch {
	case len(input.Schedule) == 0 && len(input.Delivery) == 0:
		result.Verdict = SupplyVerdictNoActiveMediaSources
		result.HeadlineBoundary = "media_source_configuration"
		result.Owner = "Media Sources configuration"
		result.Summary = "CMS found no active Media sources in the bounded tenant-scoped schedule; fresh Pods supply cannot be expected until a source is active."
	case len(dueSources) > 0:
		result.Verdict = SupplyVerdictSourceDueNotAdmitted
		result.HeadlineBoundary = "cms_admission"
		result.Owner = "CMS source-run scheduler"
		result.Summary = "One or more Media sources are due but have no active CMS source run. This is an admission observation, not a provider or queue failure."
		result.AffectedSourceIDs = uniqueSortedIDs(dueSources)
	case len(failedRequests) > 0:
		result.Verdict = SupplyVerdictSourceRunWithoutIngest
		result.HeadlineBoundary = "source_run_execution"
		result.Owner = "Source-run execution owner"
		result.Summary = "One or more terminal Media source runs have no successful CMS ingest proof. The exact failed stage remains in the source-run trace."
		result.AffectedSourceIDs = uniqueSortedIDs(failedSources)
		result.AffectedRequestIDs = uniqueSortedIDs(failedRequests)
	case len(blockedRequests) > 0:
		result.Verdict = SupplyVerdictIntakeObservationBlocked
		result.HeadlineBoundary = "intake_capacity"
		result.Owner = "CMS supply economics and intake controls"
		result.Summary = "A provider observation could not preserve upstream candidates because materialization capacity was zero and the provider lacks a qualified replay boundary."
		result.AffectedSourceIDs = uniqueSortedIDs(blockedSources)
		result.AffectedRequestIDs = uniqueSortedIDs(blockedRequests)
	case len(degradedRequests) > 0:
		result.Verdict = SupplyVerdictPodsDeliveryDegraded
		result.HeadlineBoundary = "pods_delivery_verification"
		result.Owner = "CMS delivery verifier"
		result.Summary = "CMS exhausted the bounded Pods-delivery observation budget for one or more source runs; delivery remains unknown and source execution is not replayed."
		result.AffectedSourceIDs = uniqueSortedIDs(degradedSources)
		result.AffectedRequestIDs = uniqueSortedIDs(degradedRequests)
	case input.Exposure != nil && input.Exposure.EvidenceCompleteness == "unavailable":
		return unavailableSupplyEvaluation(result, "CMS could not read the canonical Pods eligibility and exposure proof.")
	case input.Exposure != nil && input.Exposure.Verdict == "no_base_eligible_inventory":
		result.Verdict = SupplyVerdictNoEligibleInventory
		result.HeadlineBoundary = "content_readiness"
		result.Owner = "Pipeline and artifact owners"
		result.Summary = "CMS found active Media sources but no content currently satisfies the canonical base Pods eligibility predicate."
	case input.Exposure != nil && input.Exposure.Verdict == "eligible_not_generation_reachable":
		result.Verdict = SupplyVerdictGenerationOmission
		result.HeadlineBoundary = "generation_membership"
		result.Owner = "Feed Integrity"
		result.Summary = "CMS found base-eligible Pods content missing from the current active media generation."
	case input.Exposure != nil && input.Exposure.Verdict == "eligible_not_returned":
		result.Verdict = SupplyVerdictServingOmission
		result.HeadlineBoundary = "feed_return"
		result.Owner = "Feed Integrity and CMS serving"
		result.Summary = "CMS found current-generation Pods inventory but the isolated non-perturbing feed probe returned no items."
	case input.Exposure != nil && input.Exposure.Verdict == "pods_inventory_stale":
		result.Verdict = SupplyVerdictPodsInventoryStale
		result.HeadlineBoundary = "supply_freshness"
		result.Owner = "Media Circulation"
		result.Summary = "The newest current-generation Pods inventory is older than the registered supply freshness window."
	case len(deferredRequests) > 0:
		result.Verdict = SupplyVerdictUpstreamDeferred
		result.HeadlineBoundary = "deferred_inventory"
		result.Owner = "CMS deferred supply"
		result.Summary = "CMS preserved upstream identities under a qualified replay boundary, but materialization remains deferred."
		result.AffectedSourceIDs = uniqueSortedIDs(deferredSources)
		result.AffectedRequestIDs = uniqueSortedIDs(deferredRequests)
	case len(result.Unknowns) > 0:
		result.Verdict = SupplyVerdictEvidenceUnavailable
		result.HeadlineBoundary = "evidence"
		result.Owner = "CMS evidence reader"
		result.Summary = "CMS does not have complete bounded supply evidence for a safe operational conclusion."
		result.AffectedSourceIDs = uniqueSortedIDs(append(unknownSources, unknownDeliverySources...))
		result.AffectedRequestIDs = uniqueSortedIDs(unknownDeliveryRequests)
	case result.Counts.InFlight > 0 || result.Counts.DeliveryPending > 0:
		result.Verdict = SupplyVerdictObservationPending
		result.HeadlineBoundary = "observation"
		result.Owner = "CMS source-run and delivery verification"
		result.Summary = "Media source work or bounded Pods-delivery observation is still in progress; no terminal delivery conclusion is available yet."
	case result.Counts.NoUpstreamChange > 0:
		result.Verdict = SupplyVerdictHealthyNoUpstreamChange
		result.HeadlineBoundary = "provider_observation"
		result.Owner = "Source provider evidence"
		result.Summary = "The latest terminal provider evidence reported no upstream change; no downstream item delivery was expected for that run."
	default:
		result.Verdict = SupplyVerdictNoCurrentBreak
		result.HeadlineBoundary = "none_observed"
		result.Owner = "CMS supply evidence"
		result.Summary = "The bounded CMS supply sample has no current admission, execution, or Pods-delivery break. This does not assert provider no-change beyond recorded source-run evidence."
	}
	return result
}

func unavailableSupplyEvaluation(result SupplyEvaluation, reason string) SupplyEvaluation {
	result.Verdict = SupplyVerdictEvidenceUnavailable
	result.HeadlineBoundary = "evidence"
	result.Owner = "CMS evidence reader"
	result.EvidenceCompleteness = "unavailable"
	result.Summary = "CMS cannot form a supply conclusion because required evidence is unavailable."
	result.Unknowns = []string{reason}
	return result
}

func isTerminalSourceRunFailure(state string) bool {
	switch state {
	case "failed", "partial", "cancelled", "expired", "blocked":
		return true
	default:
		return false
	}
}

func uniqueSortedIDs(values []string) []string {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			set[value] = struct{}{}
		}
	}
	result := make([]string, 0, len(set))
	for value := range set {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
