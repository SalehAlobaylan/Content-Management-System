package operator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/supply"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

const (
	approvalHandoffAdapterKey   = "media_sources.approval_handoff"
	mediaCirculationAdapterKey  = "media_circulation.state"
	feedIntegrityAdapterKey     = "feed_integrity.snapshot_state"
	feedRecoveryAdapterKey      = "feed_recovery.state"
	retentionAdapterKey         = "retention.state"
	operationalEvidenceMaxAge   = 60 * time.Second
	mediaContinuitySourceLimit  = 20
	mediaSupplyEpisodeReadLimit = 12
)

// ReadBudget is a hard code-owned bound; adapters cannot silently expand their
// relationship traversal or row reads based on a model response.
type ReadBudget struct {
	MaxAdapters      int
	MaxRelationDepth int
	MaxRowsPerDomain int
}

var normalQuestionBudget = ReadBudget{MaxAdapters: 8, MaxRelationDepth: 2, MaxRowsPerDomain: 200}

type AdapterDescriptor struct {
	Key                string
	Version            string
	Domain             string
	RequiredPermission string
	MaxRows            int
	MaxAge             time.Duration
	DeepLinkBase       string
}

type RelationshipEdge struct {
	From               string
	To                 string
	RequiredPermission string
	TenantKey          string
	Cardinality        string
	MaxDepth           int
	MaxRows            int
	MaxAge             time.Duration
}

// AdapterRegistry is the sole admission list for operator reads. URLs and
// joins are registered here, rather than emitted by a browser or model.
type AdapterRegistry struct {
	adapters map[string]AdapterDescriptor
	edges    map[string][]RelationshipEdge
}

// HasRegisteredAdapterKey lets governance controls disable a real, code-owned
// read adapter without turning a free-form key into a capability.
func HasRegisteredAdapterKey(key string) bool {
	_, exists := DefaultAdapterRegistry().adapters[strings.TrimSpace(key)]
	return exists
}

func DefaultAdapterRegistry() AdapterRegistry {
	registry := AdapterRegistry{
		adapters: map[string]AdapterDescriptor{
			approvalHandoffAdapterKey: {
				Key: approvalHandoffAdapterKey, Version: "v1", Domain: "media_sources", RequiredPermission: "source:read",
				MaxRows: 200, MaxAge: operationalEvidenceMaxAge, DeepLinkBase: "/platform/media/sources",
			},
			mediaCirculationAdapterKey: {
				Key: mediaCirculationAdapterKey, Version: "v1", Domain: "media_circulation", RequiredPermission: "aggregation:read",
				MaxRows: 200, MaxAge: operationalEvidenceMaxAge, DeepLinkBase: "/platform/media/circulation",
			},
			feedIntegrityAdapterKey: {
				Key: feedIntegrityAdapterKey, Version: "v1", Domain: "feed_integrity", RequiredPermission: "feed:read",
				MaxRows: 1, MaxAge: operationalEvidenceMaxAge, DeepLinkBase: "/platform/feed-integrity",
			},
			feedRecoveryAdapterKey: {
				Key: feedRecoveryAdapterKey, Version: "v1", Domain: "feed_recovery", RequiredPermission: "feed:read",
				MaxRows: 200, MaxAge: operationalEvidenceMaxAge, DeepLinkBase: "/platform/feed-recovery",
			},
			retentionAdapterKey: {
				Key: retentionAdapterKey, Version: "v1", Domain: "retention", RequiredPermission: "feed:read",
				MaxRows: 200, MaxAge: operationalEvidenceMaxAge, DeepLinkBase: "/platform/retention",
			},
		},
		edges: map[string][]RelationshipEdge{
			"content_source": {
				{From: "content_source", To: "source_run_request", RequiredPermission: "source:read", TenantKey: "tenant_id", Cardinality: "one_to_many", MaxDepth: 1, MaxRows: 20, MaxAge: operationalEvidenceMaxAge},
			},
			"source_run_request": {
				{From: "source_run_request", To: "content_processing_event", RequiredPermission: "source:read", TenantKey: "tenant_id", Cardinality: "one_to_many", MaxDepth: 2, MaxRows: 200, MaxAge: operationalEvidenceMaxAge},
				{From: "source_run_request", To: "content_item", RequiredPermission: "source:read", TenantKey: "tenant_id", Cardinality: "one_to_many", MaxDepth: 2, MaxRows: 50, MaxAge: operationalEvidenceMaxAge},
			},
			"feed_recovery_plan": {
				{From: "feed_recovery_plan", To: "feed_recovery_run", RequiredPermission: "feed:read", TenantKey: "tenant_id", Cardinality: "one_to_many", MaxDepth: 1, MaxRows: 20, MaxAge: operationalEvidenceMaxAge},
			},
			"feed_recovery_run": {
				{From: "feed_recovery_run", To: "feed_recovery_action", RequiredPermission: "feed:read", TenantKey: "tenant_id", Cardinality: "one_to_many", MaxDepth: 2, MaxRows: 200, MaxAge: operationalEvidenceMaxAge},
			},
			"retention_policy": {
				{From: "retention_policy", To: "retention_db_sample", RequiredPermission: "feed:read", TenantKey: "tenant_id", Cardinality: "one_to_many", MaxDepth: 1, MaxRows: 20, MaxAge: operationalEvidenceMaxAge},
				{From: "retention_policy", To: "retention_run", RequiredPermission: "feed:read", TenantKey: "tenant_id", Cardinality: "one_to_many", MaxDepth: 1, MaxRows: 20, MaxAge: operationalEvidenceMaxAge},
			},
			"retention_run": {
				{From: "retention_run", To: "retention_action", RequiredPermission: "feed:read", TenantKey: "tenant_id", Cardinality: "one_to_many", MaxDepth: 2, MaxRows: 200, MaxAge: operationalEvidenceMaxAge},
			},
		},
	}
	for _, adapter := range defaultCoverageAdapters() {
		registry.adapters[adapter.Descriptor.Key] = adapter.Descriptor
	}
	return registry
}

// BuildMediaCirculationStatePacket gives the operator a bounded, tenant-scoped
// view of the policy, latest run, and outstanding recommendations. It reports
// only persisted CMS facts: it does not inspect worker queues or infer that a
// scheduled run will execute.
func (fabric *ContextFabric) BuildMediaCirculationStatePacket(ctx context.Context, visible VisibleContext, access AccessSnapshot) (DecisionPacket, error) {
	if err := visible.Validate(); err != nil {
		return DecisionPacket{}, err
	}
	if err := fabric.registry.Validate(); err != nil {
		return DecisionPacket{}, err
	}
	if visible.Domain != "media_circulation" {
		return DecisionPacket{}, fmt.Errorf("%w: media circulation requires its canonical visible context", ErrInvalidContract)
	}
	descriptor, ok := fabric.registry.Descriptor(mediaCirculationAdapterKey)
	if !ok || descriptor.MaxRows < 1 || descriptor.MaxRows > normalQuestionBudget.MaxRowsPerDomain || descriptor.DeepLinkBase != "/platform/media/circulation" {
		return DecisionPacket{}, fmt.Errorf("%w: media circulation adapter is not safely registered", ErrInvalidContract)
	}
	if err := access.ValidateFor(access.UserID, access.TenantID); err != nil || !access.HasPermission(descriptor.RequiredPermission) {
		return DecisionPacket{}, fmt.Errorf("%w: aggregation read access is required", ErrAccessUnavailable)
	}

	startedAt := fabric.now()
	now := fabric.now()
	evidence := make([]EvidenceRef, 0, 3)
	facts := make([]Fact, 0, 5)
	unknowns := []string{}

	var policy models.MediaCirculationPolicy
	if err := fabric.db.WithContext(ctx).Where("tenant_id=?", access.TenantID).First(&policy).Error; err == nil {
		policyEvidence := fabric.evidence(descriptor, access.TenantID, "media_circulation_policy:"+access.TenantID, []SubjectRef{{Type: "media_circulation_policy", ID: access.TenantID}}, policy.UpdatedAt, policy.UpdatedAt, evidenceAvailabilityForUpdatedAt(policy.UpdatedAt, now), map[string]any{"enabled": policy.Enabled, "autopilot_enabled": policy.AutopilotEnabled, "autopilot_mode": policy.AutopilotMode, "paused_until": policy.AutopilotPausedUntil})
		evidence = append(evidence, policyEvidence)
		facts = append(facts, Fact{Key: "media_circulation.policy", Value: map[string]any{"enabled": policy.Enabled, "preset": policy.Preset, "autopilot_enabled": policy.AutopilotEnabled, "autopilot_mode": policy.AutopilotMode, "paused_until": policy.AutopilotPausedUntil, "last_run_at": policy.AutopilotLastRunAt}, EvidenceIDs: []string{policyEvidence.EvidenceID}})
	} else if err == gorm.ErrRecordNotFound {
		unknowns = append(unknowns, "No media-circulation policy row exists for this tenant, so current engine configuration is unknown.")
	} else {
		return DecisionPacket{}, err
	}

	var run models.MediaCirculationRun
	if err := fabric.db.WithContext(ctx).Where("tenant_id=?", access.TenantID).Order("started_at DESC").First(&run).Error; err == nil {
		runEvidence := fabric.evidence(descriptor, access.TenantID, "media_circulation_run:"+run.PublicID.String(), []SubjectRef{{Type: "media_circulation_run", ID: run.PublicID.String()}}, run.StartedAt, run.UpdatedAt, evidenceAvailabilityForUpdatedAt(run.UpdatedAt, now), map[string]any{"status": run.Status, "trigger": run.Trigger, "finished_at": run.FinishedAt, "error": run.Error})
		evidence = append(evidence, runEvidence)
		facts = append(facts, Fact{Key: "media_circulation.latest_run", Value: map[string]any{"id": run.PublicID.String(), "status": run.Status, "trigger": run.Trigger, "started_at": run.StartedAt, "finished_at": run.FinishedAt, "summary": run.Summary, "error": run.Error}, EvidenceIDs: []string{runEvidence.EvidenceID}})
	} else if err == gorm.ErrRecordNotFound {
		unknowns = append(unknowns, "No media-circulation run has been recorded for this tenant.")
	} else {
		return DecisionPacket{}, err
	}

	var recommendations []models.MediaCirculationRecommendation
	if err := fabric.db.WithContext(ctx).Where("tenant_id=?", access.TenantID).Order("status = 'pending' DESC, updated_at DESC").Limit(descriptor.MaxRows).Find(&recommendations).Error; err != nil {
		return DecisionPacket{}, err
	}
	byStatus := map[string]int{}
	byVerdict := map[string]int{}
	latestRecommendationAt := time.Time{}
	for _, recommendation := range recommendations {
		byStatus[recommendation.Status]++
		byVerdict[recommendation.Verdict]++
		if recommendation.UpdatedAt.After(latestRecommendationAt) {
			latestRecommendationAt = recommendation.UpdatedAt
		}
	}
	recommendationEvidence := fabric.evidence(descriptor, access.TenantID, "media_circulation_recommendations:"+access.TenantID, []SubjectRef{{Type: "media_circulation_recommendation_collection", ID: access.TenantID}}, latestRecommendationAt, latestRecommendationAt, evidenceAvailabilityForUpdatedAt(latestRecommendationAt, now), map[string]any{"count": len(recommendations), "by_status": byStatus, "by_verdict": byVerdict})
	evidence = append(evidence, recommendationEvidence)
	facts = append(facts, Fact{Key: "media_circulation.recommendations", Value: map[string]any{"sampled_count": len(recommendations), "max_rows": descriptor.MaxRows, "truncated": len(recommendations) == descriptor.MaxRows, "by_status": byStatus, "by_verdict": byVerdict}, EvidenceIDs: []string{recommendationEvidence.EvidenceID}})
	if len(recommendations) > 0 {
		samples := make([]TemporalSample, 0, len(recommendations))
		for _, recommendation := range recommendations {
			samples = append(samples, TemporalSample{EvidenceID: recommendationEvidence.EvidenceID, ObservedAt: recommendation.UpdatedAt, Value: recommendation.Score})
		}
		summary, err := SummarizeTemporal(samples, descriptor.MaxRows)
		if err != nil {
			return DecisionPacket{}, err
		}
		trendEvidence := fabric.temporalEvidence(descriptor, access.TenantID, "media_circulation_recommendation_score_trend:"+access.TenantID, recommendationEvidence.RecordRefs, summary, now)
		evidence = append(evidence, trendEvidence)
		facts = append(facts, Fact{Key: "media_circulation.recommendation_score_trend", Value: map[string]any{"window_start": summary.WindowStart, "window_end": summary.WindowEnd, "first_score": summary.FirstValue, "latest_score": summary.LatestValue, "delta": summary.Delta, "direction": summary.Direction, "sampled_count": len(samples)}, EvidenceIDs: []string{trendEvidence.EvidenceID}})
	}
	if len(recommendations) == descriptor.MaxRows {
		unknowns = append(unknowns, "Recommendation results reached the registered read limit; totals beyond this bounded sample are unknown.")
	}
	continuityFacts, continuityEvidence, continuityUnknowns, err := fabric.mediaCirculationContinuityFacts(ctx, descriptor, access, now)
	if err != nil {
		return DecisionPacket{}, err
	}
	facts = append(facts, continuityFacts...)
	evidence = append(evidence, continuityEvidence...)
	unknowns = append(unknowns, continuityUnknowns...)

	return fabric.packet(visible, access, startedAt, facts, evidence, unknowns)
}

// mediaCirculationContinuityFacts brings the durable source-run evidence used
// by the Media Circulation cockpit into the registered Operator adapter. It is
// intentionally read-only and bounded; it never reads BullMQ, a provider, or a
// receipt payload, and does not turn a due checkpoint into a retry authority.
func (fabric *ContextFabric) mediaCirculationContinuityFacts(ctx context.Context, descriptor AdapterDescriptor, access AccessSnapshot, now time.Time) ([]Fact, []EvidenceRef, []string, error) {
	var sources []models.ContentSource
	if err := fabric.db.WithContext(ctx).Where("tenant_id = ? AND category = ?", access.TenantID, models.SourceCategoryMedia).
		Order("CASE WHEN next_due_at IS NULL THEN 1 ELSE 0 END, next_due_at ASC, public_id ASC").Limit(mediaContinuitySourceLimit).Find(&sources).Error; err != nil {
		return nil, nil, nil, err
	}
	rows := make([]map[string]any, 0, len(sources))
	refs := make([]SubjectRef, 0, len(sources)*2)
	observedAt := time.Time{}
	unknowns := []string{}
	for _, source := range sources {
		var latest models.SourceRunRequest
		err := fabric.db.WithContext(ctx).Where("tenant_id = ? AND content_source_id = ? AND lane = ?", access.TenantID, source.PublicID, models.SourceCategoryMedia).
			Order("requested_at DESC, public_id DESC").First(&latest).Error
		if err != nil && err != gorm.ErrRecordNotFound {
			return nil, nil, nil, err
		}
		hasLatest := err == nil
		state, reason := operatorMediaSourceScheduleState(source, hasLatest, latest, now)
		row := map[string]any{
			"source_id": source.PublicID.String(), "source_name": source.Name, "schedule_state": state, "reason": reason,
			"next_due_at": source.NextDueAt, "last_claimed_at": source.LastClaimedAt, "last_attempted_at": source.LastAttemptedAt,
			"last_provider_success_at": source.LastProviderSuccessAt, "last_new_item_at": source.LastNewItemAt,
			"last_delivery_verified_at": source.LastDeliveryVerifiedAt, "intake_circuit_until": source.IntakeCircuitUntil,
		}
		refs = append(refs, SubjectRef{Type: "content_source", ID: source.PublicID.String(), Label: source.Name})
		if source.UpdatedAt.After(observedAt) {
			observedAt = source.UpdatedAt
		}
		if !hasLatest {
			row["delivery_state"] = "unknown"
			row["delivery_reason"] = "No CMS source-run request exists for this source."
			unknowns = append(unknowns, "No CMS source-run request exists for media source "+source.Name+"; Aggregation acceptance and Pods delivery cannot be proven.")
			rows = append(rows, row)
			continue
		}
		refs = append(refs, SubjectRef{Type: "source_run_request", ID: latest.PublicID.String()})
		row["latest_request_id"] = latest.PublicID.String()
		row["latest_request_state"] = latest.State
		row["latest_request_evidence_state"] = latest.EvidenceState
		var terminalUnit models.SourceRunExecutionUnit
		if err := fabric.db.WithContext(ctx).Where("tenant_id=? AND source_run_request_id=? AND terminal_outcome<>''", access.TenantID, latest.PublicID).Order("finished_at DESC NULLS LAST, created_at DESC").First(&terminalUnit).Error; err == nil {
			row["terminal_outcome"] = terminalUnit.TerminalOutcome
		} else if err != gorm.ErrRecordNotFound {
			return nil, nil, nil, err
		}
		if latest.UpdatedAt.After(observedAt) {
			observedAt = latest.UpdatedAt
		}

		var task models.SourceRunVerificationTask
		taskErr := fabric.db.WithContext(ctx).Where("tenant_id = ? AND source_run_request_id = ? AND causation_id LIKE ?", access.TenantID, latest.PublicID, "consumer_pods_delivery:%").Order("created_at DESC, public_id DESC").First(&task).Error
		if taskErr != nil && taskErr != gorm.ErrRecordNotFound {
			return nil, nil, nil, taskErr
		}
		var events []models.SourceRunReconciliationEvent
		if err := fabric.db.WithContext(ctx).Where("tenant_id = ? AND source_run_request_id = ? AND (causation_id = ? OR causation_id LIKE ?)", access.TenantID, latest.PublicID, "consumer_delivery", "consumer_pods_delivery:%").Order("observed_at DESC, public_id DESC").Limit(26).Find(&events).Error; err != nil {
			return nil, nil, nil, err
		}
		var ingest, pods models.SourceRunReconciliationEvent
		hasIngest, hasPods := false, false
		for _, event := range events {
			if event.CausationID == "consumer_delivery" && !hasIngest {
				ingest, hasIngest = event, true
			}
			if strings.HasPrefix(event.CausationID, "consumer_pods_delivery:") && !hasPods {
				pods, hasPods = event, true
			}
		}
		deliveryState, deliveryReason := operatorMediaDeliveryState(latest, hasIngest, ingest, taskErr == nil, task, hasPods, pods)
		row["delivery_state"] = deliveryState
		row["delivery_reason"] = deliveryReason
		if hasIngest {
			row["cms_ingest_verdict"] = ingest.Verdict
			row["cms_ingest_observed_at"] = ingest.ObservedAt
		}
		if hasPods {
			row["pods_verdict"] = pods.Verdict
			row["pods_observed_at"] = pods.ObservedAt
		}
		if taskErr == nil {
			row["pods_observation_state"] = task.State
			row["pods_observation_attempts"] = task.AttemptCount
			row["pods_observation_next_at"] = task.NotBeforeAt
		}
		rows = append(rows, row)
	}
	if len(sources) == mediaContinuitySourceLimit {
		unknowns = append(unknowns, "Media source continuity results reached the registered read limit; additional source schedules are unknown.")
	}
	if observedAt.IsZero() {
		// An empty tenant-scoped collection is an observed CMS fact, not a stale
		// fabricated source timestamp. The collection query itself occurred now.
		observedAt = now
	}
	evidence := fabric.evidence(descriptor, access.TenantID, "media_circulation_source_continuity:"+access.TenantID, refs, observedAt, observedAt, evidenceAvailabilityForUpdatedAt(observedAt, now), map[string]any{"sampled_count": len(rows), "max_rows": mediaContinuitySourceLimit})
	fact := Fact{Key: "media_circulation.source_continuity", Value: map[string]any{"sampled_count": len(rows), "max_rows": mediaContinuitySourceLimit, "truncated": len(sources) == mediaContinuitySourceLimit, "sources": rows, "legacy_last_fetched_is_not_provider_success": true}, EvidenceIDs: []string{evidence.EvidenceID}}

	supplyFacts, supplyEvidence, supplyUnknowns := fabric.mediaSupplyContinuityFacts(ctx, descriptor, access, now, observedAt, rows, len(sources) == mediaContinuitySourceLimit)
	return append([]Fact{fact}, supplyFacts...), append([]EvidenceRef{evidence}, supplyEvidence...), append(unknowns, supplyUnknowns...), nil
}

// mediaSupplyContinuityFacts projects the same bounded, CMS-owned source and
// delivery observations into the Operator packet. It is deliberately a read
// projection: no source run is created, replayed, or repaired while an admin
// investigates an observed supply break.
func (fabric *ContextFabric) mediaSupplyContinuityFacts(ctx context.Context, descriptor AdapterDescriptor, access AccessSnapshot, now, sourceObservedAt time.Time, sourceRows []map[string]any, truncated bool) ([]Fact, []EvidenceRef, []string) {
	input := supply.SupplyEvaluationInput{
		TenantID: access.TenantID, EvaluatedAt: now, ScheduleAvailable: true, ScheduleTruncated: truncated,
		Schedule: make([]supply.SupplyScheduleObservation, 0, len(sourceRows)),
		Delivery: make([]supply.SupplyDeliveryObservation, 0, len(sourceRows)),
	}
	for _, row := range sourceRows {
		sourceID, _ := row["source_id"].(string)
		state, _ := row["schedule_state"].(string)
		input.Schedule = append(input.Schedule, supply.SupplyScheduleObservation{SourceID: sourceID, State: state})

		// A delivery observation is meaningful only for a concrete durable
		// source-run request. A source that has not been admitted must not be
		// recast as a Pods-delivery failure.
		requestID, hasRequest := row["latest_request_id"].(string)
		if !hasRequest || strings.TrimSpace(requestID) == "" {
			continue
		}
		requestState, _ := row["latest_request_state"].(string)
		deliveryState, _ := row["delivery_state"].(string)
		terminalOutcome, _ := row["terminal_outcome"].(string)
		input.Delivery = append(input.Delivery, supply.SupplyDeliveryObservation{RequestID: requestID, SourceID: sourceID, RequestState: requestState, State: deliveryState, TerminalOutcome: terminalOutcome})
	}
	exposure := supply.BuildPodsExposureProof(fabric.db.WithContext(ctx), access.TenantID, now)
	input.Exposure = &exposure
	evaluation := supply.EvaluateMediaSupply(input)
	digest, err := supply.EvaluationEvidenceDigest(evaluation)
	unknowns := []string{}
	if err != nil {
		// The digest is an evidence fingerprint, not authority. Retain the
		// current evaluator result as partial evidence if serialization ever
		// fails instead of hiding the underlying source continuity facts.
		digest = "unavailable"
		unknowns = append(unknowns, "CMS could not fingerprint the current Supply Continuity evaluation.")
	}
	if sourceObservedAt.IsZero() {
		sourceObservedAt = now
	}
	headlineEvidence := fabric.evidence(descriptor, access.TenantID, "media_supply_headline:"+access.TenantID,
		[]SubjectRef{{Type: "media_supply_evaluation", ID: access.TenantID}}, sourceObservedAt, sourceObservedAt,
		evidenceAvailabilityForUpdatedAt(sourceObservedAt, now), map[string]any{"evaluation": evaluation, "evaluation_digest": digest})
	headlineFact := Fact{Key: "media_circulation.supply_continuity", Value: map[string]any{
		"schema_version": evaluation.SchemaVersion, "verdict": string(evaluation.Verdict), "headline_boundary": evaluation.HeadlineBoundary,
		"owner": evaluation.Owner, "evidence_completeness": evaluation.EvidenceCompleteness, "read_only": evaluation.ReadOnly,
		"summary": evaluation.Summary, "counts": evaluation.Counts, "affected_source_ids": evaluation.AffectedSourceIDs,
		"affected_request_ids": evaluation.AffectedRequestIDs, "unknowns": evaluation.Unknowns, "evaluation_digest": digest,
	}, EvidenceIDs: []string{headlineEvidence.EvidenceID}}

	facts := []Fact{headlineFact}
	evidence := []EvidenceRef{headlineEvidence}
	exposureEvidence := fabric.evidence(descriptor, access.TenantID, "media_supply_exposure:"+access.TenantID,
		[]SubjectRef{{Type: "pods_exposure", ID: access.TenantID}}, exposure.GeneratedAt, exposure.GeneratedAt,
		evidenceAvailabilityForUpdatedAt(exposure.GeneratedAt, now), map[string]any{"exposure": exposure})
	facts = append(facts, Fact{Key: "media_circulation.pods_exposure", Value: map[string]any{
		"schema_version": exposure.SchemaVersion, "verdict": exposure.Verdict, "evidence_completeness": exposure.EvidenceCompleteness,
		"base_eligible_count": exposure.BaseEligibleCount, "reachable_count": exposure.ReachableCount, "returned_count": exposure.ReturnedCount,
		"eligible_returned_gap": exposure.EligibleReturnedGap, "repeat_pressure": exposure.RepeatPressure,
		"active_generation_id": exposure.ActiveGenerationID, "probe_id": exposure.ProbeID,
		"newest_eligible_at": exposure.NewestEligibleAt, "newest_reachable_at": exposure.NewestReachableAt, "newest_returned_at": exposure.NewestReturnedAt,
		"last_feed_rendered_at": exposure.LastFeedRenderedAt, "last_exact_view_at": exposure.LastExactViewAt,
		"returned_ids": exposure.ReturnedIDs, "rendered_ids": exposure.RenderedIDs, "viewed_ids": exposure.ViewedIDs, "unknowns": exposure.Unknowns,
	}, EvidenceIDs: []string{exposureEvidence.EvidenceID}})
	evidence = append(evidence, exposureEvidence)

	worker := supply.MediaSupplyEvaluatorWorkerStatusAt(now)
	evaluatorObservedAt := now
	if worker.LastHeartbeat != nil {
		evaluatorObservedAt = *worker.LastHeartbeat
	}
	evaluator := map[string]any{
		"worker_state": worker.State, "worker_last_heartbeat_at": worker.LastHeartbeat,
		"worker_stale_after_at": worker.StaleAfterAt, "recording_enabled": nil,
		"checkpoint_present": false, "checkpoint_last_trigger": "", "checkpoint_last_outcome": "",
		"checkpoint_last_observed_at": nil, "checkpoint_last_evaluated_at": nil, "checkpoint_evaluation_digest": nil,
	}
	evaluatorRefs := []SubjectRef{{Type: "media_supply_evaluator", ID: access.TenantID}}
	var control models.MediaSupplyControl
	controlErr := fabric.db.WithContext(ctx).Where("tenant_id = ? AND control_key = ? AND scope_type = ? AND scope_id = ?", access.TenantID,
		models.MediaSupplyControlReadEvaluation, models.MediaSupplyControlScopeTenant, models.MediaSupplyControlScopeAll).First(&control).Error
	switch controlErr {
	case nil:
		value := false
		evaluator["recording_enabled"] = value
		evaluatorRefs = append(evaluatorRefs, SubjectRef{Type: "media_supply_control", ID: access.TenantID})
		if control.UpdatedAt.After(evaluatorObservedAt) {
			evaluatorObservedAt = control.UpdatedAt
		}
	case gorm.ErrRecordNotFound:
		value := true
		evaluator["recording_enabled"] = value
	default:
		unknowns = append(unknowns, "CMS could not read the Supply evaluator control state.")
	}
	var checkpoint models.MediaSupplyEvaluationCheckpoint
	checkpointErr := fabric.db.WithContext(ctx).Where("tenant_id = ?", access.TenantID).First(&checkpoint).Error
	switch checkpointErr {
	case nil:
		evaluator["checkpoint_present"] = true
		evaluator["checkpoint_last_trigger"] = checkpoint.LastTrigger
		evaluator["checkpoint_last_outcome"] = checkpoint.LastOutcome
		evaluator["checkpoint_last_observed_at"] = checkpoint.LastObservedAt
		evaluator["checkpoint_last_evaluated_at"] = checkpoint.LastEvaluatedAt
		evaluator["checkpoint_evaluation_digest"] = checkpoint.EvaluationDigest
		evaluatorRefs = append(evaluatorRefs, SubjectRef{Type: "media_supply_evaluation_checkpoint", ID: access.TenantID})
		if checkpoint.LastObservedAt.After(evaluatorObservedAt) {
			evaluatorObservedAt = checkpoint.LastObservedAt
		}
	case gorm.ErrRecordNotFound:
		// No persisted checkpoint is a factual startup state. The current direct
		// packet remains useful and should not be presented as an outage.
	default:
		unknowns = append(unknowns, "CMS could not read the Supply evaluator checkpoint.")
	}
	evaluatorEvidence := fabric.evidence(descriptor, access.TenantID, "media_supply_evaluator:"+access.TenantID, evaluatorRefs,
		evaluatorObservedAt, evaluatorObservedAt, evidenceAvailabilityForUpdatedAt(evaluatorObservedAt, now), evaluator)
	facts = append(facts, Fact{Key: "media_circulation.supply_evaluator", Value: evaluator, EvidenceIDs: []string{evaluatorEvidence.EvidenceID}})
	evidence = append(evidence, evaluatorEvidence)

	var episodes []models.MediaSupplyEpisode
	episodeErr := fabric.db.WithContext(ctx).Where("tenant_id = ?", access.TenantID).
		Order("state ASC, last_seen_at DESC, public_id ASC").
		Limit(mediaSupplyEpisodeReadLimit).Find(&episodes).Error
	if episodeErr != nil {
		unknowns = append(unknowns, "CMS could not read the current Supply Continuity attention episodes.")
		return facts, evidence, unknowns
	}
	episodeRows := make([]map[string]any, 0, len(episodes))
	episodeRefs := make([]SubjectRef, 0, len(episodes))
	episodeObservedAt := now
	for _, episode := range episodes {
		episodeRows = append(episodeRows, map[string]any{
			"episode_id": episode.PublicID.String(), "verdict": episode.Verdict, "severity": episode.Severity,
			"owner": episode.Owner, "state": episode.State, "summary": episode.Summary,
			"first_failed_boundary": episode.FirstFailedBoundary, "evidence_completeness": episode.EvidenceCompleteness,
			"first_seen_at": episode.FirstSeenAt, "last_seen_at": episode.LastSeenAt, "resolved_at": episode.ResolvedAt,
			"has_resolution_proof": len(episode.ResolutionProof) > 0,
		})
		episodeRefs = append(episodeRefs, SubjectRef{Type: "media_supply_episode", ID: episode.PublicID.String()})
		if episode.LastSeenAt.After(episodeObservedAt) {
			episodeObservedAt = episode.LastSeenAt
		}
	}
	if len(episodes) == mediaSupplyEpisodeReadLimit {
		unknowns = append(unknowns, "Supply Continuity episode results reached the registered read limit; additional attention records are unknown.")
	}
	episodeEvidence := fabric.evidence(descriptor, access.TenantID, "media_supply_episodes:"+access.TenantID, episodeRefs,
		episodeObservedAt, episodeObservedAt, evidenceAvailabilityForUpdatedAt(episodeObservedAt, now), map[string]any{"sampled_count": len(episodeRows), "max_rows": mediaSupplyEpisodeReadLimit, "episodes": episodeRows})
	facts = append(facts, Fact{Key: "media_circulation.supply_attention", Value: map[string]any{
		"sampled_count": len(episodeRows), "max_rows": mediaSupplyEpisodeReadLimit, "truncated": len(episodes) == mediaSupplyEpisodeReadLimit,
		"episodes": episodeRows,
	}, EvidenceIDs: []string{episodeEvidence.EvidenceID}})
	evidence = append(evidence, episodeEvidence)
	return facts, evidence, unknowns
}

func operatorMediaSourceScheduleState(source models.ContentSource, hasLatest bool, latest models.SourceRunRequest, now time.Time) (string, string) {
	if !source.IsActive {
		return "paused", "This source is inactive; no scheduling admission is expected."
	}
	if source.NextDueAt == nil {
		return "unknown", "CMS has no explicit next-due checkpoint for this active source."
	}
	if hasLatest && operatorSourceRunRequestActive(latest.State) {
		return "in_flight", "A durable source run is active; CMS will verify it before treating delivery as complete."
	}
	if now.Before(*source.NextDueAt) {
		return "scheduled", "The source is not due yet under its current CMS schedule."
	}
	return "due_unadmitted", "The source is due and has no active CMS source run; this is an admission fact, not a provider failure."
}

func operatorSourceRunRequestActive(state string) bool {
	switch state {
	case models.SourceRunRequested, models.SourceRunAccepted, models.SourceRunRunning, models.SourceRunVerificationRequired:
		return true
	default:
		return false
	}
}

func operatorMediaDeliveryState(request models.SourceRunRequest, hasIngest bool, ingest models.SourceRunReconciliationEvent, hasTask bool, task models.SourceRunVerificationTask, hasPods bool, pods models.SourceRunReconciliationEvent) (string, string) {
	if hasPods && pods.Verdict == string(supply.VerdictPresent) {
		return "verified", "CMS confirmed that source-run media remains eligible for Pods, directly or through an atomized child."
	}
	if hasPods && pods.Verdict == string(supply.VerdictUnknown) && hasTask && task.State == models.SourceRunVerificationTaskTerminal {
		return "degraded", "Pods delivery evidence remained unknown after the bounded observation budget."
	}
	if hasTask && task.State != models.SourceRunVerificationTaskTerminal {
		return "pending", "CMS is waiting for its next bounded Pods delivery observation."
	}
	if request.State == models.SourceRunVerificationRequired || (hasIngest && ingest.Verdict == string(supply.VerdictUnknown)) {
		return "pending", "CMS ingest proof is still verification-required."
	}
	return "unknown", "No authoritative Pods delivery observation exists for this source run."
}

// BuildFeedRecoveryStatePacket is deliberately observational. It reads only
// the persisted preflight/run/action and availability ledgers; destructive
// target sets and reauthentication material never leave their native workflow.
func (fabric *ContextFabric) BuildFeedRecoveryStatePacket(ctx context.Context, visible VisibleContext, access AccessSnapshot) (DecisionPacket, error) {
	if err := visible.Validate(); err != nil {
		return DecisionPacket{}, err
	}
	if err := fabric.registry.Validate(); err != nil {
		return DecisionPacket{}, err
	}
	if visible.Domain != "feed_recovery" {
		return DecisionPacket{}, fmt.Errorf("%w: feed recovery requires its canonical visible context", ErrInvalidContract)
	}
	descriptor, ok := fabric.registry.Descriptor(feedRecoveryAdapterKey)
	if !ok || descriptor.MaxRows < 1 || descriptor.MaxRows > normalQuestionBudget.MaxRowsPerDomain || descriptor.DeepLinkBase != "/platform/feed-recovery" {
		return DecisionPacket{}, fmt.Errorf("%w: feed recovery adapter is not safely registered", ErrInvalidContract)
	}
	if err := access.ValidateFor(access.UserID, access.TenantID); err != nil || !access.HasPermission(descriptor.RequiredPermission) {
		return DecisionPacket{}, fmt.Errorf("%w: feed recovery read access is required", ErrAccessUnavailable)
	}
	if _, err := fabric.registry.BoundedTraversal("feed_recovery_plan", access, normalQuestionBudget); err != nil {
		return DecisionPacket{}, err
	}
	startedAt, now := fabric.now(), fabric.now()
	evidence, facts, unknowns := []EvidenceRef{}, []Fact{}, []string{}
	var plan models.FeedRecoveryPlan
	if err := fabric.db.WithContext(ctx).Where("tenant_id=?", access.TenantID).Order("created_at DESC").First(&plan).Error; err == nil {
		ref := fabric.evidence(descriptor, access.TenantID, "feed_recovery_plan:"+plan.PublicID.String(), []SubjectRef{{Type: "feed_recovery_plan", ID: plan.PublicID.String()}}, plan.CreatedAt, plan.UpdatedAt, evidenceAvailabilityForUpdatedAt(plan.UpdatedAt, now), map[string]any{"state": plan.State, "lane": plan.Lane, "level": plan.Level, "expires_at": plan.ExpiresAt, "no_full_rollback": plan.NoFullRollback})
		evidence = append(evidence, ref)
		facts = append(facts, Fact{Key: "feed_recovery.latest_plan", Value: map[string]any{"id": plan.PublicID.String(), "state": plan.State, "lane": plan.Lane, "level": plan.Level, "capacity_mode": plan.CapacityMode, "target_count": plan.TargetCount, "expires_at": plan.ExpiresAt, "no_full_rollback": plan.NoFullRollback, "manual_only": plan.NoFullRollback}, EvidenceIDs: []string{ref.EvidenceID}})
	} else if err == gorm.ErrRecordNotFound {
		unknowns = append(unknowns, "No Feed Recovery plan has been recorded for this tenant.")
	} else {
		return DecisionPacket{}, err
	}
	var run models.FeedRecoveryRun
	if err := fabric.db.WithContext(ctx).Where("tenant_id=?", access.TenantID).Order("created_at DESC").First(&run).Error; err == nil {
		ref := fabric.evidence(descriptor, access.TenantID, "feed_recovery_run:"+run.PublicID.String(), []SubjectRef{{Type: "feed_recovery_run", ID: run.PublicID.String()}}, run.CreatedAt, run.UpdatedAt, evidenceAvailabilityForUpdatedAt(run.UpdatedAt, now), map[string]any{"phase": run.Phase, "lane": run.Lane, "outcome": run.Outcome, "expected_empty": run.ExpectedEmpty})
		evidence = append(evidence, ref)
		facts = append(facts, Fact{Key: "feed_recovery.latest_run", Value: map[string]any{"id": run.PublicID.String(), "lane": run.Lane, "phase": run.Phase, "outcome": run.Outcome, "error": run.Error, "expected_empty": run.ExpectedEmpty, "rollback_deadline": run.RollbackDeadline}, EvidenceIDs: []string{ref.EvidenceID}})
	} else if err == gorm.ErrRecordNotFound {
		unknowns = append(unknowns, "No Feed Recovery run has been recorded for this tenant.")
	} else {
		return DecisionPacket{}, err
	}
	var actions []models.FeedRecoveryAction
	if run.ID != 0 {
		if err := fabric.db.WithContext(ctx).Where("run_id=?", run.ID).Order("created_at DESC").Limit(descriptor.MaxRows).Find(&actions).Error; err != nil {
			return DecisionPacket{}, err
		}
	}
	byState := map[string]int{}
	latestAt := time.Time{}
	for _, action := range actions {
		byState[action.State]++
		if action.UpdatedAt.After(latestAt) {
			latestAt = action.UpdatedAt
		}
	}
	ref := fabric.evidence(descriptor, access.TenantID, "feed_recovery_actions:"+access.TenantID, []SubjectRef{{Type: "feed_recovery_action_collection", ID: access.TenantID}}, latestAt, latestAt, evidenceAvailabilityForUpdatedAt(latestAt, now), map[string]any{"sampled_count": len(actions), "by_state": byState})
	evidence = append(evidence, ref)
	facts = append(facts, Fact{Key: "feed_recovery.actions", Value: map[string]any{"sampled_count": len(actions), "max_rows": descriptor.MaxRows, "truncated": len(actions) == descriptor.MaxRows, "by_state": byState, "destructive_controls_manual_only": true}, EvidenceIDs: []string{ref.EvidenceID}})
	if len(actions) == descriptor.MaxRows {
		unknowns = append(unknowns, "Feed Recovery action results reached the registered read limit; additional history is unknown.")
	}
	var leases []models.FeedRecoveryLaneLease
	if err := fabric.db.WithContext(ctx).Where("tenant_id=?", access.TenantID).Order("lane ASC").Limit(2).Find(&leases).Error; err != nil {
		return DecisionPacket{}, err
	}
	leaseRows := make([]map[string]any, 0, len(leases))
	leaseObservedAt := time.Time{}
	for _, lease := range leases {
		leaseRows = append(leaseRows, map[string]any{"lane": lease.Lane, "expires_at": lease.ExpiresAt, "heartbeat_at": lease.HeartbeatAt, "active": lease.ExpiresAt.After(now)})
		if lease.HeartbeatAt.After(leaseObservedAt) {
			leaseObservedAt = lease.HeartbeatAt
		}
	}
	leaseRef := fabric.evidence(descriptor, access.TenantID, "feed_recovery_leases:"+access.TenantID, []SubjectRef{{Type: "feed_recovery_lease_collection", ID: access.TenantID}}, leaseObservedAt, leaseObservedAt, evidenceAvailabilityForUpdatedAt(leaseObservedAt, now), map[string]any{"sampled_count": len(leaseRows)})
	evidence = append(evidence, leaseRef)
	facts = append(facts, Fact{Key: "feed_recovery.leases", Value: map[string]any{"sampled_count": len(leaseRows), "leases": leaseRows, "max_rows": 2}, EvidenceIDs: []string{leaseRef.EvidenceID}})

	var availability []models.FeedAvailabilityState
	if err := fabric.db.WithContext(ctx).Where("tenant_id=?", access.TenantID).Order("lane ASC").Limit(2).Find(&availability).Error; err != nil {
		return DecisionPacket{}, err
	}
	availabilityRows := make([]map[string]any, 0, len(availability))
	availabilityObservedAt := time.Time{}
	for _, state := range availability {
		availabilityRows = append(availabilityRows, map[string]any{"lane": state.Lane, "state": state.State, "message_key": state.MessageKey, "retry_after_seconds": state.RetryAfterSeconds})
		if state.UpdatedAt.After(availabilityObservedAt) {
			availabilityObservedAt = state.UpdatedAt
		}
	}
	availabilityRef := fabric.evidence(descriptor, access.TenantID, "feed_recovery_availability:"+access.TenantID, []SubjectRef{{Type: "feed_availability_collection", ID: access.TenantID}}, availabilityObservedAt, availabilityObservedAt, evidenceAvailabilityForUpdatedAt(availabilityObservedAt, now), map[string]any{"sampled_count": len(availabilityRows)})
	evidence = append(evidence, availabilityRef)
	facts = append(facts, Fact{Key: "feed_recovery.availability", Value: map[string]any{"sampled_count": len(availabilityRows), "states": availabilityRows, "max_rows": 2}, EvidenceIDs: []string{availabilityRef.EvidenceID}})

	generationIDs := make([]uuid.UUID, 0, 2)
	if run.ActiveGenerationID != nil {
		generationIDs = append(generationIDs, *run.ActiveGenerationID)
	}
	if run.CandidateGenerationID != nil && (run.ActiveGenerationID == nil || *run.CandidateGenerationID != *run.ActiveGenerationID) {
		generationIDs = append(generationIDs, *run.CandidateGenerationID)
	}
	if len(generationIDs) > 0 {
		var generations []models.FeedGeneration
		if err := fabric.db.WithContext(ctx).Where("tenant_id=? AND public_id IN ?", access.TenantID, generationIDs).Order("updated_at DESC").Limit(2).Find(&generations).Error; err != nil {
			return DecisionPacket{}, err
		}
		rows := make([]map[string]any, 0, len(generations))
		generationObservedAt := time.Time{}
		for _, generation := range generations {
			rows = append(rows, map[string]any{"id": generation.PublicID.String(), "lane": generation.Lane, "state": generation.State, "caught_up_at": generation.CaughtUpAt, "cutover_at": generation.CutoverAt, "rollback_deadline": generation.RollbackDeadline, "verification": recoveryVerificationProjection(generation.Verification)})
			if generation.UpdatedAt.After(generationObservedAt) {
				generationObservedAt = generation.UpdatedAt
			}
		}
		generationRef := fabric.evidence(descriptor, access.TenantID, "feed_recovery_generations:"+run.PublicID.String(), []SubjectRef{{Type: "feed_generation_collection", ID: run.PublicID.String()}}, generationObservedAt, generationObservedAt, evidenceAvailabilityForUpdatedAt(generationObservedAt, now), map[string]any{"sampled_count": len(rows)})
		evidence = append(evidence, generationRef)
		facts = append(facts, Fact{Key: "feed_recovery.generations", Value: map[string]any{"sampled_count": len(rows), "generations": rows, "max_rows": 2}, EvidenceIDs: []string{generationRef.EvidenceID}})
	}

	if plan.ID != 0 {
		var artifacts []models.FeedRecoveryArtifact
		if err := fabric.db.WithContext(ctx).Where("plan_id=? AND tenant_id=?", plan.ID, access.TenantID).Order("created_at DESC").Limit(20).Find(&artifacts).Error; err != nil {
			return DecisionPacket{}, err
		}
		artifactRows := make([]map[string]any, 0, len(artifacts))
		artifactObservedAt := time.Time{}
		for _, artifact := range artifacts {
			artifactRows = append(artifactRows, map[string]any{"type": artifact.ArtifactType, "state": artifact.State, "sha256": artifact.SHA256, "byte_size": artifact.ByteSize, "expires_at": artifact.ExpiresAt})
			if artifact.CreatedAt.After(artifactObservedAt) {
				artifactObservedAt = artifact.CreatedAt
			}
		}
		artifactRef := fabric.evidence(descriptor, access.TenantID, "feed_recovery_artifacts:"+plan.PublicID.String(), []SubjectRef{{Type: "feed_recovery_artifact_collection", ID: plan.PublicID.String()}}, artifactObservedAt, artifactObservedAt, evidenceAvailabilityForUpdatedAt(artifactObservedAt, now), map[string]any{"sampled_count": len(artifactRows)})
		evidence = append(evidence, artifactRef)
		facts = append(facts, Fact{Key: "feed_recovery.artifacts", Value: map[string]any{"sampled_count": len(artifactRows), "artifacts": artifactRows, "max_rows": 20}, EvidenceIDs: []string{artifactRef.EvidenceID}})
	}

	for _, action := range actions {
		if !strings.HasPrefix(action.ActionType, "verification_probe_") {
			continue
		}
		verification := recoveryVerificationProjection(action.Evidence)
		verificationRef := fabric.evidence(descriptor, access.TenantID, "feed_recovery_verification:"+action.PublicID.String(), []SubjectRef{{Type: "feed_recovery_action", ID: action.PublicID.String()}}, action.CreatedAt, action.UpdatedAt, evidenceAvailabilityForUpdatedAt(action.UpdatedAt, now), map[string]any{"state": action.State, "action_type": action.ActionType})
		evidence = append(evidence, verificationRef)
		facts = append(facts, Fact{Key: "feed_recovery.verification", Value: map[string]any{"state": action.State, "action_type": action.ActionType, "proof": verification, "includes_integrity_health_page_diversity_playback": true}, EvidenceIDs: []string{verificationRef.EvidenceID}})
		break
	}
	return fabric.packet(visible, access, startedAt, facts, evidence, unknowns)
}

// recoveryVerificationProjection permits only the code-owned recovery proof
// fields needed for an explanation. It deliberately omits any raw target set,
// request credential, or provider/object reference that may exist elsewhere
// in a native destructive workflow.
func recoveryVerificationProjection(raw []byte) map[string]any {
	var decoded map[string]any
	if len(raw) == 0 || json.Unmarshal(raw, &decoded) != nil {
		return map[string]any{"available": false}
	}
	projected := map[string]any{"available": true}
	for _, key := range []string{"feed_integrity_run", "feed_integrity_headline", "system_health_run", "system_health_overall_healthy", "feed_integrity_error", "system_health_error", "latency_clean", "gate_matrix", "inventory", "caught_up", "dual_write_reconciled"} {
		if value, ok := decoded[key]; ok {
			projected[key] = value
		}
	}
	return projected
}

// BuildRetentionStatePacket exposes the current custody posture and bounded
// history without admitting destructive compaction, retirement, maintenance,
// execution-control changes, or owner-request execution into Operator scope.
func (fabric *ContextFabric) BuildRetentionStatePacket(ctx context.Context, visible VisibleContext, access AccessSnapshot) (DecisionPacket, error) {
	if err := visible.Validate(); err != nil {
		return DecisionPacket{}, err
	}
	if err := fabric.registry.Validate(); err != nil {
		return DecisionPacket{}, err
	}
	if visible.Domain != "retention" {
		return DecisionPacket{}, fmt.Errorf("%w: retention requires its canonical visible context", ErrInvalidContract)
	}
	descriptor, ok := fabric.registry.Descriptor(retentionAdapterKey)
	if !ok || descriptor.MaxRows < 1 || descriptor.MaxRows > normalQuestionBudget.MaxRowsPerDomain || descriptor.DeepLinkBase != "/platform/retention" {
		return DecisionPacket{}, fmt.Errorf("%w: retention adapter is not safely registered", ErrInvalidContract)
	}
	if err := access.ValidateFor(access.UserID, access.TenantID); err != nil || !access.HasPermission(descriptor.RequiredPermission) {
		return DecisionPacket{}, fmt.Errorf("%w: retention read access is required", ErrAccessUnavailable)
	}
	if _, err := fabric.registry.BoundedTraversal("retention_policy", access, normalQuestionBudget); err != nil {
		return DecisionPacket{}, err
	}
	startedAt, now := fabric.now(), fabric.now()
	evidence, facts, unknowns := []EvidenceRef{}, []Fact{}, []string{}
	var policy models.RetentionPolicy
	if err := fabric.db.WithContext(ctx).Where("tenant_id=?", access.TenantID).First(&policy).Error; err == nil {
		ref := fabric.evidence(descriptor, access.TenantID, "retention_policy:"+access.TenantID, []SubjectRef{{Type: "retention_policy", ID: policy.PublicID.String()}}, policy.UpdatedAt, policy.UpdatedAt, evidenceAvailabilityForUpdatedAt(policy.UpdatedAt, now), map[string]any{"enabled": policy.Enabled, "mode": policy.Mode, "version": policy.PolicyVersion, "paused_until": policy.PausedUntil})
		evidence = append(evidence, ref)
		facts = append(facts, Fact{Key: "retention.policy", Value: map[string]any{"enabled": policy.Enabled, "mode": policy.Mode, "version": policy.PolicyVersion, "paused_until": policy.PausedUntil, "last_run_at": policy.LastRunAt}, EvidenceIDs: []string{ref.EvidenceID}})
	} else if err == gorm.ErrRecordNotFound {
		unknowns = append(unknowns, "No Retention policy row exists for this tenant.")
	} else {
		return DecisionPacket{}, err
	}
	var sample models.RetentionDBSample
	if err := fabric.db.WithContext(ctx).Where("tenant_id=?", access.TenantID).Order("measured_at DESC").First(&sample).Error; err == nil {
		ref := fabric.evidence(descriptor, access.TenantID, "retention_db_sample:"+sample.PublicID.String(), []SubjectRef{{Type: "retention_db_sample", ID: sample.PublicID.String()}}, sample.MeasuredAt, sample.CreatedAt, evidenceAvailabilityForUpdatedAt(sample.CreatedAt, now), map[string]any{"database_bytes": sample.DatabaseBytes, "allocated_bytes": sample.AllocatedBytes, "reusable_bytes": sample.ReusableBytes})
		evidence = append(evidence, ref)
		facts = append(facts, Fact{Key: "retention.latest_capacity", Value: map[string]any{"id": sample.PublicID.String(), "database_bytes": sample.DatabaseBytes, "allocated_bytes": sample.AllocatedBytes, "reusable_bytes": sample.ReusableBytes, "measured_at": sample.MeasuredAt, "provider_source": sample.ProviderSource}, EvidenceIDs: []string{ref.EvidenceID}})
	} else if err == gorm.ErrRecordNotFound {
		unknowns = append(unknowns, "No Retention capacity sample has been recorded for this tenant.")
	} else {
		return DecisionPacket{}, err
	}
	var runs []models.RetentionRun
	if err := fabric.db.WithContext(ctx).Where("tenant_id=?", access.TenantID).Order("started_at DESC").Limit(descriptor.MaxRows).Find(&runs).Error; err != nil {
		return DecisionPacket{}, err
	}
	byStatus, byVerdict := map[string]int{}, map[string]int{}
	latestAt := time.Time{}
	for _, run := range runs {
		byStatus[run.Status]++
		byVerdict[run.Verdict]++
		if run.UpdatedAt.After(latestAt) {
			latestAt = run.UpdatedAt
		}
	}
	ref := fabric.evidence(descriptor, access.TenantID, "retention_runs:"+access.TenantID, []SubjectRef{{Type: "retention_run_collection", ID: access.TenantID}}, latestAt, latestAt, evidenceAvailabilityForUpdatedAt(latestAt, now), map[string]any{"sampled_count": len(runs), "by_status": byStatus, "by_verdict": byVerdict})
	evidence = append(evidence, ref)
	facts = append(facts, Fact{Key: "retention.runs", Value: map[string]any{"sampled_count": len(runs), "max_rows": descriptor.MaxRows, "truncated": len(runs) == descriptor.MaxRows, "by_status": byStatus, "by_verdict": byVerdict, "destructive_execution_manual_only": true}, EvidenceIDs: []string{ref.EvidenceID}})
	if len(runs) == descriptor.MaxRows {
		unknowns = append(unknowns, "Retention run results reached the registered read limit; additional history is unknown.")
	}
	appendRetentionFact := func(key, recordType, recordID string, observedAt time.Time, payload, value map[string]any) {
		factRef := fabric.evidence(descriptor, access.TenantID, key+":"+recordID, []SubjectRef{{Type: recordType, ID: recordID}}, observedAt, observedAt, evidenceAvailabilityForUpdatedAt(observedAt, now), payload)
		evidence = append(evidence, factRef)
		facts = append(facts, Fact{Key: key, Value: value, EvidenceIDs: []string{factRef.EvidenceID}})
	}

	var controls models.RetentionExecutionControl
	if err := fabric.db.WithContext(ctx).Where("tenant_id=?", access.TenantID).First(&controls).Error; err == nil {
		appendRetentionFact("retention.execution_controls", "retention_execution_control", access.TenantID, controls.UpdatedAt, map[string]any{"updated_at": controls.UpdatedAt}, map[string]any{"canonical_compaction_enabled": controls.CanonicalCompactionEnabled, "historical_enabled": controls.HistoricalEnabled, "owner_runs_enabled": controls.OwnerRunsEnabled, "feed_recovery_rotate_enabled": controls.FeedRecoveryRotateEnabled, "feed_recovery_purge_enabled": controls.FeedRecoveryPurgeEnabled, "operator_cannot_arm_destructive_controls": true})
	} else if err == gorm.ErrRecordNotFound {
		unknowns = append(unknowns, "No Retention execution-control row exists; destructive execution is fail-closed.")
	} else {
		return DecisionPacket{}, err
	}

	var actions []models.RetentionAction
	if err := fabric.db.WithContext(ctx).Where("tenant_id=?", access.TenantID).Order("updated_at DESC").Limit(descriptor.MaxRows).Find(&actions).Error; err != nil {
		return DecisionPacket{}, err
	}
	byClass, byOutcome, breakerCandidates := map[string]int{}, map[string]int{}, map[string]int{}
	actionsObservedAt := time.Time{}
	for _, action := range actions {
		byClass[action.ActionClass]++
		byOutcome[action.Outcome]++
		if action.Outcome == models.RetentionActionToolFailed || action.Outcome == models.RetentionActionVerifyFailed {
			breakerCandidates[action.ActionClass]++
		}
		if action.UpdatedAt.After(actionsObservedAt) {
			actionsObservedAt = action.UpdatedAt
		}
	}
	appendRetentionFact("retention.actions_trust", "retention_action_collection", access.TenantID, actionsObservedAt, map[string]any{"sampled_count": len(actions)}, map[string]any{"sampled_count": len(actions), "max_rows": descriptor.MaxRows, "by_class": byClass, "by_outcome": byOutcome, "failure_counts_for_breaker_review": breakerCandidates, "operator_cannot_reset_or_arm_breakers": true})

	var holds []models.RetentionHold
	if err := fabric.db.WithContext(ctx).Where("tenant_id=? AND released_at IS NULL", access.TenantID).Order("updated_at DESC").Limit(descriptor.MaxRows).Find(&holds).Error; err != nil {
		return DecisionPacket{}, err
	}
	holdsByType, holdsByClass := map[string]int{}, map[string]int{}
	holdsObservedAt := time.Time{}
	for _, hold := range holds {
		holdsByType[hold.TargetType]++
		holdsByClass[hold.HoldClass]++
		if hold.UpdatedAt.After(holdsObservedAt) {
			holdsObservedAt = hold.UpdatedAt
		}
	}
	appendRetentionFact("retention.holds", "retention_hold_collection", access.TenantID, holdsObservedAt, map[string]any{"active_count": len(holds)}, map[string]any{"active_count": len(holds), "max_rows": descriptor.MaxRows, "by_target_type": holdsByType, "by_hold_class": holdsByClass, "target_ids_withheld": true})

	var compaction []models.RetentionCompactionManifest
	if err := fabric.db.WithContext(ctx).Where("tenant_id=?", access.TenantID).Order("updated_at DESC").Limit(20).Find(&compaction).Error; err != nil {
		return DecisionPacket{}, err
	}
	compactionRows := make([]map[string]any, 0, len(compaction))
	compactionObservedAt := time.Time{}
	for _, manifest := range compaction {
		compactionRows = append(compactionRows, map[string]any{"id": manifest.PublicID.String(), "state": manifest.State, "story_count": manifest.StoryCount, "anchor_count": manifest.AnchorCount, "protected_count": manifest.ProtectedCount, "retire_count": manifest.RetireCount, "estimated_bytes": manifest.EstimatedBytes, "expires_at": manifest.ExpiresAt})
		if manifest.UpdatedAt.After(compactionObservedAt) {
			compactionObservedAt = manifest.UpdatedAt
		}
	}
	appendRetentionFact("retention.compaction_manifests", "retention_compaction_manifest_collection", access.TenantID, compactionObservedAt, map[string]any{"sampled_count": len(compactionRows)}, map[string]any{"sampled_count": len(compactionRows), "max_rows": 20, "manifests": compactionRows, "target_ids_withheld": true, "operator_execution_manual_only": true})

	var historical []models.RetentionHistoricalManifest
	if err := fabric.db.WithContext(ctx).Where("tenant_id=?", access.TenantID).Order("updated_at DESC").Limit(20).Find(&historical).Error; err != nil {
		return DecisionPacket{}, err
	}
	historicalRows := make([]map[string]any, 0, len(historical))
	historicalObservedAt := time.Time{}
	for _, manifest := range historical {
		historicalRows = append(historicalRows, map[string]any{"id": manifest.PublicID.String(), "state": manifest.State, "content_count": manifest.ContentCount, "story_count": manifest.StoryCount, "estimated_bytes": manifest.EstimatedBytes, "expires_at": manifest.ExpiresAt})
		if manifest.UpdatedAt.After(historicalObservedAt) {
			historicalObservedAt = manifest.UpdatedAt
		}
	}
	appendRetentionFact("retention.historical_manifests", "retention_historical_manifest_collection", access.TenantID, historicalObservedAt, map[string]any{"sampled_count": len(historicalRows)}, map[string]any{"sampled_count": len(historicalRows), "max_rows": 20, "manifests": historicalRows, "target_ids_withheld": true, "historical_retirement_manual_only": true})

	var recoveryArtifacts []models.RetentionRecoveryArtifact
	if err := fabric.db.WithContext(ctx).Where("tenant_id=?", access.TenantID).Order("updated_at DESC").Limit(20).Find(&recoveryArtifacts).Error; err != nil {
		return DecisionPacket{}, err
	}
	var historicalArtifacts []models.RetentionHistoricalRecoveryArtifact
	if err := fabric.db.WithContext(ctx).Where("tenant_id=?", access.TenantID).Order("updated_at DESC").Limit(20).Find(&historicalArtifacts).Error; err != nil {
		return DecisionPacket{}, err
	}
	artifactRows := make([]map[string]any, 0, len(recoveryArtifacts)+len(historicalArtifacts))
	artifactObservedAt := time.Time{}
	for _, artifact := range recoveryArtifacts {
		artifactRows = append(artifactRows, map[string]any{"family": "compaction", "state": artifact.State, "sha256": artifact.SHA256, "compressed_bytes": artifact.CompressedBytes, "expires_at": artifact.ExpiresAt, "verified_at": artifact.VerifiedAt})
		if artifact.UpdatedAt.After(artifactObservedAt) {
			artifactObservedAt = artifact.UpdatedAt
		}
	}
	for _, artifact := range historicalArtifacts {
		artifactRows = append(artifactRows, map[string]any{"family": "historical", "state": artifact.State, "sha256": artifact.SHA256, "compressed_bytes": artifact.CompressedBytes, "expires_at": artifact.ExpiresAt, "verified_at": artifact.VerifiedAt})
		if artifact.UpdatedAt.After(artifactObservedAt) {
			artifactObservedAt = artifact.UpdatedAt
		}
	}
	appendRetentionFact("retention.recovery_artifacts", "retention_recovery_artifact_collection", access.TenantID, artifactObservedAt, map[string]any{"sampled_count": len(artifactRows)}, map[string]any{"sampled_count": len(artifactRows), "max_rows_per_family": 20, "artifacts": artifactRows, "artifact_keys_withheld": true})

	var archives []models.NewsMonthArchive
	if err := fabric.db.WithContext(ctx).Where("tenant_id=?", access.TenantID).Order("month_start DESC, revision DESC").Limit(12).Find(&archives).Error; err != nil {
		return DecisionPacket{}, err
	}
	archiveRows := make([]map[string]any, 0, len(archives))
	archiveObservedAt := time.Time{}
	for _, archive := range archives {
		archiveRows = append(archiveRows, map[string]any{"id": archive.PublicID.String(), "month_start": archive.MonthStart, "state": archive.State, "revision": archive.Revision, "limited_coverage": archive.LimitedCoverage, "qualified_count": archive.QualifiedCount, "selected_count": archive.SelectedCount, "verified_at": archive.VerifiedAt, "finalized_at": archive.FinalizedAt})
		if archive.UpdatedAt.After(archiveObservedAt) {
			archiveObservedAt = archive.UpdatedAt
		}
	}
	appendRetentionFact("retention.monthly_archives", "news_month_archive_collection", access.TenantID, archiveObservedAt, map[string]any{"sampled_count": len(archiveRows)}, map[string]any{"sampled_count": len(archiveRows), "max_rows": 12, "archives": archiveRows, "operator_may_explain_not_mutate_archive": true})

	var ownerRequests []models.RetentionOwnerRequest
	if err := fabric.db.WithContext(ctx).Where("tenant_id=?", access.TenantID).Order("updated_at DESC").Limit(20).Find(&ownerRequests).Error; err != nil {
		return DecisionPacket{}, err
	}
	ownerRows := make([]map[string]any, 0, len(ownerRequests))
	ownerObservedAt := time.Time{}
	for _, request := range ownerRequests {
		ownerRows = append(ownerRows, map[string]any{"id": request.PublicID.String(), "owner_system": request.OwnerSystem, "status": request.Status, "max_bytes": request.MaxBytes, "max_items": request.MaxItems, "max_actions": request.MaxActions, "correlation_id": request.CorrelationID, "expires_at": request.ExpiresAt, "result_hash": request.ResultHash})
		if request.UpdatedAt.After(ownerObservedAt) {
			ownerObservedAt = request.UpdatedAt
		}
	}
	appendRetentionFact("retention.owner_requests", "retention_owner_request_collection", access.TenantID, ownerObservedAt, map[string]any{"sampled_count": len(ownerRows)}, map[string]any{"sampled_count": len(ownerRows), "max_rows": 20, "requests": ownerRows, "owner_execution_manual_only": true})

	var maintenance models.RetentionMaintenanceReport
	if err := fabric.db.WithContext(ctx).Where("tenant_id=?", access.TenantID).Order("created_at DESC").First(&maintenance).Error; err == nil {
		appendRetentionFact("retention.maintenance", "retention_maintenance_report", maintenance.PublicID.String(), maintenance.CreatedAt, map[string]any{"state": maintenance.State}, map[string]any{"state": maintenance.State, "database_bytes": maintenance.DatabaseBytes, "target_bytes": maintenance.TargetBytes, "provider_bytes": maintenance.ProviderBytes, "provider_fresh": maintenance.ProviderFresh, "postgres_ready": maintenance.PostgresReady, "provider_ready": maintenance.ProviderReady, "physical_rewrite_manual_only": true})
	} else if err != gorm.ErrRecordNotFound {
		return DecisionPacket{}, err
	}
	return fabric.packet(visible, access, startedAt, facts, evidence, unknowns)
}

func (fabric *ContextFabric) packet(visible VisibleContext, access AccessSnapshot, startedAt time.Time, facts []Fact, evidence []EvidenceRef, unknowns []string) (DecisionPacket, error) {
	packet := DecisionPacket{SchemaVersion: ContractVersion, PacketID: uuid.NewString(), TenantID: access.TenantID, ActorID: access.UserID, VisibleContext: visible, CollectionStartedAt: startedAt, CollectionEndedAt: fabric.now(), Completeness: "complete", Facts: facts, Evidence: evidence, Recommendations: deterministicRecommendations(visible, evidence, unknowns), Unknowns: unknowns}
	packet.Fingerprint = fingerprintPacket(packet)
	if err := packet.Validate(); err != nil {
		return DecisionPacket{}, err
	}
	return packet, nil
}

// deterministicRecommendations return only registered Console destinations.
// They explain the next safe inspection, never create a mutation or a
// model-defined URL.
func deterministicRecommendations(visible VisibleContext, evidence []EvidenceRef, unknowns []string) []Recommendation {
	if len(evidence) == 0 {
		return nil
	}
	first := evidence[0]
	title, summary := "Inspect the registered operational surface", "Review the current CMS evidence before deciding on a change."
	manualOnly := false
	switch visible.Domain {
	case "media_sources":
		title, summary = "Review approval handoff evidence", "Inspect the source, its durable run request, and processing lineage in Media Sources."
	case "media_circulation":
		title, summary = "Review circulation recommendations", "Inspect current policy, the latest recorded run, and the bounded recommendation backlog."
	case "feed_integrity":
		title, summary = "Review the News snapshot", "Inspect the live snapshot state and, when appropriate, prepare the registered refresh action."
	case "feed_recovery":
		title, summary, manualOnly = "Review Feed Recovery controls", "Inspect the latest plan and run. Destructive recovery operations remain manual-only.", true
	case "retention":
		title, summary, manualOnly = "Review Retention custody", "Inspect the current capacity and policy evidence. Destructive custody operations remain manual-only.", true
	}
	if len(unknowns) > 0 {
		summary += " Some required state is unknown and must not be inferred."
	}
	return []Recommendation{{ID: "inspect:" + visible.Domain, Kind: "inspect", Title: title, Summary: summary, DeepLink: first.DeepLink, EvidenceIDs: []string{first.EvidenceID}, ManualOnly: manualOnly}}
}

func (registry AdapterRegistry) Descriptor(key string) (AdapterDescriptor, bool) {
	descriptor, ok := registry.adapters[key]
	return descriptor, ok
}

func (registry AdapterRegistry) EdgesFrom(kind string) []RelationshipEdge {
	return append([]RelationshipEdge(nil), registry.edges[kind]...)
}

type ContextFabric struct {
	db       *gorm.DB
	registry AdapterRegistry
	now      func() time.Time
	memory   *MemoryStore
	embedder MemoryEmbedder
}

func NewContextFabric(db *gorm.DB, registry AdapterRegistry) *ContextFabric {
	return &ContextFabric{db: db, registry: registry, now: func() time.Time { return time.Now().UTC() }}
}

func (fabric *ContextFabric) WithMemoryRetrieval(store *MemoryStore, embedder MemoryEmbedder) *ContextFabric {
	fabric.memory, fabric.embedder = store, embedder
	return fabric
}

func (fabric *ContextFabric) AttachHistoricalRetrieval(ctx context.Context, packet *DecisionPacket, access AccessSnapshot, question string) error {
	if fabric.memory == nil || fabric.embedder == nil || strings.TrimSpace(question) == "" {
		return nil
	}
	hits, _, err := fabric.memory.Retrieve(ctx, access, question, fabric.embedder)
	if err != nil {
		return err
	}
	AttachRetrievedMemory(packet, hits, fabric.now())
	packet.Fingerprint = fingerprintPacket(*packet)
	return packet.Validate()
}

// BuildPacket selects only a registered adapter for the canonical Console
// domain. Callers never choose database tables, joins, or deep-link URLs.
func (fabric *ContextFabric) BuildPacket(ctx context.Context, visible VisibleContext, access AccessSnapshot) (DecisionPacket, error) {
	if err := fabric.ensureVisibleAdapterEnabled(visible, access.TenantID); err != nil {
		return DecisionPacket{}, err
	}
	switch visible.Domain {
	case "media_sources":
		for _, subject := range visible.Subjects {
			if subject.Type == "content_source" || subject.Type == "source" {
				return fabric.BuildApprovalHandoffPacket(ctx, visible, access)
			}
		}
		return fabric.BuildCoveragePacket(ctx, visible, access)
	case "media_circulation":
		return fabric.BuildMediaCirculationStatePacket(ctx, visible, access)
	case "feed_integrity":
		return fabric.BuildFeedIntegritySnapshotPacket(ctx, visible, access)
	case "feed_recovery":
		return fabric.BuildFeedRecoveryStatePacket(ctx, visible, access)
	case "retention":
		return fabric.BuildRetentionStatePacket(ctx, visible, access)
	default:
		return fabric.BuildCoveragePacket(ctx, visible, access)
	}
}

// ensureVisibleAdapterEnabled applies tenant disable-only controls to every
// read path, including interactive, scheduled, and shadow investigations.
// A disabled adapter is never silently bypassed by a more generic coverage
// projection.
func (fabric *ContextFabric) ensureVisibleAdapterEnabled(visible VisibleContext, tenantID string) error {
	key := AdapterKeyForVisibleContext(visible)
	return EnsureAdapterCapabilityEnabled(fabric.db, tenantID, key, fabric.now())
}

// AdapterKeyForVisibleContext returns the exact registered read capability
// that backs a typed Console context. Schedulers use the same key as live
// investigations so adapter stops are consistent across execution modes.
func AdapterKeyForVisibleContext(visible VisibleContext) string {
	key := coverageAdapterKey(visible.Domain)
	switch visible.Domain {
	case "media_sources":
		for _, subject := range visible.Subjects {
			if subject.Type == "content_source" || subject.Type == "source" {
				key = approvalHandoffAdapterKey
				break
			}
		}
	case "media_circulation":
		key = mediaCirculationAdapterKey
	case "feed_integrity":
		key = feedIntegrityAdapterKey
	case "feed_recovery":
		key = feedRecoveryAdapterKey
	case "retention":
		key = retentionAdapterKey
	}
	return key
}

// BuildFeedIntegritySnapshotPacket owns the single bounded, reversible action
// context currently admitted to the Operator catalog. The live database read
// proves snapshot state; it never treats the Console's cached status as fact.
func (fabric *ContextFabric) BuildFeedIntegritySnapshotPacket(ctx context.Context, visible VisibleContext, access AccessSnapshot) (DecisionPacket, error) {
	if err := visible.Validate(); err != nil {
		return DecisionPacket{}, err
	}
	if err := fabric.registry.Validate(); err != nil {
		return DecisionPacket{}, err
	}
	if visible.Domain != "feed_integrity" {
		return DecisionPacket{}, fmt.Errorf("%w: feed integrity requires its canonical visible context", ErrInvalidContract)
	}
	descriptor, ok := fabric.registry.Descriptor(feedIntegrityAdapterKey)
	if !ok || descriptor.MaxRows != 1 || descriptor.DeepLinkBase != "/platform/feed-integrity" {
		return DecisionPacket{}, fmt.Errorf("%w: feed integrity adapter is not safely registered", ErrInvalidContract)
	}
	if err := access.ValidateFor(access.UserID, access.TenantID); err != nil || !access.HasPermission(descriptor.RequiredPermission) {
		return DecisionPacket{}, fmt.Errorf("%w: feed integrity read access is required", ErrAccessUnavailable)
	}
	window, err := feedIntegrityWindowFromVisibleContext(visible)
	if err != nil {
		return DecisionPacket{}, err
	}
	startedAt, now := fabric.now(), fabric.now()
	var snapshot models.NewsSnapshot
	if err := fabric.db.WithContext(ctx).Where("tenant_id=? AND \"window\"=?", access.TenantID, window).First(&snapshot).Error; err == nil {
		ref := fabric.evidence(descriptor, access.TenantID, "news_snapshot:"+access.TenantID+":"+window, []SubjectRef{{Type: "news_window", ID: window}}, snapshot.BuiltAt, snapshot.BuiltAt, EvidenceAvailable, map[string]any{"window": snapshot.Window, "slide_count": snapshot.SlideCount, "dirty": snapshot.Dirty, "generation": snapshot.Generation, "observed_at": now})
		facts := []Fact{{Key: "feed_integrity.news_snapshot", Value: map[string]any{"window": snapshot.Window, "slide_count": snapshot.SlideCount, "dirty": snapshot.Dirty, "generation": snapshot.Generation, "built_at": snapshot.BuiltAt, "age_seconds": int(now.Sub(snapshot.BuiltAt).Seconds())}, EvidenceIDs: []string{ref.EvidenceID}}}
		return fabric.packet(visible, access, startedAt, facts, []EvidenceRef{ref}, nil)
	} else if err == gorm.ErrRecordNotFound {
		// The missing row is still a live, concrete observation; it is enough to
		// create a bounded refresh plan but explicitly describes the absence.
		ref := fabric.evidence(descriptor, access.TenantID, "news_snapshot:"+access.TenantID+":"+window, []SubjectRef{{Type: "news_window", ID: window}}, now, now, EvidenceAvailable, map[string]any{"window": window, "exists": false})
		facts := []Fact{{Key: "feed_integrity.news_snapshot", Value: map[string]any{"window": window, "exists": false, "dirty": true}, EvidenceIDs: []string{ref.EvidenceID}}}
		return fabric.packet(visible, access, startedAt, facts, []EvidenceRef{ref}, []string{"No cached News snapshot exists for this window; CMS can refresh it, but no cache state can be compared yet."})
	} else {
		return DecisionPacket{}, err
	}
}

func feedIntegrityWindowFromVisibleContext(visible VisibleContext) (string, error) {
	var window string
	for _, subject := range visible.Subjects {
		if subject.Type == "news_window" {
			window = strings.ToLower(strings.TrimSpace(subject.ID))
			break
		}
	}
	if window != "today" && window != "week" && window != "month" {
		return "", fmt.Errorf("%w: feed integrity requires a registered News window", ErrInvalidContract)
	}
	if visible.Selection == nil || visible.Selection.Mode != "explicit" || visible.Selection.Count != 1 || len(visible.Selection.IDs) != 1 || visible.Selection.IDs[0] != window {
		return "", fmt.Errorf("%w: feed integrity action context requires one explicit matching window", ErrInvalidContract)
	}
	return window, nil
}

// BuildApprovalHandoffPacket creates the deterministic source-approval
// investigation packet. It is intentionally CMS-only and never observes or
// queries BullMQ; missing request acceptance remains an explicit unknown.
func (fabric *ContextFabric) BuildApprovalHandoffPacket(ctx context.Context, visible VisibleContext, access AccessSnapshot) (DecisionPacket, error) {
	if err := visible.Validate(); err != nil {
		return DecisionPacket{}, err
	}
	if err := fabric.registry.Validate(); err != nil {
		return DecisionPacket{}, err
	}
	descriptor, ok := fabric.registry.Descriptor(approvalHandoffAdapterKey)
	if !ok || descriptor.MaxRows < 1 || descriptor.MaxRows > normalQuestionBudget.MaxRowsPerDomain || descriptor.DeepLinkBase != "/platform/media/sources" {
		return DecisionPacket{}, fmt.Errorf("%w: approval handoff adapter is not safely registered", ErrInvalidContract)
	}
	if err := access.ValidateFor(access.UserID, access.TenantID); err != nil || !access.HasPermission(descriptor.RequiredPermission) {
		return DecisionPacket{}, fmt.Errorf("%w: source read access is required", ErrAccessUnavailable)
	}
	if _, err := fabric.registry.BoundedTraversal("content_source", access, normalQuestionBudget); err != nil {
		return DecisionPacket{}, err
	}
	sourceID, err := sourceIDFromVisibleContext(visible)
	if err != nil {
		return DecisionPacket{}, err
	}
	startedAt := fabric.now()
	var source models.ContentSource
	if err := fabric.db.WithContext(ctx).Where("public_id=? AND tenant_id=?", sourceID, access.TenantID).First(&source).Error; err != nil {
		return DecisionPacket{}, err
	}

	now := fabric.now()
	evidence := make([]EvidenceRef, 0, 4)
	facts := make([]Fact, 0, 6)
	sourceEvidence := fabric.evidence(descriptor, access.TenantID, "content_source:"+source.PublicID.String(), []SubjectRef{{Type: "content_source", ID: source.PublicID.String(), Label: source.Name}}, source.UpdatedAt, source.UpdatedAt, EvidenceAvailable, map[string]any{"source_id": source.PublicID.String(), "active": source.IsActive})
	evidence = append(evidence, sourceEvidence)
	facts = append(facts, Fact{Key: "approval_handoff.source", Value: map[string]any{"id": source.PublicID.String(), "name": source.Name, "active": source.IsActive, "category": source.Category}, EvidenceIDs: []string{sourceEvidence.EvidenceID}})

	var run models.SourceRunRequest
	err = fabric.db.WithContext(ctx).
		Where("tenant_id=? AND content_source_id=?", access.TenantID, source.PublicID).
		Order("requested_at DESC").Limit(1).First(&run).Error
	unknowns := []string{}
	if err == gorm.ErrRecordNotFound {
		unknowns = append(unknowns, "No CMS source-run request exists for this source; the system cannot prove that Aggregation accepted or queued a handoff.")
		facts = append(facts, Fact{Key: "approval_handoff.queue_proven", Value: false, EvidenceIDs: []string{sourceEvidence.EvidenceID}})
	} else if err != nil {
		return DecisionPacket{}, err
	} else {
		runEvidence := fabric.evidence(descriptor, access.TenantID, "source_run_request:"+run.PublicID.String(), []SubjectRef{{Type: "source_run_request", ID: run.PublicID.String()}, {Type: "content_source", ID: source.PublicID.String(), Label: source.Name}}, run.RequestedAt, run.UpdatedAt, evidenceAvailabilityForRun(run, now), map[string]any{"state": run.State, "job_id": run.AggregationJobID, "correlation_id": run.CorrelationID})
		evidence = append(evidence, runEvidence)
		facts = append(facts,
			Fact{Key: "approval_handoff.request", Value: map[string]any{"id": run.PublicID.String(), "state": run.State, "requested_by": run.RequestedBy, "requested_at": run.RequestedAt, "accepted_at": run.AcceptedAt, "aggregation_job_id": run.AggregationJobID, "failure_class": run.FailureClass}, EvidenceIDs: []string{runEvidence.EvidenceID}},
			Fact{Key: "approval_handoff.queue_proven", Value: run.State != models.SourceRunRequested, EvidenceIDs: []string{runEvidence.EvidenceID}},
		)
		var events []models.ContentProcessingEvent
		if err := fabric.db.WithContext(ctx).Where("tenant_id=? AND source_run_request_id=?", access.TenantID, run.ID).Order("occurred_at DESC").Limit(descriptor.MaxRows).Find(&events).Error; err != nil {
			return DecisionPacket{}, err
		}
		if len(events) > 0 {
			latest := events[0]
			eventEvidence := fabric.evidence(descriptor, access.TenantID, "content_processing_event:"+latest.PublicID.String(), []SubjectRef{{Type: "content_processing_event", ID: latest.PublicID.String()}, {Type: "source_run_request", ID: run.PublicID.String()}}, latest.OccurredAt, latest.CreatedAt, EvidenceAvailable, map[string]any{"stage": latest.Stage, "state": latest.State, "event_class": latest.EventClass})
			evidence = append(evidence, eventEvidence)
			facts = append(facts, Fact{Key: "approval_handoff.latest_processing", Value: map[string]any{"stage": latest.Stage, "state": latest.State, "event_class": latest.EventClass, "occurred_at": latest.OccurredAt}, EvidenceIDs: []string{eventEvidence.EvidenceID}})
		}
	}
	var first models.ContentItem
	contentQuery := fabric.db.WithContext(ctx).Where("tenant_id=? AND content_source_id=?", access.TenantID, source.PublicID)
	if err := contentQuery.Order("created_at ASC").First(&first).Error; err == nil {
		itemEvidence := fabric.evidence(descriptor, access.TenantID, "content_item:"+first.PublicID.String(), []SubjectRef{{Type: "content_item", ID: first.PublicID.String()}}, first.CreatedAt, first.UpdatedAt, EvidenceAvailable, map[string]any{"status": first.Status, "has_transcript": first.TranscriptID != nil, "has_embedding": first.Embedding != nil})
		evidence = append(evidence, itemEvidence)
		facts = append(facts, Fact{Key: "approval_handoff.first_item", Value: map[string]any{"id": first.PublicID.String(), "status": first.Status, "created_at": first.CreatedAt, "transcript_ready": first.TranscriptID != nil, "embedding_ready": first.Embedding != nil}, EvidenceIDs: []string{itemEvidence.EvidenceID}})
	} else if err == gorm.ErrRecordNotFound {
		unknowns = append(unknowns, "No content item has been recorded for this source yet.")
	} else {
		return DecisionPacket{}, err
	}

	return fabric.packet(visible, access, startedAt, facts, evidence, unknowns)
}

func sourceIDFromVisibleContext(visible VisibleContext) (uuid.UUID, error) {
	for _, subject := range visible.Subjects {
		if subject.Type == "content_source" || subject.Type == "source" {
			id, err := uuid.Parse(strings.TrimSpace(subject.ID))
			if err != nil {
				return uuid.Nil, fmt.Errorf("%w: source subject must be a UUID", ErrInvalidContract)
			}
			return id, nil
		}
	}
	return uuid.Nil, fmt.Errorf("%w: approval handoff requires an explicit content_source subject", ErrInvalidContract)
}

func (fabric *ContextFabric) evidence(descriptor AdapterDescriptor, tenantID, evidenceID string, refs []SubjectRef, observedAt, sourceUpdatedAt time.Time, availability EvidenceAvailability, value any) EvidenceRef {
	fetchedAt := fabric.now()
	if observedAt.IsZero() {
		observedAt = fetchedAt
	}
	if sourceUpdatedAt.IsZero() {
		sourceUpdatedAt = observedAt
	}
	return EvidenceRef{EvidenceID: evidenceID, Authority: EvidenceLive, Domain: descriptor.Domain, AdapterKey: descriptor.Key, AdapterVersion: descriptor.Version, TenantID: tenantID, RequiredPermission: descriptor.RequiredPermission, RecordRefs: refs, DeepLink: descriptor.DeepLinkBase, ObservedAt: observedAt, FetchedAt: fetchedAt, MaxAgeSeconds: int(descriptor.MaxAge.Seconds()), ExpiresAt: fetchedAt.Add(descriptor.MaxAge), ContentHash: fingerprintValue(value), SourceVersion: sourceUpdatedAt.UTC().Format(time.RFC3339Nano), Availability: availability}
}

func (fabric *ContextFabric) temporalEvidence(descriptor AdapterDescriptor, tenantID, evidenceID string, refs []SubjectRef, summary TemporalSummary, fetchedAt time.Time) EvidenceRef {
	return EvidenceRef{EvidenceID: evidenceID, Authority: EvidenceTemporal, Domain: descriptor.Domain, AdapterKey: descriptor.Key, AdapterVersion: descriptor.Version, TenantID: tenantID, RequiredPermission: descriptor.RequiredPermission, RecordRefs: refs, DeepLink: descriptor.DeepLinkBase, ObservedAt: summary.WindowEnd, FetchedAt: fetchedAt, MaxAgeSeconds: int(descriptor.MaxAge.Seconds()), ExpiresAt: fetchedAt.Add(descriptor.MaxAge), ContentHash: fingerprintValue(summary), SourceVersion: summary.WindowEnd.UTC().Format(time.RFC3339Nano), Availability: EvidenceAvailable}
}

func evidenceAvailabilityForRun(run models.SourceRunRequest, now time.Time) EvidenceAvailability {
	if run.State == models.SourceRunFailed {
		return EvidencePartial
	}
	if !run.UpdatedAt.IsZero() && now.Sub(run.UpdatedAt) > operationalEvidenceMaxAge {
		return EvidenceStale
	}
	return EvidenceAvailable
}

func evidenceAvailabilityForUpdatedAt(updatedAt, now time.Time) EvidenceAvailability {
	if updatedAt.IsZero() {
		return EvidenceUnavailable
	}
	if now.Sub(updatedAt) > operationalEvidenceMaxAge {
		return EvidenceStale
	}
	return EvidenceAvailable
}

func fingerprintPacket(packet DecisionPacket) string {
	copy := packet
	copy.PacketID = ""
	copy.Fingerprint = ""
	copy.CollectionStartedAt = time.Time{}
	copy.CollectionEndedAt = time.Time{}
	return fingerprintValue(copy)
}

func fingerprintValue(value any) string {
	raw, _ := json.Marshal(value)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func sortedFactKeys(facts []Fact) []string {
	keys := make([]string, 0, len(facts))
	for _, fact := range facts {
		keys = append(keys, fact.Key)
	}
	sort.Strings(keys)
	return keys
}
