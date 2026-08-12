package operator

import (
	"fmt"
	"sort"
	"strings"

	"content-management-system/src/models"
	"content-management-system/src/supply"

	"github.com/google/uuid"
)

// ToolCatalog is a static, code-owned admission list. Database controls can
// disable an admitted key later, but no database row, browser request, or LLM
// response can create a tool or expand its arguments.
type ToolCatalog struct {
	tools map[string]ToolDescriptor
}

func DefaultToolCatalog() ToolCatalog {
	refreshSnapshot := ToolDescriptor{
		Key: "feed_integrity.refresh_snapshot", Version: "v1", OwnerDomain: "feed_integrity",
		TargetType: "feed_window", ArgumentSchema: "feed-window-v1", RequiredPermission: "feed:manage",
		RiskTier: RiskRoutine, Batchable: false, TargetCap: 1, Executor: "feed_integrity.refresh_snapshot",
		Monitor: "feed_integrity.action_monitor", Verifier: "feed_integrity.freshness_verify",
		Idempotency: "operator_plan_step", Cancellation: "before_start_only", Rollback: "not_required_idempotent_refresh",
		Contingencies:   []string{"snapshot_refresh_failed:stop_and_record_failure", "verification_failed:stop_and_record_failure"},
		AffectedDomains: []string{"feed_integrity", "news"}, LocalizedActionKey: "operator.action.refresh_snapshot",
	}
	sourceRun := ToolDescriptor{
		Key: "sources.run_once", Version: "v1", OwnerDomain: "sources",
		TargetType: "content_source", ArgumentSchema: "content-source-id-v1", RequiredPermission: "source:write",
		RiskTier: RiskRoutine, Batchable: false, TargetCap: 1, Executor: "sources.run_once",
		Monitor: "source_lineage.request_monitor", Verifier: "source_lineage.acceptance_verify",
		Idempotency: "operator_plan_step", Cancellation: "before_start_only", Rollback: "not_required_idempotent_handoff",
		Contingencies:   []string{"source_preflight_changed:stop_and_record_failure", "aggregation_handoff_failed:stop_and_record_failure", "acceptance_record_failed:stop_and_record_failure"},
		AffectedDomains: []string{"sources", "pipeline"}, LocalizedActionKey: "operator.action.run_source",
	}
	mediaSourceRun := sourceRun
	mediaSourceRun.Key, mediaSourceRun.OwnerDomain, mediaSourceRun.Executor = "media_sources.run_once", "media_sources", "media_sources.run_once"
	mediaSourceRun.AffectedDomains = []string{"media_sources", "media_circulation", "atomization"}
	mediaSourceRun.LocalizedActionKey = "operator.action.run_media_source"
	tools := map[string]ToolDescriptor{
		refreshSnapshot.Key: refreshSnapshot,
		sourceRun.Key:       sourceRun,
		mediaSourceRun.Key:  mediaSourceRun,
	}
	// This is the Supply evaluator's only Operator-admitted mutation. It is a
	// durable, tenant-scoped emergency brake for new evidence recording;
	// observations, verification, cancellation, and safe episode resolution
	// remain available. It can never resume or otherwise widen authority.
	tools["media_circulation.supply.disable_evaluator"] = ToolDescriptor{
		Key: "media_circulation.supply.disable_evaluator", Version: "v1", OwnerDomain: "media_circulation",
		TargetType: "media_supply_evaluator", ArgumentSchema: "fixed-current-media-supply-evaluator-v1", RequiredPermission: "aggregation:manage",
		RiskTier: RiskHigh, Batchable: false, TargetCap: 1, Executor: "media_circulation.supply.disable_evaluator",
		Monitor: "media_supply.evaluator_monitor", Verifier: "media_supply.evaluator_control_verify", Idempotency: "operator_plan_step",
		Cancellation: "before_start_only", Rollback: "disable_only",
		Contingencies:   []string{"control_already_disabled:verify_current_state", "control_write_failed:stop_and_record_failure", "verification_failed:stop_and_record_failure"},
		AffectedDomains: []string{"media_circulation"}, LocalizedActionKey: "operator.action.media_circulation_supply_disable_evaluator",
	}
	for _, key := range OperatorSupplyRecoveryToolKeys() {
		native, ok := supply.SupplyAction(key)
		if !ok {
			continue
		}
		risk := RiskRoutine
		if native.Risk == "high" {
			risk = RiskHigh
		}
		tools[key] = ToolDescriptor{
			Key: key, Version: "v1", OwnerDomain: "media_circulation", TargetType: "media_supply_episode",
			ArgumentSchema: "media-supply-episode-uuid-v1", RequiredPermission: "aggregation:manage", RiskTier: risk,
			Batchable: false, TargetCap: 1, Executor: key, Monitor: "media_supply.action_monitor", Verifier: "media_supply.signed_handoff_verify",
			Idempotency: "operator_plan_step", Cancellation: "before_start_only", Rollback: native.Rollback,
			Contingencies:   []string{"episode_or_target_changed:stop_and_record_failure", "native_action_disabled:stop_and_record_failure", "durable_handoff_failed:stop_and_record_failure"},
			AffectedDomains: append([]string(nil), native.AffectedDomains...), LocalizedActionKey: "operator.action." + strings.ReplaceAll(key, ".", "_"),
		}
	}
	for _, spec := range []struct{ key, domain, category, rollback string }{
		{"sources.pause", "sources", "news", "sources.resume"},
		{"sources.resume", "sources", "news", "sources.pause"},
		{"media_sources.pause", "media_sources", "media", "media_sources.resume"},
		{"media_sources.resume", "media_sources", "media", "media_sources.pause"},
	} {
		tools[spec.key] = ToolDescriptor{Key: spec.key, Version: "v1", OwnerDomain: spec.domain, TargetType: spec.domain + "_source", ArgumentSchema: "content-source-uuid-v1", RequiredPermission: "source:write", RiskTier: RiskRoutine, Batchable: false, TargetCap: 1, Executor: spec.key, Monitor: spec.domain + ".source_monitor", Verifier: spec.domain + ".source_verify", Idempotency: "operator_plan_step", Cancellation: "before_start_only", Rollback: spec.rollback, Contingencies: []string{"source_missing_or_category_changed:stop_and_record_failure", "verification_failed:stop_and_record_failure"}, AffectedDomains: []string{spec.domain, "pipeline"}, LocalizedActionKey: "operator.action." + strings.ReplaceAll(spec.key, ".", "_")}
	}
	for _, descriptor := range []ToolDescriptor{
		{Key: "feed_integrity.suppress_episode.1h", Version: "v1", OwnerDomain: "feed_integrity", TargetType: "feed_integrity_episode", ArgumentSchema: "episode-uuid-v1", RequiredPermission: "feed:manage", RiskTier: RiskHigh, Batchable: false, TargetCap: 1, Executor: "feed_integrity.suppress_episode.1h", Monitor: "feed_integrity.action_monitor", Verifier: "feed_integrity.suppression_verify", Idempotency: "operator_plan_step", Cancellation: "before_start_only", Rollback: "feed_integrity.revoke_suppression", Contingencies: []string{"episode_not_open:stop_and_record_failure", "verification_failed:stop_and_record_failure"}, AffectedDomains: []string{"feed_integrity"}, LocalizedActionKey: "operator.action.suppress_feed_integrity_episode"},
		{Key: "feed_integrity.revoke_suppression", Version: "v1", OwnerDomain: "feed_integrity", TargetType: "feed_integrity_suppression", ArgumentSchema: "suppression-uuid-v1", RequiredPermission: "feed:manage", RiskTier: RiskRoutine, Batchable: false, TargetCap: 1, Executor: "feed_integrity.revoke_suppression", Monitor: "feed_integrity.action_monitor", Verifier: "feed_integrity.suppression_verify", Idempotency: "operator_plan_step", Cancellation: "before_start_only", Rollback: "new_signed_suppression_plan_required", Contingencies: []string{"suppression_not_active:stop_and_record_failure", "verification_failed:stop_and_record_failure"}, AffectedDomains: []string{"feed_integrity"}, LocalizedActionKey: "operator.action.revoke_feed_integrity_suppression"},
		{Key: "real_experience.suppress_incident.1h", Version: "v1", OwnerDomain: "real_experience", TargetType: "experience_incident", ArgumentSchema: "incident-uuid-v1", RequiredPermission: "feed:manage", RiskTier: RiskHigh, Batchable: false, TargetCap: 1, Executor: "real_experience.suppress_incident.1h", Monitor: "real_experience.action_monitor", Verifier: "real_experience.suppression_verify", Idempotency: "operator_plan_step", Cancellation: "before_start_only", Rollback: "real_experience.revoke_suppression", Contingencies: []string{"incident_not_open:stop_and_record_failure", "verification_failed:stop_and_record_failure"}, AffectedDomains: []string{"real_experience"}, LocalizedActionKey: "operator.action.suppress_experience_incident"},
		{Key: "real_experience.revoke_suppression", Version: "v1", OwnerDomain: "real_experience", TargetType: "experience_suppression", ArgumentSchema: "suppression-uuid-v1", RequiredPermission: "feed:manage", RiskTier: RiskRoutine, Batchable: false, TargetCap: 1, Executor: "real_experience.revoke_suppression", Monitor: "real_experience.action_monitor", Verifier: "real_experience.suppression_verify", Idempotency: "operator_plan_step", Cancellation: "before_start_only", Rollback: "new_signed_suppression_plan_required", Contingencies: []string{"suppression_not_active:stop_and_record_failure", "verification_failed:stop_and_record_failure"}, AffectedDomains: []string{"real_experience"}, LocalizedActionKey: "operator.action.revoke_experience_suppression"},
	} {
		tools[descriptor.Key] = descriptor
	}
	// These are deliberately one-way, time-bounded operational brakes.  They
	// are not policy editors: a plan can only pause the named tenant-local
	// automation for 24 hours, never enable it, alter its mode, or tune limits.
	// Each key is still a distinct catalog entry and executor case.
	for _, spec := range []struct {
		key, domain, permission, rollback string
		affected                          []string
	}{
		{"feed_integrity.pause.24h", "feed_integrity", "feed:manage", "expires_automatically_after_24h", []string{"feed_integrity"}},
		{"real_experience.pause.24h", "real_experience", "feed:manage", "expires_automatically_after_24h", []string{"real_experience"}},
		{"retention.pause.24h", "retention", "feed:manage", "expires_automatically_after_24h", []string{"retention"}},
		{"ai_economics.pause.24h", "ai_economics", "content:write", "expires_automatically_after_24h", []string{"ai_economics"}},
		{"news_circulation.pause.24h", "news_circulation", "feed:manage", "expires_automatically_after_24h", []string{"news_circulation", "news"}},
		{"media_circulation.pause.24h", "media_circulation", "feed:manage", "expires_automatically_after_24h", []string{"media_circulation"}},
		{"redundancy.pause.24h", "redundancy", "content:write", "expires_automatically_after_24h", []string{"redundancy"}},
		{"pipeline.pause.24h", "pipeline", "aggregation:manage", "expires_automatically_after_24h", []string{"pipeline"}},
		{"enrichment.pause.24h", "enrichment", "content:write", "expires_automatically_after_24h", []string{"enrichment"}},
		{"embeddings.pause_campaigns.24h", "embeddings", "content:write", "expires_automatically_after_24h", []string{"embeddings"}},
		{"topics_preferences.pause.24h", "topics_preferences", "content:write", "expires_automatically_after_24h", []string{"topics_preferences"}},
		{"media_library.pause.24h", "media_library", "content:write", "expires_automatically_after_24h", []string{"media_library", "atomization"}},
	} {
		tools[spec.key] = ToolDescriptor{Key: spec.key, Version: "v1", OwnerDomain: spec.domain, TargetType: "current_domain_policy", ArgumentSchema: "fixed-current-domain-v1", RequiredPermission: spec.permission, RiskTier: RiskHigh, Batchable: false, TargetCap: 1, Executor: spec.key, Monitor: spec.domain + ".pause_monitor", Verifier: spec.domain + ".pause_verify", Idempotency: "operator_plan_step", Cancellation: "before_start_only", Rollback: spec.rollback, Contingencies: []string{"policy_missing_or_changed:stop_and_record_failure", "verification_failed:stop_and_record_failure"}, AffectedDomains: spec.affected, LocalizedActionKey: "operator.action." + strings.ReplaceAll(spec.key, ".", "_")}
	}
	// Operator governance has no generic dispatcher. Each stable key below is
	// a separate typed state transition with a fixed cadence, feedback duration,
	// or control meaning. The browser can select a target, but cannot supply
	// executable arguments or turn a broad filter into an action.
	for _, spec := range []struct {
		key, targetType, schema, permission, risk, rollback string
		affected                                            []string
	}{
		{"operator.schedule.create.hourly", "completed_investigation", "investigation-uuid-v1", "feed:manage", "routine", "cancel_before_worker_claim", []string{"operator"}},
		{"operator.schedule.create.daily", "completed_investigation", "investigation-uuid-v1", "feed:manage", "routine", "cancel_before_worker_claim", []string{"operator"}},
		{"operator.schedule.create.weekly", "completed_investigation", "investigation-uuid-v1", "feed:manage", "routine", "cancel_before_worker_claim", []string{"operator"}},
		{"operator.schedule.pause", "operator_schedule", "schedule-uuid-v1", "feed:manage", "routine", "new_signed_resume_plan_required", []string{"operator"}},
		{"operator.schedule.resume", "operator_schedule", "schedule-uuid-v1", "feed:manage", "routine", "new_signed_pause_plan_required", []string{"operator"}},
		{"operator.schedule.takeover", "operator_schedule", "schedule-uuid-v1", "feed:manage", "high_impact", "takeover_is_forward_only", []string{"operator"}},
		{"operator.recommendation.snooze.15m", "operator_recommendation", "recommendation-uuid-v1", "feed:manage", "routine", "new_signed_feedback_plan_required", []string{"operator"}},
		{"operator.recommendation.snooze.1h", "operator_recommendation", "recommendation-uuid-v1", "feed:manage", "routine", "new_signed_feedback_plan_required", []string{"operator"}},
		{"operator.recommendation.snooze.1d", "operator_recommendation", "recommendation-uuid-v1", "feed:manage", "routine", "new_signed_feedback_plan_required", []string{"operator"}},
		{"operator.recommendation.snooze.7d", "operator_recommendation", "recommendation-uuid-v1", "feed:manage", "routine", "new_signed_feedback_plan_required", []string{"operator"}},
		{"operator.recommendation.dismiss", "operator_recommendation", "recommendation-uuid-v1", "feed:manage", "routine", "new_signed_feedback_plan_required", []string{"operator"}},
		{"operator.recommendation.subject_override", "operator_recommendation", "recommendation-uuid-v1", "feed:manage", "high_impact", "new_signed_override_change_required", []string{"operator"}},
		{"operator.control.disable.read", "operator_control", "fixed-control-v1", "feed:manage", "high_impact", "disable_only", []string{"operator"}},
		{"operator.control.disable.llm", "operator_control", "fixed-control-v1", "feed:manage", "high_impact", "disable_only", []string{"operator"}},
		{"operator.control.disable.execution", "operator_control", "fixed-control-v1", "feed:manage", "high_impact", "disable_only", []string{"operator"}},
		{"operator.control.disable.schedules", "operator_control", "fixed-control-v1", "feed:manage", "high_impact", "disable_only", []string{"operator"}},
	} {
		tier := RiskRoutine
		if spec.risk == "high_impact" {
			tier = RiskHigh
		}
		tools[spec.key] = ToolDescriptor{
			Key: spec.key, Version: "v1", OwnerDomain: "operator", TargetType: spec.targetType, ArgumentSchema: spec.schema,
			RequiredPermission: spec.permission, RiskTier: tier, Batchable: false, TargetCap: 1, Executor: spec.key,
			Monitor: "operator.governance_monitor", Verifier: "operator.governance_verify", Idempotency: "operator_plan_step",
			Cancellation: "before_start_only", Rollback: spec.rollback,
			Contingencies:   []string{"target_preflight_changed:stop_and_record_failure", "verification_failed:stop_and_record_failure"},
			AffectedDomains: spec.affected, LocalizedActionKey: "operator.action." + strings.ReplaceAll(spec.key, ".", "_"),
		}
	}
	tools["operator.share.create"] = ToolDescriptor{Key: "operator.share.create", Version: "v1", OwnerDomain: "operator", TargetType: "investigation_recipient", ArgumentSchema: "investigation-recipient-v1", RequiredPermission: "feed:manage", RiskTier: RiskHigh, Batchable: false, TargetCap: 1, Executor: "operator.share.create", Monitor: "operator.governance_monitor", Verifier: "operator.governance_verify", Idempotency: "operator_plan_step", Cancellation: "before_start_only", Rollback: "operator.share.revoke", Contingencies: []string{"investigation_not_shareable:stop_and_record_failure", "recipient_access_changes:redact_on_read", "verification_failed:stop_and_record_failure"}, AffectedDomains: []string{"operator"}, LocalizedActionKey: "operator.action.share_create"}
	tools["operator.share.revoke"] = ToolDescriptor{Key: "operator.share.revoke", Version: "v1", OwnerDomain: "operator", TargetType: "operator_share", ArgumentSchema: "share-uuid-v1", RequiredPermission: "feed:manage", RiskTier: RiskRoutine, Batchable: false, TargetCap: 1, Executor: "operator.share.revoke", Monitor: "operator.governance_monitor", Verifier: "operator.governance_verify", Idempotency: "operator_plan_step", Cancellation: "before_start_only", Rollback: "new_signed_share_plan_required", Contingencies: []string{"share_not_found:stop_and_record_failure", "verification_failed:stop_and_record_failure"}, AffectedDomains: []string{"operator"}, LocalizedActionKey: "operator.action.share_revoke"}
	for _, spec := range []struct{ key, kind, schema string }{
		{"operator.control.disable.adapter", "adapter", "registered-adapter-key-v1"},
		{"operator.control.disable.tool", "tool", "registered-tool-key-v1"},
	} {
		tools[spec.key] = ToolDescriptor{Key: spec.key, Version: "v1", OwnerDomain: "operator", TargetType: "operator_" + spec.kind + "_control", ArgumentSchema: spec.schema, RequiredPermission: "feed:manage", RiskTier: RiskHigh, Batchable: false, TargetCap: 1, Executor: spec.key, Monitor: "operator.governance_monitor", Verifier: "operator.governance_verify", Idempotency: "operator_plan_step", Cancellation: "before_start_only", Rollback: "disable_only", Contingencies: []string{"target_preflight_changed:stop_and_record_failure", "verification_failed:stop_and_record_failure"}, AffectedDomains: []string{"operator"}, LocalizedActionKey: "operator.action." + strings.ReplaceAll(spec.key, ".", "_")}
	}
	return ToolCatalog{tools: tools}
}

func (catalog ToolCatalog) Lookup(key string) (ToolDescriptor, bool) {
	descriptor, ok := catalog.tools[strings.TrimSpace(key)]
	return descriptor, ok
}

// ListForDomain exposes a stable, sorted read model of static descriptors. It
// is intentionally not a dispatcher: callers still must select one returned
// key and CreateOperatorPlan reconstructs every argument and precondition.
func (catalog ToolCatalog) ListForDomain(domain string) []ToolDescriptor {
	items := make([]ToolDescriptor, 0)
	for _, descriptor := range catalog.tools {
		if descriptor.OwnerDomain == domain {
			items = append(items, descriptor)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	return items
}

// DeriveArguments is deliberately code-owned. The browser supplies an exact
// target selection, never an argument object; each registered tool derives the
// small normalized argument set it is willing to execute.
func (catalog ToolCatalog) DeriveArguments(toolKey string, targetIDs []string) (map[string]any, error) {
	descriptor, ok := catalog.Lookup(toolKey)
	if !ok {
		return nil, fmt.Errorf("%w: unregistered tool", ErrForbiddenTool)
	}
	if len(targetIDs) != 1 || strings.TrimSpace(targetIDs[0]) == "" {
		return nil, fmt.Errorf("%w: registered tool requires one exact target", ErrInvalidContract)
	}
	switch descriptor.Key {
	case "feed_integrity.refresh_snapshot":
		window := strings.ToLower(strings.TrimSpace(targetIDs[0]))
		if window != "today" && window != "week" && window != "month" {
			return nil, fmt.Errorf("%w: unsupported registered snapshot window", ErrInvalidContract)
		}
		return map[string]any{"window": window}, nil
	case "media_circulation.supply.disable_evaluator":
		if targetIDs[0] != "current" {
			return nil, fmt.Errorf("%w: Supply evaluator target must be current", ErrInvalidContract)
		}
		return map[string]any{
			"control_key": models.MediaSupplyControlReadEvaluation,
			"scope_type":  models.MediaSupplyControlScopeTenant,
			"scope_id":    models.MediaSupplyControlScopeAll,
		}, nil
	case supply.SupplyActionRepairMissedAdmission, supply.SupplyActionReclaimDispatchClaim, supply.SupplyActionTransferUnitLease,
		supply.SupplyActionAdoptUnitJob, supply.SupplyActionRedeliverReceipt, supply.SupplyActionVerifyEffect,
		supply.SupplyActionFinalizeVerifiedNoChange, supply.SupplyActionCancelUnstarted, supply.SupplyActionPipelineResumeExactStage,
		supply.SupplyActionArtifactRequestTranscript, supply.SupplyActionArtifactRequestImageEmbedding,
		supply.SupplyActionArtifactRequestTextEmbedding, supply.SupplyActionArtifactRequestLLMMetadata,
		supply.SupplyActionAtomizationExecuteExactParent, supply.SupplyActionFeedGenerationAttachVerifiedMember:
		id, err := registeredUUIDTarget(targetIDs[0])
		if err != nil {
			return nil, err
		}
		return map[string]any{"episode_id": id}, nil
	case "sources.run_once", "media_sources.run_once", "sources.pause", "sources.resume", "media_sources.pause", "media_sources.resume":
		id, err := uuid.Parse(strings.TrimSpace(targetIDs[0]))
		if err != nil || id == uuid.Nil {
			return nil, fmt.Errorf("%w: source target must be a UUID", ErrInvalidContract)
		}
		return map[string]any{"source_id": id.String()}, nil
	case "feed_integrity.suppress_episode.1h":
		id, err := registeredUUIDTarget(targetIDs[0])
		if err != nil {
			return nil, err
		}
		return map[string]any{"episode_id": id, "ttl_minutes": 60}, nil
	case "feed_integrity.revoke_suppression":
		id, err := registeredUUIDTarget(targetIDs[0])
		if err != nil {
			return nil, err
		}
		return map[string]any{"suppression_id": id}, nil
	case "real_experience.suppress_incident.1h":
		id, err := registeredUUIDTarget(targetIDs[0])
		if err != nil {
			return nil, err
		}
		return map[string]any{"incident_id": id, "ttl_minutes": 60}, nil
	case "real_experience.revoke_suppression":
		id, err := registeredUUIDTarget(targetIDs[0])
		if err != nil {
			return nil, err
		}
		return map[string]any{"suppression_id": id}, nil
	case "feed_integrity.pause.24h", "real_experience.pause.24h", "retention.pause.24h", "ai_economics.pause.24h", "news_circulation.pause.24h", "media_circulation.pause.24h", "redundancy.pause.24h", "pipeline.pause.24h", "enrichment.pause.24h", "embeddings.pause_campaigns.24h", "topics_preferences.pause.24h", "media_library.pause.24h":
		if targetIDs[0] != "current" {
			return nil, fmt.Errorf("%w: pause target must be current", ErrInvalidContract)
		}
		return map[string]any{"duration_minutes": 24 * 60}, nil
	case "operator.schedule.create.hourly", "operator.schedule.create.daily", "operator.schedule.create.weekly":
		id, err := registeredUUIDTarget(targetIDs[0])
		if err != nil {
			return nil, err
		}
		cadence := map[string]int{"operator.schedule.create.hourly": 60, "operator.schedule.create.daily": 24 * 60, "operator.schedule.create.weekly": 7 * 24 * 60}[descriptor.Key]
		return map[string]any{"investigation_id": id, "cadence_minutes": cadence}, nil
	case "operator.schedule.pause", "operator.schedule.resume", "operator.schedule.takeover":
		id, err := registeredUUIDTarget(targetIDs[0])
		if err != nil {
			return nil, err
		}
		return map[string]any{"schedule_id": id}, nil
	case "operator.share.create":
		parts := strings.SplitN(targetIDs[0], "|", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("%w: share target must contain an investigation and recipient", ErrInvalidContract)
		}
		investigationID, err := registeredUUIDTarget(parts[0])
		recipientID := strings.TrimSpace(parts[1])
		if err != nil || recipientID == "" || len(recipientID) > 255 || strings.ContainsAny(recipientID, "|\r\n") {
			return nil, fmt.Errorf("%w: share recipient target is invalid", ErrInvalidContract)
		}
		return map[string]any{"investigation_id": investigationID, "recipient_id": recipientID}, nil
	case "operator.share.revoke":
		id, err := registeredUUIDTarget(targetIDs[0])
		if err != nil {
			return nil, err
		}
		return map[string]any{"share_id": id}, nil
	case "operator.recommendation.snooze.15m", "operator.recommendation.snooze.1h", "operator.recommendation.snooze.1d", "operator.recommendation.snooze.7d", "operator.recommendation.dismiss", "operator.recommendation.subject_override":
		id, err := registeredUUIDTarget(targetIDs[0])
		if err != nil {
			return nil, err
		}
		arguments := map[string]any{"recommendation_id": id}
		if duration, ok := map[string]int{"operator.recommendation.snooze.15m": 15, "operator.recommendation.snooze.1h": 60, "operator.recommendation.snooze.1d": 24 * 60, "operator.recommendation.snooze.7d": 7 * 24 * 60}[descriptor.Key]; ok {
			arguments["snooze_minutes"] = duration
		}
		return arguments, nil
	case "operator.control.disable.read", "operator.control.disable.llm", "operator.control.disable.execution", "operator.control.disable.schedules":
		if targetIDs[0] != "current" {
			return nil, fmt.Errorf("%w: control target must be current", ErrInvalidContract)
		}
		kind := strings.TrimPrefix(descriptor.Key, "operator.control.disable.")
		return map[string]any{"control_kind": kind}, nil
	case "operator.control.disable.adapter":
		adapterKey := strings.TrimSpace(targetIDs[0])
		if !HasRegisteredAdapterKey(adapterKey) {
			return nil, fmt.Errorf("%w: adapter control target is not registered", ErrInvalidContract)
		}
		return map[string]any{"control_kind": "adapter", "capability_key": adapterKey}, nil
	case "operator.control.disable.tool":
		capabilityKey := strings.TrimSpace(targetIDs[0])
		if _, exists := catalog.Lookup(capabilityKey); !exists || PermanentlyForbiddenOperatorTool(capabilityKey) {
			return nil, fmt.Errorf("%w: tool control target is not registered", ErrInvalidContract)
		}
		return map[string]any{"control_kind": "tool", "capability_key": capabilityKey}, nil
	default:
		return nil, fmt.Errorf("%w: registered tool has no argument derivation", ErrInvalidContract)
	}
}

func registeredUUIDTarget(raw string) (string, error) {
	id, err := uuid.Parse(strings.TrimSpace(raw))
	if err != nil || id == uuid.Nil {
		return "", fmt.Errorf("%w: target must be a UUID", ErrInvalidContract)
	}
	return id.String(), nil
}

// BuildCanonicalPlan accepts only an explicit, current context and an
// already-admitted descriptor. It intentionally does not execute anything.
func (catalog ToolCatalog) BuildCanonicalPlan(packet DecisionPacket, access AccessSnapshot, toolKey string, targetIDs []string, normalizedArguments map[string]any) (CanonicalPlan, error) {
	if err := packet.Validate(); err != nil {
		return CanonicalPlan{}, err
	}
	if err := access.ValidateFor(packet.ActorID, packet.TenantID); err != nil {
		return CanonicalPlan{}, err
	}
	descriptor, ok := catalog.Lookup(toolKey)
	if !ok {
		return CanonicalPlan{}, fmt.Errorf("%w: unregistered tool", ErrForbiddenTool)
	}
	if err := descriptor.Validate(); err != nil || !access.HasPermission(descriptor.RequiredPermission) {
		return CanonicalPlan{}, fmt.Errorf("%w: tool is not currently permitted", ErrForbiddenTool)
	}
	if !ToolAdmittedForDomain(packet.VisibleContext.Domain, descriptor.Key) {
		return CanonicalPlan{}, fmt.Errorf("%w: tool is not admitted for this context domain", ErrForbiddenTool)
	}
	if packet.Conflicts != nil && len(packet.Conflicts) > 0 {
		return CanonicalPlan{}, fmt.Errorf("%w: conflicting evidence blocks action planning", ErrInvalidContract)
	}
	if packet.Completeness != "complete" {
		return CanonicalPlan{}, fmt.Errorf("%w: partial evidence blocks action planning", ErrInvalidContract)
	}
	if !visibleIntentAllowed(packet.VisibleContext, IntentResolve) {
		return CanonicalPlan{}, fmt.Errorf("%w: resolve is not admitted for this context", ErrInvalidContract)
	}
	if packet.VisibleContext.Selection == nil || packet.VisibleContext.Selection.Mode != "explicit" {
		return CanonicalPlan{}, fmt.Errorf("%w: filtered selections cannot mutate", ErrInvalidContract)
	}
	if len(targetIDs) == 0 || len(targetIDs) > descriptor.TargetCap {
		return CanonicalPlan{}, fmt.Errorf("%w: target count exceeds registered tool limit", ErrInvalidContract)
	}
	if !descriptor.Batchable && len(targetIDs) != 1 {
		return CanonicalPlan{}, fmt.Errorf("%w: non-batch tool requires one target", ErrInvalidContract)
	}
	if packet.VisibleContext.Selection != nil && !subsetOf(targetIDs, packet.VisibleContext.Selection.IDs) {
		return CanonicalPlan{}, fmt.Errorf("%w: targets must be explicitly selected", ErrInvalidContract)
	}
	if containsUnsafeArgument(normalizedArguments) {
		return CanonicalPlan{}, fmt.Errorf("%w: unsafe plan arguments", ErrInvalidContract)
	}
	evidenceIDs := SortedEvidenceIDs(packet.Evidence)
	// A fixed 24-hour pause is a disable-only containment brake. It does not
	// make a decision from the age of a run or queue observation: the worker
	// locks the live tenant policy row and verifies the resulting expiry before
	// it can succeed. Keep every other mutation freshness-gated, but allow this
	// narrowly registered brake to be prepared from a stale read so a healthy
	// emergency path is not hidden by an old status timestamp.
	allowStaleContainmentEvidence := strings.HasSuffix(descriptor.Key, ".pause.24h")
	for _, evidence := range packet.Evidence {
		if evidence.Availability != EvidenceAvailable {
			if allowStaleContainmentEvidence && evidence.Availability == EvidenceStale {
				continue
			}
			return CanonicalPlan{}, fmt.Errorf("%w: non-current evidence blocks action planning", ErrInvalidContract)
		}
	}
	sortedTargets := append([]string(nil), targetIDs...)
	sort.Strings(sortedTargets)
	return CanonicalPlan{
		SchemaVersion: ContractVersion, PlanID: uuid.NewString(), TenantID: packet.TenantID, ActorID: packet.ActorID,
		ToolKey: descriptor.Key, ToolVersion: descriptor.Version, TargetIDs: sortedTargets,
		NormalizedArguments: normalizedArguments, EvidenceIDs: evidenceIDs, EvidenceFingerprint: packet.Fingerprint,
		AccessVersion: access.AccessVersion, RiskTier: descriptor.RiskTier,
		Cancellation: descriptor.Cancellation, Rollback: descriptor.Rollback, Contingencies: append([]string(nil), descriptor.Contingencies...),
	}, nil
}

func subsetOf(values, allowed []string) bool {
	set := make(map[string]struct{}, len(allowed))
	for _, item := range allowed {
		set[item] = struct{}{}
	}
	for _, item := range values {
		if strings.TrimSpace(item) == "" {
			return false
		}
		if _, ok := set[item]; !ok {
			return false
		}
	}
	return true
}

func containsUnsafeArgument(arguments map[string]any) bool {
	for key, value := range arguments {
		needle := strings.ToLower(strings.TrimSpace(key))
		for _, forbidden := range []string{"url", "sql", "shell", "queue", "token", "secret", "password", "migration"} {
			if strings.Contains(needle, forbidden) {
				return true
			}
		}
		if nested, ok := value.(map[string]any); ok && containsUnsafeArgument(nested) {
			return true
		}
	}
	return false
}
