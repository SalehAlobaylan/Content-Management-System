package operator

import "strings"

// DomainActionAdmission is the reviewed, code-owned action boundary for a
// Console domain. A domain may intentionally expose only manual work; that is
// still an explicit admission decision, not an omission the model can fill.
type DomainActionAdmission struct {
	Domain       string
	ToolKeys     []string
	ManualOnly   []string
	NoToolReason string
}

// DefaultDomainActionCatalog covers every launch domain in the Console route
// manifest. ToolKeys are executable only when present in DefaultToolCatalog;
// the other entries preserve their existing native workflows until a separate
// bounded mutation service and verifier are admitted.
func DefaultDomainActionCatalog() []DomainActionAdmission {
	return []DomainActionAdmission{
		{Domain: "global_ops", ManualOnly: []string{"fleet_pause_resume", "incident_acknowledge"}, NoToolReason: "multi-member operations require native operational ownership"},
		{Domain: "system_health", ManualOnly: []string{"service_pause_resume", "incident_close"}, NoToolReason: "service controls remain native"},
		{Domain: "feed_integrity", ToolKeys: []string{"feed_integrity.refresh_snapshot", "feed_integrity.suppress_episode.1h", "feed_integrity.revoke_suppression", "feed_integrity.pause.24h"}},
		{Domain: "feed_recovery", ManualOnly: []string{"repair", "rotate_lane", "purge_reseed", "rollback"}, NoToolReason: "native recovery lease and reauthentication are required"},
		{Domain: "retention", ToolKeys: []string{"retention.pause.24h"}, ManualOnly: []string{"policy", "prepare_compaction", "owner_execution"}, NoToolReason: "destructive custody work remains permanently manual-only"},
		{Domain: "real_experience", ToolKeys: []string{"real_experience.suppress_incident.1h", "real_experience.revoke_suppression", "real_experience.pause.24h"}, ManualOnly: []string{"run_probe", "incident_close"}, NoToolReason: "experience evaluation ownership remains native"},
		{Domain: "ai_economics", ToolKeys: []string{"ai_economics.pause.24h"}, ManualOnly: []string{"budget_policy", "governor"}, NoToolReason: "spend caps and policy edits require a dedicated contract"},
		{Domain: "sources", ToolKeys: []string{"sources.run_once", "sources.pause", "sources.resume"}, ManualOnly: []string{"config_update"}},
		{Domain: "content", ManualOnly: []string{"status_repair", "enrichment_trigger"}, NoToolReason: "content mutations need item-level policy admission"},
		{Domain: "news", ToolKeys: []string{"feed_integrity.refresh_snapshot"}, ManualOnly: []string{"merge", "override"}},
		{Domain: "news_finding", ManualOnly: []string{"sweep", "build_graph", "suggestion_decision"}, NoToolReason: "discovery batch ownership remains native"},
		{Domain: "news_circulation", ToolKeys: []string{"news_circulation.pause.24h"}, ManualOnly: []string{"generate", "apply", "dismiss", "revert"}, NoToolReason: "recommendation eligibility is native"},
		{Domain: "media_sources", ToolKeys: []string{"media_sources.run_once", "media_sources.pause", "media_sources.resume"}, ManualOnly: []string{"suggestion_decision"}},
		{Domain: "atomization", ManualOnly: []string{"atomize", "reatomize", "chapter_decision", "sweep"}, NoToolReason: "media policy and review gates require dedicated admission"},
		{Domain: "media_circulation", ToolKeys: []string{"media_circulation.pause.24h"}, ManualOnly: []string{"generate", "apply", "dismiss", "revert", "override"}, NoToolReason: "circulation proof and projected state are native"},
		{Domain: "redundancy", ToolKeys: []string{"redundancy.pause.24h"}, ManualOnly: []string{"confirm_pair", "reject_pair", "canonical_decision"}, NoToolReason: "pair evidence must be revalidated by native workflow"},
		{Domain: "media_library", ToolKeys: []string{"media_library.pause.24h"}, ManualOnly: []string{"transcribe", "enrichment_trigger", "metadata_repair"}, NoToolReason: "artifact operations require item-level policy admission"},
		{Domain: "storage_quality", ManualOnly: []string{"restore", "reconcile", "probe", "archive", "reencode"}, NoToolReason: "storage ownership and playback dependency checks are native"},
		{Domain: "pipeline", ToolKeys: []string{"pipeline.pause.24h"}, ManualOnly: []string{"retry", "run"}, NoToolReason: "queue work remains behind native retry policy"},
		{Domain: "enrichment", ToolKeys: []string{"enrichment.pause.24h"}, ManualOnly: []string{"trigger", "retry", "autopilot_control"}, NoToolReason: "artifact lane policy admission is pending"},
		{Domain: "intelligence", ManualOnly: []string{"refresh", "flag", "preview", "config"}, NoToolReason: "ranking changes need a reversible high-impact contract"},
		{Domain: "embeddings", ToolKeys: []string{"embeddings.pause_campaigns.24h"}, ManualOnly: []string{"campaign_preview", "campaign_start", "campaign_abort"}, NoToolReason: "campaign lifecycle is native"},
		{Domain: "topics_preferences", ToolKeys: []string{"topics_preferences.pause.24h"}, ManualOnly: []string{"proposal_decision", "mapping", "merge"}, NoToolReason: "preference impact requires native review"},
		{Domain: "moderation", ManualOnly: []string{"resolve", "dismiss", "allow"}, NoToolReason: "moderation policy and target evidence are native"},
		{Domain: "auth_center", ManualOnly: []string{"iam_mutation"}, NoToolReason: "IAM mutations are permanently manual-only"},
		{Domain: "operator", ToolKeys: []string{"operator.schedule.create.hourly", "operator.schedule.create.daily", "operator.schedule.create.weekly", "operator.schedule.pause", "operator.schedule.resume", "operator.schedule.takeover", "operator.share.create", "operator.share.revoke", "operator.recommendation.snooze.15m", "operator.recommendation.snooze.1h", "operator.recommendation.snooze.1d", "operator.recommendation.snooze.7d", "operator.recommendation.dismiss", "operator.recommendation.subject_override", "operator.control.disable.read", "operator.control.disable.llm", "operator.control.disable.execution", "operator.control.disable.schedules", "operator.control.disable.adapter", "operator.control.disable.tool"}},
	}
}

// PermanentlyForbiddenOperatorCapabilities is deliberately broader than the
// current tool list. Future convenience endpoints must fail closed when they
// resemble a disallowed control plane or destructive operation.
var PermanentlyForbiddenOperatorCapabilities = []string{
	"delete", "purge", "reseed", "destructive_retention", "owner_execution", "iam.",
	"secret", "token", "password", "migration", "restart", "arbitrary_api", "arbitrary_queue",
}

func PermanentlyForbiddenOperatorTool(key string) bool {
	key = strings.ToLower(strings.TrimSpace(key))
	for _, forbidden := range PermanentlyForbiddenOperatorCapabilities {
		if strings.Contains(key, forbidden) {
			return true
		}
	}
	return false
}

func ValidateDomainActionCatalog() bool {
	tools := DefaultToolCatalog()
	seen := map[string]struct{}{}
	for _, domain := range DefaultDomainActionCatalog() {
		if strings.TrimSpace(domain.Domain) == "" || len(domain.ToolKeys) == 0 && len(domain.ManualOnly) == 0 {
			return false
		}
		if _, exists := seen[domain.Domain]; exists {
			return false
		}
		seen[domain.Domain] = struct{}{}
		for _, key := range domain.ToolKeys {
			descriptor, ok := tools.Lookup(key)
			if !ok || descriptor.Validate() != nil {
				return false
			}
		}
	}
	return true
}
