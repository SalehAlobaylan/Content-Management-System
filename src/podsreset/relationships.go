package podsreset

import "sort"

// RelationshipPolicy is the reset owner’s explicit disposition for one
// durable or polymorphic reference to a content identity. Unknown references
// are never inferred safe from a generic write trigger.
type RelationshipPolicy struct {
	Table         string          `json:"table"`
	Column        string          `json:"column"`
	Class         string          `json:"class"`
	Disposition   DataDisposition `json:"disposition"`
	Owner         string          `json:"owner"`
	Reason        string          `json:"reason"`
	Postcondition string          `json:"postcondition"`
}

var relationshipPolicies = []RelationshipPolicy{
	{Table: "artifact_coverage_requests", Column: "content_item_id", Class: "artifact_coverage_work", Disposition: Blocked, Owner: "aggregation/cms", Reason: "coverage work can publish new artifacts", Postcondition: "active and uncertain requests are drained; terminal evidence remains"},
	{Table: "atomization_generations", Column: "parent_content_item_id", Class: "atomization_payload", Disposition: DetachRecompute, Owner: "cms", Reason: "generated plans are derived from the parent transcript", Postcondition: "generation and chapter units are superseded; plan and unit payload are cleared"},
	{Table: "atomization_work_requests", Column: "parent_content_item_id", Class: "atomization_work", Disposition: Blocked, Owner: "aggregation/cms", Reason: "work requests can publish child feed units", Postcondition: "queued, leased, running, verifying, and uncertain work is absent"},
	{Table: "chapters", Column: "child_content_item_id", Class: "chapter_feed_link", Disposition: DetachRecompute, Owner: "cms/editorial", Reason: "chapter-to-child links must not point at a retired feed item", Postcondition: "all links to the target are detached before chapter cleanup"},
	{Table: "content_flags", Column: "content_item_id", Class: "moderation_protection", Disposition: Protected, Owner: "cms/trust", Reason: "a content flag is a protective signal", Postcondition: "flagged content is blocked; flag history is preserved"},
	{Table: "content_items", Column: "parent_content_item_id", Class: "content_lineage", Disposition: RetainIdentity, Owner: "cms", Reason: "parent provenance is part of the stable content identity", Postcondition: "lineage remains on the minimal archived row; new links to retired parents are rejected"},
	{Table: "content_processing_events", Column: "content_item_id", Class: "processing_ledger", Disposition: RetainIdentity, Owner: "cms", Reason: "processing events are immutable audit evidence", Postcondition: "event history remains linked to the retained identity"},
	{Table: "content_stage_receipts", Column: "content_item_id", Class: "processing_ledger", Disposition: RetainIdentity, Owner: "cms", Reason: "stage receipts prove prior pipeline effects", Postcondition: "receipt history remains linked to the retained identity"},
	{Table: "content_stage_requests", Column: "content_item_id", Class: "processing_work_and_ledger", Disposition: Blocked, Owner: "cms", Reason: "nonterminal stages can republish metadata while terminal stages are audit evidence", Postcondition: "all stages are terminal before reset; stage history remains unchanged"},
	{Table: "content_item_topics", Column: "content_item_id", Class: "topic_membership", Disposition: DeleteOwnedPayload, Owner: "cms", Reason: "item-to-topic edges are item-owned derived projections", Postcondition: "only the selected item’s topic edges are removed; topic catalog rows remain"},
	{Table: "feed_generation_membership_repairs", Column: "content_item_id", Class: "feed_repair_work_and_ledger", Disposition: Blocked, Owner: "cms/feed", Reason: "an active repair can reattach a retired feed unit while terminal rows are audit history", Postcondition: "queued, running, verifying, and uncertain repairs are absent; terminal history remains"},
	{Table: "feed_recovery_media_purge_items", Column: "content_item_id", Class: "feed_recovery_work", Disposition: Blocked, Owner: "cms/aggregation", Reason: "recovery purge owns a separate exact-object saga", Postcondition: "no prepared or unresolved purge saga references the target"},
	{Table: "feed_recovery_plan_targets", Column: "target_id", Class: "feed_recovery_plan_target", Disposition: Blocked, Owner: "cms/feed-recovery", Reason: "an approved or pending media recovery plan may still act on the target", Postcondition: "no unexpired plan awaiting approval or unresolved recovery run references the target; terminal plan evidence remains"},
	{Table: "media_artifact_manifests", Column: "content_item_id", Class: "artifact_registry", Disposition: DetachRecompute, Owner: "aggregation/cms", Reason: "registry rows must stop advertising removed objects", Postcondition: "owned manifests are terminal deleted evidence with delivery URLs cleared"},
	{Table: "media_artifact_manifests", Column: "parent_content_item_id", Class: "artifact_registry", Disposition: DetachRecompute, Owner: "aggregation/cms", Reason: "parent-owned registry rows must stop advertising removed objects", Postcondition: "owned manifests are terminal deleted evidence with delivery URLs cleared"},
	{Table: "media_atomization_runs", Column: "parent_content_item_id", Class: "atomization_ledger", Disposition: RetainIdentity, Owner: "aggregation/cms", Reason: "run results are durable operational history", Postcondition: "run history remains; active work is blocked"},
	{Table: "media_circulation_recommendations", Column: "subject_id", Class: "circulation_recommendation_work", Disposition: Blocked, Owner: "cms/media-circulation", Reason: "pending and processing item-family recommendations may initiate eviction or re-admission work", Postcondition: "all actionable item-family recommendations are resolved; terminal recommendation evidence remains"},
	{Table: "media_chapter_drafts", Column: "parent_content_item_id", Class: "editorial_chapter_draft", Disposition: Protected, Owner: "cms/editorial", Reason: "drafts may contain human-authored chapter decisions", Postcondition: "any unresolved or applied draft requires editorial resolution before reset"},
	{Table: "media_hls_packages", Column: "content_item_id", Class: "delivery_projection", Disposition: DetachRecompute, Owner: "cms", Reason: "package rows are derived delivery state", Postcondition: "packages are failed and cannot serve retired media"},
	{Table: "media_intelligence_scores", Column: "content_item_id", Class: "derived_media_score", Disposition: DeleteOwnedPayload, Owner: "cms", Reason: "score is a derived content projection", Postcondition: "only the selected item’s score is removed"},
	{Table: "media_playback_health_receipts", Column: "content_item_id", Class: "playback_health_ledger", Disposition: RetainIdentity, Owner: "cms", Reason: "health receipts are diagnostic history", Postcondition: "receipt history remains linked to the retained identity"},
	{Table: "media_rendition_generations", Column: "content_item_id", Class: "delivery_projection", Disposition: DetachRecompute, Owner: "cms", Reason: "rendition rows are derived delivery state", Postcondition: "all owned generations are superseded and detached from serving"},
	{Table: "media_storage_artifact_events", Column: "content_item_id", Class: "storage_audit", Disposition: RetainIdentity, Owner: "cms", Reason: "artifact events are immutable provider-effect evidence", Postcondition: "event history remains linked to the retained identity"},
	{Table: "media_storage_artifact_events", Column: "parent_content_item_id", Class: "storage_audit", Disposition: RetainIdentity, Owner: "cms", Reason: "parent lineage is part of immutable artifact evidence", Postcondition: "event history remains linked to the retained identity"},
	{Table: "media_supply_action_previews", Column: "target_id", Class: "supply_action_preview", Disposition: Blocked, Owner: "cms/media-circulation", Reason: "an active preview can be approved into a durable supply action", Postcondition: "no unexpired active content-item preview remains; consumed and invalidated preview history remains"},
	{Table: "media_studio_actions", Column: "content_item_id", Class: "studio_action_ledger", Disposition: RetainIdentity, Owner: "cms", Reason: "studio actions are operational history", Postcondition: "history remains; active owner work is blocked"},
	{Table: "moderation_reports", Column: "target_id", Class: "moderation_protection", Disposition: Protected, Owner: "cms/trust", Reason: "an open report is a protective signal", Postcondition: "open reports block reset; report history remains"},
	{Table: "experience_events", Column: "content_id", Class: "experience_ledger", Disposition: RetainIdentity, Owner: "cms/experience", Reason: "short-lived telemetry is evidence, not item-owned payload", Postcondition: "event evidence remains linked to the stable identity until its own retention expires"},
	{Table: "enrichment_autopilot_actions", Column: "content_id", Class: "enrichment_action_ledger", Disposition: RetainIdentity, Owner: "cms/enrichment", Reason: "autopilot actions preserve operational provenance", Postcondition: "action history remains linked to the stable identity"},
	{Table: "news_ingest_tombstones", Column: "original_content_id", Class: "ingest_retirement_ledger", Disposition: RetainIdentity, Owner: "cms/news-retention", Reason: "ingest tombstones preserve the identity behind an irreversible news retirement", Postcondition: "tombstone identity evidence remains unchanged"},
	{Table: "retention_compaction_batches", Column: "target_ids", Class: "retention_batch_scope", Disposition: Blocked, Owner: "cms/news-retention", Reason: "a durable compaction batch owns an exact content target set", Postcondition: "no unverified compaction batch references the target; verified batch evidence remains"},
	{Table: "retention_compaction_manifests", Column: "anchor_content_ids", Class: "retention_manifest_protection", Disposition: Protected, Owner: "cms/news-retention", Reason: "anchor IDs preserve canonical story representatives", Postcondition: "no active manifest anchors the target; terminal manifest evidence remains"},
	{Table: "retention_compaction_manifests", Column: "protected_content_ids", Class: "retention_manifest_protection", Disposition: Protected, Owner: "cms/news-retention", Reason: "protected IDs are explicit retention exclusions", Postcondition: "no active manifest protects the target; terminal manifest evidence remains"},
	{Table: "retention_compaction_manifests", Column: "retire_content_ids", Class: "retention_manifest_work", Disposition: Blocked, Owner: "cms/news-retention", Reason: "an active manifest may retire the target under its own approval", Postcondition: "no active or unknown manifest state references the target"},
	{Table: "news_month_archive_stories", Column: "lead_content_id", Class: "archive_identity", Disposition: RetainIdentity, Owner: "cms/editorial", Reason: "frozen archive leads are canonical historical identity", Postcondition: "archive lead identity remains stable and is never cascade-deleted"},
	{Table: "news_month_archive_story_sources", Column: "original_content_id", Class: "archive_provenance", Disposition: RetainIdentity, Owner: "cms/editorial", Reason: "representative source snapshots preserve the original canonical identity", Postcondition: "archive source snapshots remain unchanged and linked to the stable identity"},
	{Table: "pipeline_autopilot_actions", Column: "content_item_id", Class: "pipeline_action_ledger", Disposition: RetainIdentity, Owner: "cms", Reason: "autopilot actions are operational history", Postcondition: "action history remains linked to the retained identity"},
	{Table: "pipeline_repair_requests", Column: "content_item_id", Class: "pipeline_repair_work", Disposition: Blocked, Owner: "cms", Reason: "repair work may rewrite content or artifacts", Postcondition: "active and uncertain repairs are absent; terminal history remains"},
	{Table: "pipeline_stage_leases", Column: "content_item_id", Class: "pipeline_lease", Disposition: Blocked, Owner: "cms", Reason: "an active lease can republish content", Postcondition: "no claimed, running, verifying, or unknown lease remains"},
	{Table: "pods_boundary_observations", Column: "content_item_id", Class: "boundary_ledger", Disposition: RetainIdentity, Owner: "cms", Reason: "boundary observations are audit evidence", Postcondition: "observation history remains linked to the retained identity"},
	{Table: "redundancy_families", Column: "canonical_content_item_id", Class: "redundancy_evidence", Disposition: Blocked, Owner: "cms/redundancy", Reason: "canonical family membership needs its owner to resolve it", Postcondition: "target has no unresolved family relationship"},
	{Table: "redundancy_family_members", Column: "content_item_id", Class: "redundancy_evidence", Disposition: Blocked, Owner: "cms/redundancy", Reason: "family membership needs its owner to resolve it", Postcondition: "target has no unresolved family relationship"},
	{Table: "redundancy_fingerprints", Column: "content_item_id", Class: "redundancy_evidence", Disposition: Blocked, Owner: "cms/redundancy", Reason: "fingerprint evidence needs its owner to resolve it", Postcondition: "target has no unresolved fingerprint relationship"},
	{Table: "redundancy_pairs", Column: "item_a_id", Class: "redundancy_evidence", Disposition: Blocked, Owner: "cms/redundancy", Reason: "pair evidence needs its owner to resolve it", Postcondition: "target has no unresolved pair relationship"},
	{Table: "redundancy_pairs", Column: "item_b_id", Class: "redundancy_evidence", Disposition: Blocked, Owner: "cms/redundancy", Reason: "pair evidence needs its owner to resolve it", Postcondition: "target has no unresolved pair relationship"},
	{Table: "retention_holds", Column: "target_id", Class: "retention_protection", Disposition: Protected, Owner: "cms/retention", Reason: "an active item or story hold is a protection", Postcondition: "active content/story holds block reset; released/expired hold history remains"},
	{Table: "source_run_receipts", Column: "content_item_id", Class: "source_run_ledger", Disposition: RetainIdentity, Owner: "cms/aggregation", Reason: "source-run receipts are ingest audit history", Postcondition: "receipt history remains linked to the retained identity"},
	{Table: "stories", Column: "retained_lead_content_id", Class: "story_identity", Disposition: RetainIdentity, Owner: "cms/news", Reason: "retained story leads preserve canonical story identity", Postcondition: "lead identity is not cascade-deleted or repointed implicitly"},
	{Table: "storage_operation_sagas", Column: "content_item_id", Class: "storage_operation_work", Disposition: Blocked, Owner: "cms/aggregation", Reason: "storage operations may move or remove provider objects", Postcondition: "all storage operations have known terminal outcomes"},
	{Table: "transcription_batch_items", Column: "content_item_id", Class: "transcription_batch_work", Disposition: Blocked, Owner: "media/cms", Reason: "accepted batch items may start transcription", Postcondition: "pending and accepted batch items are absent; terminal history remains"},
	{Table: "transcription_generations", Column: "content_item_id", Class: "transcription_payload", Disposition: DetachRecompute, Owner: "media/cms", Reason: "generation output is derived from the retired media", Postcondition: "generation is superseded and transcript link is cleared unless shared"},
	{Table: "transcription_jobs", Column: "content_item_id", Class: "transcription_work", Disposition: Blocked, Owner: "media/cms", Reason: "queued, running, or uncertain jobs may write transcript data", Postcondition: "all transcription jobs are terminal before reset"},
	{Table: "transcription_segment_units", Column: "content_item_id", Class: "transcription_payload", Disposition: DetachRecompute, Owner: "media/cms", Reason: "segment results are derived transcript payload", Postcondition: "segments are superseded and payload is cleared"},
	{Table: "transcripts", Column: "content_item_id", Class: "transcript_payload", Disposition: DeleteOwnedPayload, Owner: "media/cms", Reason: "transcript payload is cleared only when no surviving consumer references it", Postcondition: "shared transcript payload is preserved; owned payload is cleared and identity remains"},
	{Table: "transcript_quality", Column: "content_item_id", Class: "derived_transcript_quality", Disposition: DeleteOwnedPayload, Owner: "media/cms", Reason: "quality score is a derived transcript projection", Postcondition: "only the selected item’s quality projection is removed"},
	{Table: "transcript_versions", Column: "content_item_id", Class: "transcript_version_history", Disposition: RetainIdentity, Owner: "media/cms", Reason: "version metadata preserves provenance and approval history", Postcondition: "version rows and approval provenance remain; exclusively owned text payload is cleared"},
	{Table: "transcript_versions", Column: "transcript_id", Class: "transcript_version_history", Disposition: RetainIdentity, Owner: "media/cms", Reason: "version rows preserve transcript lineage", Postcondition: "version identity and audit fields remain"},
	{Table: "user_interactions", Column: "content_item_id", Class: "user_data_protection", Disposition: Protected, Owner: "cms/trust", Reason: "user interactions are protected product data", Postcondition: "any interaction blocks reset; user history is never silently deleted"},
	{Table: "media_circulation_overrides", Column: "subject_id", Class: "circulation_protection", Disposition: Protected, Owner: "cms/media-circulation", Reason: "standing editorial/storage protections belong to the circulation owner", Postcondition: "active item/family editorial_hold, never_archive, and keep_latest_n_hot overrides block reset"},
	{Table: "media_supply_action_requests", Column: "target_id", Class: "supply_action_work", Disposition: Blocked, Owner: "cms/media-circulation", Reason: "supply actions may admit new work for the item", Postcondition: "active and uncertain content-item actions are absent"},
	{Table: "feed_generation_memberships", Column: "member_id", Class: "feed_membership", Disposition: DetachRecompute, Owner: "cms/feed", Reason: "membership is polymorphic derived serving state", Postcondition: "all target memberships are detached before provider deletion and new retired-item memberships are rejected"},
}

func RelationshipPolicies() []RelationshipPolicy {
	policies := append([]RelationshipPolicy(nil), relationshipPolicies...)
	sort.Slice(policies, func(i, j int) bool {
		if policies[i].Table == policies[j].Table {
			return policies[i].Column < policies[j].Column
		}
		return policies[i].Table < policies[j].Table
	})
	return policies
}

func RelationshipPolicyFor(table, column string) (RelationshipPolicy, bool) {
	for _, policy := range relationshipPolicies {
		if policy.Table == table && policy.Column == column {
			return policy, true
		}
	}
	return RelationshipPolicy{}, false
}
