package supply

// SupplyActionDescriptor is the static admission record for a native Supply
// repair. It is deliberately not a dispatcher: workers must bind an admitted
// request to one of these exact descriptors before any owner handoff.
type SupplyActionDescriptor struct {
	Key             string
	TargetType      string
	RequiredState   string
	ExecutionOwner  string
	Verification    string
	Cancellation    string
	Rollback        string
	Risk            string
	TargetCap       int
	AffectedDomains []string
}

const (
	SupplyActionRepairMissedAdmission    = "source_run.repair_missed_admission"
	SupplyActionReclaimDispatchClaim     = "source_run.reclaim_dispatch_claim"
	SupplyActionTransferUnitLease        = "source_run.transfer_execution_unit_lease"
	SupplyActionAdoptUnitJob             = "source_run.adopt_unit_job"
	SupplyActionRedeliverReceipt         = "source_run.redeliver_receipt"
	SupplyActionVerifyEffect             = "source_run.verify_effect"
	SupplyActionFinalizeVerifiedNoChange = "source_run.finalize_verified_no_change"
	SupplyActionCancelUnstarted          = "source_run.cancel_unstarted"
	// SupplyActionPipelineResumeExactStage is approval-required and carries no
	// browser-derived stage or queue input. CMS derives the one exact stage from
	// current immutable item evidence at every boundary.
	SupplyActionPipelineResumeExactStage           = "pipeline.resume_exact_stage"
	SupplyActionArtifactRequestTranscript          = "artifact.request_transcript"
	SupplyActionArtifactRequestImageEmbedding      = "artifact.request_image_embedding"
	SupplyActionArtifactRequestTextEmbedding       = "artifact.request_text_embedding"
	SupplyActionArtifactRequestLLMMetadata         = "artifact.request_llm_metadata"
	SupplyActionAtomizationExecuteExactParent      = "atomization.execute_exact_parent"
	SupplyActionStudioClearExactChildren           = "studio.clear_exact_children"
	SupplyActionFeedGenerationAttachVerifiedMember = "feed_generation.attach_verified_member"
)

var supplyActions = map[string]SupplyActionDescriptor{
	SupplyActionRepairMissedAdmission:              {SupplyActionRepairMissedAdmission, "content_source", "due_unadmitted", "cms", "source_run_request_created", "before_start_only", "none", "routine", 1, []string{"media_circulation", "sources"}},
	SupplyActionReclaimDispatchClaim:               {SupplyActionReclaimDispatchClaim, "source_run_attempt", "dispatcher_lease_expired", "cms", "attempt_claim_current", "before_start_only", "none", "routine", 1, []string{"media_circulation", "sources"}},
	SupplyActionTransferUnitLease:                  {SupplyActionTransferUnitLease, "source_run_execution_unit", "proven_no_effect", "cms", "unit_lease_current", "before_start_only", "none", "high", 1, []string{"media_circulation", "pipeline"}},
	SupplyActionAdoptUnitJob:                       {SupplyActionAdoptUnitJob, "source_run_execution_unit", "manifested_unstarted", "aggregation_dispatcher", "owner_receipt", "before_owner_effect", "none", "high", 1, []string{"media_circulation", "pipeline"}},
	SupplyActionRedeliverReceipt:                   {SupplyActionRedeliverReceipt, "source_run_retained_receipt", "retained", "aggregation_receipt", "cms_receipt_ledger", "before_owner_effect", "none", "routine", 1, []string{"media_circulation", "pipeline"}},
	SupplyActionVerifyEffect:                       {SupplyActionVerifyEffect, "source_run_execution_unit", "verification_required", "cms", "reconciliation_event", "before_start_only", "none", "routine", 1, []string{"media_circulation"}},
	SupplyActionFinalizeVerifiedNoChange:           {SupplyActionFinalizeVerifiedNoChange, "source_run_execution_unit", "verifier_present_no_change", "cms", "terminal_request_proof", "before_start_only", "none", "routine", 1, []string{"media_circulation", "sources"}},
	SupplyActionCancelUnstarted:                    {SupplyActionCancelUnstarted, "source_run_execution_unit", "unstarted", "cms", "cancelled_terminal", "before_start_only", "none", "routine", 1, []string{"media_circulation", "sources"}},
	SupplyActionPipelineResumeExactStage:           {SupplyActionPipelineResumeExactStage, "content_item", "exact_stage_unverified", "aggregation_pipeline", "persisted_processing_event", "verify_after_effect", "forward_only", "high", 1, []string{"pipeline", "media_circulation"}},
	SupplyActionArtifactRequestTranscript:          {SupplyActionArtifactRequestTranscript, "content_item", "transcript_absent", "media", "persisted_correlated_transcript", "verify_after_effect", "forward_only", "high", 1, []string{"media", "enrichment", "media_circulation"}},
	SupplyActionArtifactRequestImageEmbedding:      {SupplyActionArtifactRequestImageEmbedding, "content_item", "image_embedding_absent", "media", "persisted_correlated_image_embedding", "verify_after_effect", "forward_only", "high", 1, []string{"media", "embeddings", "media_circulation"}},
	SupplyActionArtifactRequestTextEmbedding:       {SupplyActionArtifactRequestTextEmbedding, "content_item", "text_embedding_absent", "enrichment", "persisted_correlated_text_embedding", "verify_after_effect", "forward_only", "high", 1, []string{"enrichment", "embeddings", "media_circulation"}},
	SupplyActionArtifactRequestLLMMetadata:         {SupplyActionArtifactRequestLLMMetadata, "content_item", "llm_metadata_absent", "enrichment", "persisted_correlated_llm_metadata", "verify_after_effect", "forward_only", "high", 1, []string{"enrichment", "media_circulation"}},
	SupplyActionAtomizationExecuteExactParent:      {SupplyActionAtomizationExecuteExactParent, "content_item", "long_parent_ready_for_atomization", "aggregation_atomization", "verified_child_set", "verify_after_effect", "forward_only", "high", 1, []string{"media_atomization", "media_studio", "media_circulation"}},
	SupplyActionStudioClearExactChildren:           {SupplyActionStudioClearExactChildren, "atomization_work_request", "exact_child_set_pending_clearance", "cms_studio", "verified_studio_clearance", "verify_after_effect", "forward_only", "high", 1, []string{"media_studio", "media_circulation"}},
	SupplyActionFeedGenerationAttachVerifiedMember: {SupplyActionFeedGenerationAttachVerifiedMember, "content_item", "base_eligible_missing_from_active_generation", "cms", "verified_active_generation_membership", "before_start_only", "reversible_membership", "high", 1, []string{"feed_integrity", "media_circulation", "pods"}},
}

func SupplyAction(key string) (SupplyActionDescriptor, bool) {
	descriptor, ok := supplyActions[key]
	return descriptor, ok
}

func SupplyActionKeys() []string {
	return []string{SupplyActionRepairMissedAdmission, SupplyActionReclaimDispatchClaim, SupplyActionTransferUnitLease, SupplyActionAdoptUnitJob, SupplyActionRedeliverReceipt, SupplyActionVerifyEffect, SupplyActionFinalizeVerifiedNoChange, SupplyActionCancelUnstarted, SupplyActionPipelineResumeExactStage, SupplyActionArtifactRequestTranscript, SupplyActionArtifactRequestImageEmbedding, SupplyActionArtifactRequestTextEmbedding, SupplyActionArtifactRequestLLMMetadata, SupplyActionAtomizationExecuteExactParent, SupplyActionStudioClearExactChildren, SupplyActionFeedGenerationAttachVerifiedMember}
}
