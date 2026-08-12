package operator

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/supply"
	"content-management-system/src/tests/testdb"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

func fabricTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db := testdb.Open(t)
	if err := db.AutoMigrate(
		&models.OperatorCapabilityControl{}, &models.OperatorRecommendation{}, &models.OperatorRecommendationFeedback{},
		&models.ContentSource{}, &models.SourceRunRequest{}, &models.SourceRunAttempt{}, &models.SourceRunExecutionUnit{}, &models.SourceRunReceipt{}, &models.SourceRunVerificationTask{}, &models.SourceRunReconciliationEvent{}, &models.ContentProcessingEvent{}, &models.ContentItem{},
		&models.DiscoveryProfile{}, &models.SourceSuggestion{},
		&models.MediaCirculationPolicy{}, &models.MediaCirculationRun{}, &models.MediaCirculationRecommendation{},
		&models.MediaSupplyControl{}, &models.MediaSupplyEvaluationCheckpoint{}, &models.MediaSupplyEpisode{}, &models.MediaSupplyEpisodeEvent{},
		&models.NewsSnapshot{},
		&models.FeedRecoveryPlan{}, &models.FeedRecoveryRun{}, &models.FeedRecoveryAction{}, &models.FeedRecoveryLaneLease{}, &models.FeedRecoveryArtifact{}, &models.FeedAvailabilityState{}, &models.FeedGeneration{},
		&models.RetentionPolicy{}, &models.RetentionExecutionControl{}, &models.RetentionDBSample{}, &models.RetentionRun{}, &models.RetentionAction{}, &models.RetentionHold{}, &models.RetentionCompactionManifest{}, &models.RetentionHistoricalManifest{}, &models.RetentionRecoveryArtifact{}, &models.RetentionHistoricalRecoveryArtifact{}, &models.NewsMonthArchive{}, &models.RetentionOwnerRequest{}, &models.RetentionMaintenanceReport{},
	); err != nil {
		t.Fatalf("migrate context fabric test schema: %v", err)
	}
	clear := func() {
		_ = db.Exec("DELETE FROM operator_recommendation_feedback").Error
		_ = db.Exec("DELETE FROM operator_recommendations").Error
		_ = db.Exec("DELETE FROM operator_capability_controls").Error
		_ = db.Exec("DELETE FROM retention_maintenance_reports").Error
		_ = db.Exec("DELETE FROM retention_owner_requests").Error
		_ = db.Exec("DELETE FROM news_month_archives").Error
		_ = db.Exec("DELETE FROM retention_historical_recovery_artifacts").Error
		_ = db.Exec("DELETE FROM retention_recovery_artifacts").Error
		_ = db.Exec("DELETE FROM retention_historical_manifests").Error
		_ = db.Exec("DELETE FROM retention_compaction_manifests").Error
		_ = db.Exec("DELETE FROM retention_holds").Error
		_ = db.Exec("DELETE FROM retention_actions").Error
		_ = db.Exec("DELETE FROM retention_runs").Error
		_ = db.Exec("DELETE FROM retention_db_samples").Error
		_ = db.Exec("DELETE FROM retention_execution_controls").Error
		_ = db.Exec("DELETE FROM retention_policies").Error
		_ = db.Exec("DELETE FROM feed_recovery_actions").Error
		_ = db.Exec("DELETE FROM feed_recovery_artifacts").Error
		_ = db.Exec("DELETE FROM feed_recovery_lane_leases").Error
		_ = db.Exec("DELETE FROM feed_availability_states").Error
		_ = db.Exec("DELETE FROM feed_generations").Error
		_ = db.Exec("DELETE FROM feed_recovery_runs").Error
		_ = db.Exec("DELETE FROM feed_recovery_plans").Error
		_ = db.Exec("DELETE FROM media_circulation_recommendations").Error
		_ = db.Exec("DELETE FROM media_circulation_runs").Error
		_ = db.Exec("DELETE FROM media_circulation_policies").Error
		_ = db.Exec("DELETE FROM media_supply_episode_events").Error
		_ = db.Exec("DELETE FROM media_supply_episodes").Error
		_ = db.Exec("DELETE FROM media_supply_evaluation_checkpoints").Error
		_ = db.Exec("DELETE FROM media_supply_controls").Error
		_ = db.Exec("DELETE FROM news_snapshots").Error
		_ = db.Exec("DELETE FROM content_processing_events").Error
		_ = db.Exec("DELETE FROM source_suggestions").Error
		_ = db.Exec("DELETE FROM discovery_profiles").Error
		_ = db.Exec("DELETE FROM content_items").Error
		_ = db.Exec("DELETE FROM source_run_reconciliation_events").Error
		_ = db.Exec("DELETE FROM source_run_verification_tasks").Error
		_ = db.Exec("DELETE FROM source_run_receipts").Error
		_ = db.Exec("DELETE FROM source_run_execution_units").Error
		_ = db.Exec("DELETE FROM source_run_attempts").Error
		_ = db.Exec("DELETE FROM source_run_requests").Error
		_ = db.Exec("DELETE FROM content_sources").Error
	}
	clear()
	t.Cleanup(clear)
	return db
}

func approvalVisibleContext(sourceID string) VisibleContext {
	return VisibleContext{SchemaVersion: ContractVersion, Domain: "media_sources", View: "approval_handoff", Filters: map[string]any{}, Subjects: []SubjectRef{{Type: "content_source", ID: sourceID}}, AvailableIntents: []Intent{IntentExplain, IntentInvestigate}}
}

func mediaSourcesVisibleContext() VisibleContext {
	return VisibleContext{SchemaVersion: ContractVersion, Domain: "media_sources", View: "list", Filters: map[string]any{}, Subjects: []SubjectRef{}, AvailableIntents: []Intent{IntentExplain, IntentInvestigate, IntentRecommend}}
}

func approvalAccess() AccessSnapshot {
	return AccessSnapshot{UserID: "admin-a", TenantID: "tenant-a", Active: true, IsAdmin: true, AccessVersion: "access-v1"}
}

func mediaCirculationVisibleContext() VisibleContext {
	return VisibleContext{SchemaVersion: ContractVersion, Domain: "media_circulation", View: "cockpit", Filters: map[string]any{}, Subjects: []SubjectRef{{Type: "tenant", ID: "tenant-a"}}, AvailableIntents: []Intent{IntentExplain, IntentInvestigate, IntentRecommend}}
}

func feedRecoveryVisibleContext() VisibleContext {
	return VisibleContext{SchemaVersion: ContractVersion, Domain: "feed_recovery", View: "operations", Filters: map[string]any{}, Subjects: []SubjectRef{{Type: "tenant", ID: "tenant-a"}}, AvailableIntents: []Intent{IntentExplain, IntentInvestigate, IntentRecommend}}
}

func retentionVisibleContext() VisibleContext {
	return VisibleContext{SchemaVersion: ContractVersion, Domain: "retention", View: "custody", Filters: map[string]any{}, Subjects: []SubjectRef{{Type: "tenant", ID: "tenant-a"}}, AvailableIntents: []Intent{IntentExplain, IntentInvestigate, IntentRecommend}}
}

func feedIntegrityVisibleContext(window string) VisibleContext {
	return VisibleContext{SchemaVersion: ContractVersion, Domain: "feed_integrity", View: "snapshot", Filters: map[string]any{}, Subjects: []SubjectRef{{Type: "news_window", ID: window}}, Selection: &ExplicitSelection{Mode: "explicit", IDs: []string{window}, Count: 1}, AvailableIntents: []Intent{IntentExplain, IntentInvestigate, IntentRecommend, IntentResolve}}
}

func TestFeedIntegritySnapshotFabricBuildsExactRefreshContext(t *testing.T) {
	db := fabricTestDB(t)
	now := time.Date(2026, time.July, 31, 4, 0, 0, 0, time.UTC)
	snapshot := models.NewsSnapshot{TenantID: "tenant-a", Window: models.NewsWindowToday, SlideCount: 7, Dirty: true, Generation: 4, BuiltAt: now.Add(-time.Hour)}
	if err := db.Create(&snapshot).Error; err != nil {
		t.Fatal(err)
	}
	fabric := NewContextFabric(db, DefaultAdapterRegistry())
	fabric.now = func() time.Time { return now }
	packet, err := fabric.BuildFeedIntegritySnapshotPacket(context.Background(), feedIntegrityVisibleContext("today"), approvalAccess())
	if err != nil {
		t.Fatal(err)
	}
	if err := packet.Validate(); err != nil || len(packet.Evidence) != 1 || packet.Evidence[0].DeepLink != "/platform/feed-integrity" {
		t.Fatalf("unexpected exact snapshot packet=%#v err=%v", packet, err)
	}
	arguments, err := DefaultToolCatalog().DeriveArguments("feed_integrity.refresh_snapshot", []string{"today"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DefaultToolCatalog().BuildCanonicalPlan(packet, catalogAccess(), "feed_integrity.refresh_snapshot", []string{"today"}, arguments); err != nil {
		t.Fatalf("fresh exact snapshot evidence must support the registered plan: %v", err)
	}
}

func TestApprovalHandoffFabricUsesCMSLineageNotQueueInference(t *testing.T) {
	db := fabricTestDB(t)
	now := time.Date(2026, time.July, 31, 0, 0, 0, 0, time.UTC)
	source := models.ContentSource{TenantID: "tenant-a", Name: "Podcast source", Type: models.SourceTypePodcast, Category: models.SourceCategoryMedia}
	if err := db.Create(&source).Error; err != nil {
		t.Fatal(err)
	}
	run := models.SourceRunRequest{TenantID: "tenant-a", ContentSourceID: source.PublicID, RequestedBy: "approval_handoff", State: models.SourceRunCompleted, AggregationJobID: "fetch-42", CorrelationID: "correlation-42", IdempotencyKey: "source-run:42", RequestedAt: now, AcceptedAt: &now, StartedAt: &now, FinishedAt: &now}
	if err := db.Create(&run).Error; err != nil {
		t.Fatal(err)
	}
	event := models.ContentProcessingEvent{TenantID: "tenant-a", ContentSourceID: &source.PublicID, SourceRunRequestID: &run.ID, Stage: "text_embedding", State: "completed", Producer: "enrichment", EventClass: "text_embedding_persisted", OccurredAt: now}
	if err := db.Create(&event).Error; err != nil {
		t.Fatal(err)
	}
	title := "First episode"
	item := models.ContentItem{TenantID: "tenant-a", Type: models.ContentTypePodcast, Source: models.SourceTypePodcast, Status: models.ContentStatusReady, Title: &title, ContentSourceID: &source.PublicID, SourceRunRequestID: &run.ID}
	if err := db.Create(&item).Error; err != nil {
		t.Fatal(err)
	}
	fabric := NewContextFabric(db, DefaultAdapterRegistry())
	fabric.now = func() time.Time { return now.Add(time.Second) }
	packet, err := fabric.BuildApprovalHandoffPacket(context.Background(), approvalVisibleContext(source.PublicID.String()), approvalAccess())
	if err != nil {
		t.Fatal(err)
	}
	if err := packet.Validate(); err != nil {
		t.Fatal(err)
	}
	if len(packet.Evidence) < 3 || packet.Completeness != "complete" {
		t.Fatalf("unexpected packet: %+v", packet)
	}
	if len(packet.Recommendations) != 1 || packet.Recommendations[0].DeepLink != "/platform/media/sources" {
		t.Fatalf("approval handoff must expose one registered inspection recommendation: %#v", packet.Recommendations)
	}
	queueProven := false
	for _, fact := range packet.Facts {
		if fact.Key == "approval_handoff.queue_proven" {
			value, ok := fact.Value.(bool)
			queueProven = ok && value
		}
	}
	if !queueProven {
		t.Fatal("accepted CMS run must prove handoff without a queue query")
	}
}

func TestApprovalHandoffFabricReportsUnknownWhenNoRunLedgerExists(t *testing.T) {
	db := fabricTestDB(t)
	source := models.ContentSource{TenantID: "tenant-a", Name: "Unstarted source", Type: models.SourceTypeRSS, Category: models.SourceCategoryMedia}
	if err := db.Create(&source).Error; err != nil {
		t.Fatal(err)
	}
	fabric := NewContextFabric(db, DefaultAdapterRegistry())
	packet, err := fabric.BuildApprovalHandoffPacket(context.Background(), approvalVisibleContext(source.PublicID.String()), approvalAccess())
	if err != nil {
		t.Fatal(err)
	}
	if len(packet.Unknowns) != 2 {
		t.Fatalf("expected an honest missing-run/item response, got %#v", packet.Unknowns)
	}
	for _, fact := range packet.Facts {
		if fact.Key == "approval_handoff.queue_proven" && fact.Value != false {
			t.Fatalf("missing ledger must not infer queue state: %#v", fact)
		}
	}
	rawPacket, err := json.Marshal(packet)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"purge_manifest", "artifact_key", "reauth_proof", "password"} {
		if strings.Contains(string(rawPacket), forbidden) {
			t.Fatalf("recovery packet leaked prohibited native material %q: %s", forbidden, rawPacket)
		}
	}
}

func TestWholeConsoleMediaSourcesCoverageUsesTenantScopedRecords(t *testing.T) {
	db := fabricTestDB(t)
	mediaSource := models.ContentSource{TenantID: "tenant-a", Name: "Tenant A media", Type: models.SourceTypePodcast, Category: models.SourceCategoryMedia}
	foreignSource := models.ContentSource{TenantID: "tenant-b", Name: "Tenant B media", Type: models.SourceTypePodcast, Category: models.SourceCategoryMedia}
	if err := db.Create(&mediaSource).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&foreignSource).Error; err != nil {
		t.Fatal(err)
	}
	fabric := NewContextFabric(db, DefaultAdapterRegistry())
	packet, err := fabric.BuildPacket(context.Background(), mediaSourcesVisibleContext(), approvalAccess())
	if err != nil {
		t.Fatal(err)
	}
	if err := packet.Validate(); err != nil {
		t.Fatal(err)
	}
	var source map[string]any
	for _, fact := range packet.Facts {
		if fact.Key == "media_sources.source" {
			source = fact.Value.(map[string]any)
		}
	}
	if source == nil || source["tenant_id"] != "tenant-a" || source["name"] != "Tenant A media" {
		t.Fatalf("media-source coverage leaked or omitted tenant data: %#v", source)
	}
}

func TestApprovalHandoffFabricRejectsUnregisteredPermission(t *testing.T) {
	fabric := NewContextFabric(nil, DefaultAdapterRegistry())
	access := approvalAccess()
	access.IsAdmin = false
	access.Permissions = []string{"feed:read"}
	if _, err := fabric.BuildApprovalHandoffPacket(context.Background(), approvalVisibleContext("00000000-0000-4000-8000-000000000001"), access); err == nil {
		t.Fatal("source-read permission is required before any adapter read")
	}
}

func TestMediaCirculationFabricUsesBoundedPersistedCMSFacts(t *testing.T) {
	db := fabricTestDB(t)
	now := time.Date(2026, time.July, 31, 0, 0, 0, 0, time.UTC)
	policy := models.DefaultMediaCirculationPolicy("tenant-a")
	policy.Enabled = true
	policy.AutopilotEnabled = true
	policy.AutopilotLastRunAt = &now
	if err := db.Create(&policy).Error; err != nil {
		t.Fatal(err)
	}
	run := models.MediaCirculationRun{TenantID: "tenant-a", Trigger: "scheduled", Mode: "observe", Status: "completed", StartedAt: now, FinishedAt: &now, Summary: "evaluated safely"}
	if err := db.Create(&run).Error; err != nil {
		t.Fatal(err)
	}
	recommendation := models.MediaCirculationRecommendation{TenantID: "tenant-a", UnitType: models.MediaCirculationUnitSource, SubjectID: run.PublicID, SubjectKind: "content_source", Verdict: "pull_now", Action: "pull", Score: 0.9, Status: models.MediaCirculationRecStatusPending}
	if err := db.Create(&recommendation).Error; err != nil {
		t.Fatal(err)
	}
	fabric := NewContextFabric(db, DefaultAdapterRegistry())
	fabric.now = func() time.Time { return now.Add(time.Second) }
	packet, err := fabric.BuildMediaCirculationStatePacket(context.Background(), mediaCirculationVisibleContext(), approvalAccess())
	if err != nil {
		t.Fatal(err)
	}
	if err := packet.Validate(); err != nil {
		t.Fatal(err)
	}
	if len(packet.Facts) != 8 || len(packet.Evidence) != 8 || packet.Completeness != "complete" {
		t.Fatalf("unexpected circulation packet: %+v", packet)
	}
	if len(packet.Recommendations) != 1 || packet.Recommendations[0].ManualOnly {
		t.Fatalf("circulation recommendation must remain read-only guidance: %#v", packet.Recommendations)
	}
	for _, fact := range packet.Facts {
		if fact.Key == "media_circulation.recommendations" {
			value := fact.Value.(map[string]any)
			if value["sampled_count"] != 1 || value["truncated"] != false {
				t.Fatalf("unexpected bounded recommendation fact: %#v", value)
			}
		}
		if fact.Key == "media_circulation.recommendation_score_trend" {
			value := fact.Value.(map[string]any)
			if value["direction"] != "unchanged" || value["sampled_count"] != 1 {
				t.Fatalf("unexpected evidence-bound score trend: %#v", value)
			}
		}
	}
}

func TestMediaCirculationFabricIncludesBoundedSourceContinuityEvidence(t *testing.T) {
	db := fabricTestDB(t)
	now := time.Date(2026, time.August, 9, 12, 0, 0, 0, time.UTC)
	past := now.Add(-time.Minute)
	source := models.ContentSource{TenantID: "tenant-a", Name: "Due podcast", Type: models.SourceTypePodcast, Category: models.SourceCategoryMedia, NextDueAt: &past}
	if err := db.Create(&source).Error; err != nil {
		t.Fatal(err)
	}
	request := models.SourceRunRequest{TenantID: "tenant-a", ContentSourceID: source.PublicID, Lane: models.SourceCategoryMedia, Purpose: "circulation", RequestedBy: "schedule", State: models.SourceRunSucceeded, EvidenceState: "verified", RequestedAt: now.Add(-time.Hour)}
	if err := db.Create(&request).Error; err != nil {
		t.Fatal(err)
	}
	task := models.SourceRunVerificationTask{TenantID: "tenant-a", TaskKey: "delivery", SourceRunRequestID: request.PublicID, ContentSourceID: source.PublicID, EffectIdentity: "pods", ScopeType: "source_run", ScopeID: request.PublicID.String(), Stage: "delivery", EvidenceBoundary: "pods", CausationID: "consumer_pods_delivery:test", VerifierName: "cms", VerifierSchemaVersion: "v1", State: models.SourceRunVerificationTaskTerminal, TerminalVerdict: string(supply.VerdictPresent)}
	if err := db.Create(&task).Error; err != nil {
		t.Fatal(err)
	}
	event := models.SourceRunReconciliationEvent{TenantID: "tenant-a", EventKey: "pods-present", SourceRunRequestID: request.PublicID, ContentSourceID: source.PublicID, EffectIdentity: "pods", ScopeType: "source_run", ScopeID: request.PublicID.String(), Stage: "delivery", Verdict: string(supply.VerdictPresent), EvidenceSnapshot: "snapshot", VerifierSchemaVersion: "v1", VerificationTaskID: task.PublicID, CausationID: "consumer_pods_delivery:test", ProvenanceDigest: "digest", ObservedAt: now}
	if err := db.Create(&event).Error; err != nil {
		t.Fatal(err)
	}
	digest := "evaluation-digest"
	checkpoint := models.MediaSupplyEvaluationCheckpoint{TenantID: "tenant-a", LastTrigger: models.MediaSupplyEvaluationTriggerScheduled, LastOutcome: models.MediaSupplyEvaluationOutcomeEvaluated, LastObservedAt: now, LastEvaluatedAt: &now, EvaluationDigest: &digest}
	if err := db.Create(&checkpoint).Error; err != nil {
		t.Fatal(err)
	}
	episode := models.MediaSupplyEpisode{PublicID: uuid.New(), TenantID: "tenant-a", Fingerprint: "supply-fingerprint", FirstFailedBoundary: "cms_admission", Verdict: string(supply.SupplyVerdictSourceDueNotAdmitted), Severity: "major", Owner: "CMS source-run scheduler", State: models.MediaSupplyEpisodeOpen, Summary: "Due source has no active run.", AffectedSubjects: datatypes.JSON([]byte(`[]`)), EvidenceDigest: digest, EvidenceCompleteness: "complete", Evidence: datatypes.JSON([]byte(`{}`)), FirstSeenAt: now.Add(-time.Minute), LastSeenAt: now}
	if err := db.Create(&episode).Error; err != nil {
		t.Fatal(err)
	}
	fabric := NewContextFabric(db, DefaultAdapterRegistry())
	fabric.now = func() time.Time { return now }
	packet, err := fabric.BuildMediaCirculationStatePacket(context.Background(), mediaCirculationVisibleContext(), approvalAccess())
	if err != nil {
		t.Fatal(err)
	}
	var continuity, headline, evaluator, attention map[string]any
	for _, fact := range packet.Facts {
		if fact.Key == "media_circulation.source_continuity" {
			continuity = fact.Value.(map[string]any)
		}
		if fact.Key == "media_circulation.supply_continuity" {
			headline = fact.Value.(map[string]any)
		}
		if fact.Key == "media_circulation.supply_evaluator" {
			evaluator = fact.Value.(map[string]any)
		}
		if fact.Key == "media_circulation.supply_attention" {
			attention = fact.Value.(map[string]any)
		}
	}
	if continuity == nil || continuity["sampled_count"] != 1 || continuity["legacy_last_fetched_is_not_provider_success"] != true {
		t.Fatalf("missing source continuity fact: %#v", continuity)
	}
	rows := continuity["sources"].([]map[string]any)
	if rows[0]["schedule_state"] != "due_unadmitted" || rows[0]["delivery_state"] != "verified" || rows[0]["pods_verdict"] != string(supply.VerdictPresent) {
		t.Fatalf("unexpected source continuity row: %#v", rows[0])
	}
	if headline == nil || headline["verdict"] != string(supply.SupplyVerdictSourceDueNotAdmitted) || headline["read_only"] != true {
		t.Fatalf("missing deterministic supply headline: %#v", headline)
	}
	if evaluator == nil || evaluator["checkpoint_present"] != true || evaluator["checkpoint_last_outcome"] != models.MediaSupplyEvaluationOutcomeEvaluated {
		t.Fatalf("missing evaluator checkpoint evidence: %#v", evaluator)
	}
	if attention == nil || attention["sampled_count"] != 1 || attention["truncated"] != false {
		t.Fatalf("missing bounded supply attention evidence: %#v", attention)
	}
	attentionRows := attention["episodes"].([]map[string]any)
	if len(attentionRows) != 1 || attentionRows[0]["episode_id"] != episode.PublicID.String() || attentionRows[0]["state"] != models.MediaSupplyEpisodeOpen {
		t.Fatalf("unexpected attention episode projection: %#v", attentionRows)
	}
}

func TestMediaCirculationFabricDoesNotReadOtherTenantRecommendations(t *testing.T) {
	db := fabricTestDB(t)
	now := time.Date(2026, time.July, 31, 0, 0, 0, 0, time.UTC)
	for _, tenantID := range []string{"tenant-a", "tenant-b"} {
		recommendation := models.MediaCirculationRecommendation{TenantID: tenantID, UnitType: models.MediaCirculationUnitSource, SubjectID: uuid.New(), SubjectKind: "content_source", Verdict: "pull_now", Action: "pull", Score: 0.9, Status: models.MediaCirculationRecStatusPending}
		if err := db.Create(&recommendation).Error; err != nil {
			t.Fatal(err)
		}
	}
	fabric := NewContextFabric(db, DefaultAdapterRegistry())
	fabric.now = func() time.Time { return now.Add(time.Second) }
	packet, err := fabric.BuildMediaCirculationStatePacket(context.Background(), mediaCirculationVisibleContext(), approvalAccess())
	if err != nil {
		t.Fatal(err)
	}
	for _, fact := range packet.Facts {
		if fact.Key == "media_circulation.recommendations" {
			if fact.Value.(map[string]any)["sampled_count"] != 1 {
				t.Fatalf("tenant-scoped collection included foreign recommendations: %#v", fact.Value)
			}
		}
	}
}

func TestMediaCirculationFabricRejectsWrongDomain(t *testing.T) {
	fabric := NewContextFabric(nil, DefaultAdapterRegistry())
	if _, err := fabric.BuildMediaCirculationStatePacket(context.Background(), approvalVisibleContext("00000000-0000-4000-8000-000000000001"), approvalAccess()); err == nil {
		t.Fatal("media circulation adapter must reject a non-canonical visible context")
	}
}

func TestFeedRecoveryFabricSurfacesStateWithoutDestructiveTargets(t *testing.T) {
	db := fabricTestDB(t)
	now := time.Date(2026, time.July, 31, 0, 0, 0, 0, time.UTC)
	plan := models.FeedRecoveryPlan{TenantID: "tenant-a", Lane: "news", Level: "repair", CapacityMode: "bounded", State: "awaiting_approval", PlanHash: "plan-hash", ManifestHash: "manifest-hash", SourceChecksum: "source-hash", ExpiresAt: now.Add(time.Hour), TargetCount: 3}
	if err := db.Create(&plan).Error; err != nil {
		t.Fatal(err)
	}
	candidateID := uuid.New()
	generation := models.FeedGeneration{PublicID: candidateID, TenantID: "tenant-a", Lane: "news", State: "candidate", Verification: []byte(`{"caught_up":true,"dual_write_reconciled":true}`)}
	if err := db.Create(&generation).Error; err != nil {
		t.Fatal(err)
	}
	run := models.FeedRecoveryRun{PlanID: plan.ID, TenantID: "tenant-a", Lane: "news", CorrelationID: plan.PublicID, Phase: "verification", Outcome: "succeeded", CandidateGenerationID: &candidateID}
	if err := db.Create(&run).Error; err != nil {
		t.Fatal(err)
	}
	action := models.FeedRecoveryAction{RunID: run.ID, ActionType: "repair_memberships", State: "succeeded", IdempotencyKey: "repair-1"}
	if err := db.Create(&action).Error; err != nil {
		t.Fatal(err)
	}
	verification := models.FeedRecoveryAction{RunID: run.ID, ActionType: "verification_probe_1", State: "succeeded", IdempotencyKey: "verify-1", Evidence: []byte(`{"feed_integrity_headline":"all_clear","system_health_overall_healthy":true,"gate_matrix":{"feed_integrity_all_clear":true},"inventory":[{"lane":"news","news_page_slides":12,"news_sources":3,"news_dominant_source_pct":0.25}]}`)}
	if err := db.Create(&verification).Error; err != nil {
		t.Fatal(err)
	}
	lease := models.FeedRecoveryLaneLease{TenantID: "tenant-a", Lane: "news", RunID: run.ID, FencingToken: uuid.New(), AcquiredAt: now, HeartbeatAt: now, ExpiresAt: now.Add(time.Minute)}
	if err := db.Create(&lease).Error; err != nil {
		t.Fatal(err)
	}
	availability := models.FeedAvailabilityState{TenantID: "tenant-a", Lane: "news", State: "normal", UpdatedAt: now}
	if err := db.Create(&availability).Error; err != nil {
		t.Fatal(err)
	}
	artifact := models.FeedRecoveryArtifact{PlanID: plan.ID, TenantID: "tenant-a", ArtifactType: "recovery_map", ArtifactKey: "system/recovery/private", SHA256: "artifact-hash", ByteSize: 512, State: "verified", ExpiresAt: now.Add(time.Hour)}
	if err := db.Create(&artifact).Error; err != nil {
		t.Fatal(err)
	}
	fabric := NewContextFabric(db, DefaultAdapterRegistry())
	fabric.now = func() time.Time { return now.Add(time.Second) }
	packet, err := fabric.BuildFeedRecoveryStatePacket(context.Background(), feedRecoveryVisibleContext(), approvalAccess())
	if err != nil {
		t.Fatal(err)
	}
	if err := packet.Validate(); err != nil {
		t.Fatal(err)
	}
	if len(packet.Facts) != 8 || len(packet.Evidence) != 8 {
		t.Fatalf("unexpected recovery packet: %+v", packet)
	}
	if len(packet.Recommendations) != 1 || !packet.Recommendations[0].ManualOnly {
		t.Fatalf("recovery recommendation must preserve manual-only boundary: %#v", packet.Recommendations)
	}
	for _, fact := range packet.Facts {
		if fact.Key == "feed_recovery.actions" {
			value := fact.Value.(map[string]any)
			if value["destructive_controls_manual_only"] != true {
				t.Fatalf("recovery packet lost manual-only boundary: %#v", value)
			}
		}
		if fact.Key == "feed_recovery.verification" {
			value := fact.Value.(map[string]any)
			proof := value["proof"].(map[string]any)
			if value["includes_integrity_health_page_diversity_playback"] != true || proof["feed_integrity_headline"] != "all_clear" || proof["system_health_overall_healthy"] != true {
				t.Fatalf("recovery verification proof is incomplete: %#v", value)
			}
		}
		if fact.Key == "feed_recovery.artifacts" {
			artifactRows := fact.Value.(map[string]any)["artifacts"].([]map[string]any)
			if len(artifactRows) != 1 || artifactRows[0]["type"] != "recovery_map" || artifactRows[0]["artifact_key"] != nil {
				t.Fatalf("artifact proof must be bounded and omit object keys: %#v", artifactRows)
			}
		}
	}
}

func TestRetentionFabricSurfacesCustodyWithoutExecutionAdmission(t *testing.T) {
	db := fabricTestDB(t)
	now := time.Date(2026, time.July, 31, 0, 0, 0, 0, time.UTC)
	policy := models.DefaultRetentionPolicy("tenant-a")
	policy.Enabled = true
	if err := db.Create(&policy).Error; err != nil {
		t.Fatal(err)
	}
	sample := models.RetentionDBSample{TenantID: "tenant-a", DatabaseBytes: 1024, AllocatedBytes: 2048, ReusableBytes: 512, MeasuredAt: now, ProviderSource: "local"}
	if err := db.Create(&sample).Error; err != nil {
		t.Fatal(err)
	}
	run := models.RetentionRun{TenantID: "tenant-a", Lane: "database", Trigger: "scheduled", Mode: "observe", Status: "completed", Verdict: "healthy", PolicyVersion: policy.PolicyVersion, StartedAt: now, HeartbeatAt: now}
	if err := db.Create(&run).Error; err != nil {
		t.Fatal(err)
	}
	control := models.RetentionExecutionControl{TenantID: "tenant-a", CanonicalCompactionEnabled: true, HistoricalEnabled: true, OwnerRunsEnabled: true}
	if err := db.Create(&control).Error; err != nil {
		t.Fatal(err)
	}
	action := models.RetentionAction{RunID: run.ID, TenantID: "tenant-a", ActionClass: models.RetentionActionRefreshNewsSnapshots, OwnerSystem: "news_database", TargetScope: "windows:today", Mode: models.RetentionModeAssist, Decision: "approval_required", Outcome: models.RetentionActionApprovalRequired, IdempotencyKey: "retention-action-1", EvidenceFingerprint: "retention-evidence"}
	if err := db.Create(&action).Error; err != nil {
		t.Fatal(err)
	}
	hold := models.RetentionHold{TenantID: "tenant-a", TargetType: "content_item", TargetID: uuid.New(), HoldClass: "legal", Reason: "test", CreatedBy: "admin-a"}
	if err := db.Create(&hold).Error; err != nil {
		t.Fatal(err)
	}
	manifestJSON := []byte(`[]`)
	compaction := models.RetentionCompactionManifest{RunID: run.ID, TenantID: "tenant-a", PolicyVersion: policy.PolicyVersion, Timezone: "Asia/Riyadh", ManifestHash: "compaction-manifest", State: "prepared", StoryIDs: manifestJSON, AnchorContentIDs: manifestJSON, ProtectedContentIDs: manifestJSON, RetireContentIDs: manifestJSON, Evidence: []byte(`{}`), StoryCount: 1, AnchorCount: 1, ProtectedCount: 1, RetireCount: 0, ExpiresAt: now.Add(time.Hour)}
	if err := db.Create(&compaction).Error; err != nil {
		t.Fatal(err)
	}
	historical := models.RetentionHistoricalManifest{RunID: run.ID, TenantID: "tenant-a", PolicyVersion: policy.PolicyVersion, Timezone: "Asia/Riyadh", ManifestHash: "historical-manifest", State: "prepared", ContentIDs: manifestJSON, StoryIDs: manifestJSON, Evidence: []byte(`{}`), ContentCount: 0, StoryCount: 1, ExpiresAt: now.Add(time.Hour)}
	if err := db.Create(&historical).Error; err != nil {
		t.Fatal(err)
	}
	artifact := models.RetentionRecoveryArtifact{ActionID: action.ID, ManifestID: compaction.ID, TenantID: "tenant-a", ArtifactKey: "private/recovery-map", SHA256: "artifact", CompressedBytes: 12, UncompressedBytes: 24, State: "verified", ExpiresAt: now.Add(time.Hour)}
	if err := db.Create(&artifact).Error; err != nil {
		t.Fatal(err)
	}
	historicalArtifact := models.RetentionHistoricalRecoveryArtifact{ActionID: action.ID + 1, ManifestID: historical.ID, TenantID: "tenant-a", ArtifactKey: "private/historical-map", SHA256: "historical-artifact", CompressedBytes: 12, UncompressedBytes: 24, State: "verified", ExpiresAt: now.Add(time.Hour)}
	if err := db.Create(&historicalArtifact).Error; err != nil {
		t.Fatal(err)
	}
	archive := models.NewsMonthArchive{TenantID: "tenant-a", MonthStart: now.AddDate(0, -1, 0), Timezone: "Asia/Riyadh", Revision: 1, PolicyVersionID: 1, State: "finalized", Headline: "Review", Introduction: "Review", Sections: []byte(`[]`), HeadlineAR: "مراجعة", IntroductionAR: "مراجعة", SectionsAR: []byte(`[]`), SelectionManifest: []byte(`{}`), SelectionHash: "selection", CompositionHash: "composition", QualifiedCount: 4, SelectedCount: 2, Verification: []byte(`{}`), BuiltAt: now, FinalizedAt: &now}
	if err := db.Create(&archive).Error; err != nil {
		t.Fatal(err)
	}
	owner := models.RetentionOwnerRequest{ActionID: action.ID, TenantID: "tenant-a", OwnerSystem: "storage", IdempotencyKey: "owner-1", RequestHash: "owner-request", AllowedActionClasses: []byte(`[]`), MaxBytes: 1024, MaxItems: 5, MaxActions: 1, ExpiresAt: now.Add(time.Hour), Status: "prepared", Result: []byte(`{}`)}
	if err := db.Create(&owner).Error; err != nil {
		t.Fatal(err)
	}
	maintenance := models.RetentionMaintenanceReport{TenantID: "tenant-a", DatabaseBytes: 1024, TargetBytes: 2048, ProviderSource: "local", ProviderFresh: true, PostgresReady: true, ProviderReady: true, BlockingReasons: []byte(`[]`), State: "measured", Evidence: []byte(`{}`)}
	if err := db.Create(&maintenance).Error; err != nil {
		t.Fatal(err)
	}
	fabric := NewContextFabric(db, DefaultAdapterRegistry())
	fabric.now = func() time.Time { return now.Add(time.Second) }
	packet, err := fabric.BuildRetentionStatePacket(context.Background(), retentionVisibleContext(), approvalAccess())
	if err != nil {
		t.Fatal(err)
	}
	if err := packet.Validate(); err != nil {
		t.Fatal(err)
	}
	if len(packet.Facts) != 12 || len(packet.Evidence) != 12 {
		t.Fatalf("unexpected retention packet: %+v", packet)
	}
	if len(packet.Recommendations) != 1 || !packet.Recommendations[0].ManualOnly {
		t.Fatalf("retention recommendation must preserve manual-only boundary: %#v", packet.Recommendations)
	}
	for _, fact := range packet.Facts {
		if fact.Key == "retention.runs" {
			value := fact.Value.(map[string]any)
			if value["destructive_execution_manual_only"] != true {
				t.Fatalf("retention packet lost manual-only boundary: %#v", value)
			}
		}
		if fact.Key == "retention.recovery_artifacts" {
			for _, artifact := range fact.Value.(map[string]any)["artifacts"].([]map[string]any) {
				if artifact["artifact_key"] != nil {
					t.Fatalf("recovery artifact key leaked: %#v", artifact)
				}
			}
		}
		if fact.Key == "retention.owner_requests" && fact.Value.(map[string]any)["owner_execution_manual_only"] != true {
			t.Fatalf("owner execution boundary was lost: %#v", fact.Value)
		}
	}
}
