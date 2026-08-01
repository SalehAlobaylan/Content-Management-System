package operator

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func catalogPacket() DecisionPacket {
	now := time.Now().UTC()
	return DecisionPacket{
		SchemaVersion: ContractVersion, PacketID: "packet-1", Fingerprint: "packet-hash", TenantID: "tenant-a", ActorID: "admin-a",
		VisibleContext:      VisibleContext{SchemaVersion: ContractVersion, Domain: "feed_integrity", View: "episode", Filters: map[string]any{}, Subjects: []SubjectRef{{Type: "feed_window", ID: "today"}}, Selection: &ExplicitSelection{Mode: "explicit", IDs: []string{"today"}, Count: 1}, AvailableIntents: []Intent{IntentResolve}},
		CollectionStartedAt: now, CollectionEndedAt: now, Completeness: "complete",
		Evidence: []EvidenceRef{{EvidenceID: "ev-1", Authority: EvidenceLive, Domain: "feed_integrity", AdapterKey: "feed_integrity.state", AdapterVersion: "v1", TenantID: "tenant-a", RequiredPermission: "feed:read", DeepLink: "/platform/feed-integrity", ObservedAt: now, FetchedAt: now, MaxAgeSeconds: 60, ExpiresAt: now.Add(time.Minute), ContentHash: "hash", SourceVersion: "1", Availability: EvidenceAvailable}},
		Facts:    []Fact{{Key: "feed_integrity.window", Value: "today", EvidenceIDs: []string{"ev-1"}}},
	}
}

func catalogAccess() AccessSnapshot {
	return AccessSnapshot{UserID: "admin-a", TenantID: "tenant-a", Active: true, Permissions: []string{"feed:manage"}, AccessVersion: "v1"}
}

func TestCatalogBuildsOnlyRegisteredExplicitFreshPlan(t *testing.T) {
	plan, err := DefaultToolCatalog().BuildCanonicalPlan(catalogPacket(), catalogAccess(), "feed_integrity.refresh_snapshot", []string{"today"}, map[string]any{"window": "today"})
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.Validate(); err != nil {
		t.Fatal(err)
	}
	if plan.ToolVersion != "v1" || plan.TargetIDs[0] != "today" {
		t.Fatalf("unexpected plan: %#v", plan)
	}
	if plan.Cancellation != "before_start_only" || plan.Rollback != "not_required_idempotent_refresh" || len(plan.Contingencies) != 2 {
		t.Fatalf("expected signed lifecycle contract, got %#v", plan)
	}
}

func TestCatalogListsOnlyStaticDescriptorsForRequestedDomain(t *testing.T) {
	items := DefaultToolCatalog().ListForDomain("media_sources")
	if len(items) == 0 {
		t.Fatal("expected admitted media-source actions")
	}
	for _, item := range items {
		if item.OwnerDomain != "media_sources" || item.Key == "" {
			t.Fatalf("catalog leaked an invalid descriptor: %#v", item)
		}
	}
	if got := DefaultToolCatalog().ListForDomain("not-a-domain"); len(got) != 0 {
		t.Fatalf("unknown domains must not acquire actions: %#v", got)
	}
}

func TestCatalogDerivesFixedArgumentsFromExactTargetOnly(t *testing.T) {
	arguments, err := DefaultToolCatalog().DeriveArguments("feed_integrity.refresh_snapshot", []string{"week"})
	if err != nil || arguments["window"] != "week" {
		t.Fatalf("expected fixed window argument, arguments=%#v err=%v", arguments, err)
	}
	if _, err := DefaultToolCatalog().DeriveArguments("feed_integrity.refresh_snapshot", []string{"../../admin"}); err == nil {
		t.Fatal("an arbitrary target must not become a tool argument")
	}
}

func TestCatalogAdmitsOnlyBoundedCorrelatedSourceRuns(t *testing.T) {
	id := uuid.NewString()
	arguments, err := DefaultToolCatalog().DeriveArguments("sources.run_once", []string{id})
	if err != nil || arguments["source_id"] != id {
		t.Fatalf("expected exact source id argument, arguments=%#v err=%v", arguments, err)
	}
	if _, err := DefaultToolCatalog().DeriveArguments("media_sources.run_once", []string{"not-a-uuid"}); err == nil {
		t.Fatal("media source run must reject an untyped target")
	}
	for _, key := range []string{"sources.pause", "sources.resume", "media_sources.pause", "media_sources.resume"} {
		arguments, err = DefaultToolCatalog().DeriveArguments(key, []string{id})
		if err != nil || arguments["source_id"] != id {
			t.Fatalf("%s must accept exactly one source UUID: arguments=%#v err=%v", key, arguments, err)
		}
	}
}

func TestCatalogDerivesGovernanceArgumentsFromStaticKeys(t *testing.T) {
	id := uuid.NewString()
	arguments, err := DefaultToolCatalog().DeriveArguments("operator.schedule.create.daily", []string{id})
	if err != nil || arguments["investigation_id"] != id || arguments["cadence_minutes"] != 24*60 {
		t.Fatalf("daily schedule must have server-derived cadence, arguments=%#v err=%v", arguments, err)
	}
	arguments, err = DefaultToolCatalog().DeriveArguments("operator.recommendation.snooze.1h", []string{id})
	if err != nil || arguments["recommendation_id"] != id || arguments["snooze_minutes"] != 60 {
		t.Fatalf("snooze must have server-derived duration, arguments=%#v err=%v", arguments, err)
	}
	if _, err = DefaultToolCatalog().DeriveArguments("operator.control.disable.execution", []string{"anything_else"}); err == nil {
		t.Fatal("control disables must reject arbitrary targets")
	}
	if _, err = DefaultToolCatalog().DeriveArguments("operator.control.disable.adapter", []string{"not-a-registered-adapter"}); err == nil {
		t.Fatal("adapter control must reject unknown adapter keys")
	}
	arguments, err = DefaultToolCatalog().DeriveArguments("operator.control.disable.tool", []string{"feed_integrity.refresh_snapshot"})
	if err != nil || arguments["capability_key"] != "feed_integrity.refresh_snapshot" {
		t.Fatalf("tool control must use an existing static tool, arguments=%#v err=%v", arguments, err)
	}
	shareTarget := uuid.NewString() + "|recipient-admin"
	arguments, err = DefaultToolCatalog().DeriveArguments("operator.share.create", []string{shareTarget})
	if err != nil || arguments["recipient_id"] != "recipient-admin" {
		t.Fatalf("share must require a typed investigation/recipient target, arguments=%#v err=%v", arguments, err)
	}
	if _, err = DefaultToolCatalog().DeriveArguments("operator.share.create", []string{uuid.NewString() + "|"}); err == nil {
		t.Fatal("share must reject a missing recipient")
	}
}

func TestCatalogDerivesBoundedSuppressionArguments(t *testing.T) {
	id := uuid.NewString()
	arguments, err := DefaultToolCatalog().DeriveArguments("feed_integrity.suppress_episode.1h", []string{id})
	if err != nil || arguments["episode_id"] != id || arguments["ttl_minutes"] != 60 {
		t.Fatalf("feed suppression must use a fixed one-hour contract, arguments=%#v err=%v", arguments, err)
	}
	arguments, err = DefaultToolCatalog().DeriveArguments("real_experience.revoke_suppression", []string{id})
	if err != nil || arguments["suppression_id"] != id {
		t.Fatalf("experience revoke must use one exact suppression target, arguments=%#v err=%v", arguments, err)
	}
	if _, err = DefaultToolCatalog().DeriveArguments("feed_integrity.suppress_episode.1h", []string{"not-a-uuid"}); err == nil {
		t.Fatal("suppression target must be a typed UUID")
	}
}

func TestCatalogAdmitsOnlyFixedCurrentDomainPauseArguments(t *testing.T) {
	for _, key := range []string{
		"feed_integrity.pause.24h", "real_experience.pause.24h", "retention.pause.24h", "ai_economics.pause.24h",
		"news_circulation.pause.24h", "media_circulation.pause.24h", "redundancy.pause.24h", "pipeline.pause.24h",
		"enrichment.pause.24h", "embeddings.pause_campaigns.24h", "topics_preferences.pause.24h", "media_library.pause.24h",
	} {
		arguments, err := DefaultToolCatalog().DeriveArguments(key, []string{"current"})
		if err != nil || arguments["duration_minutes"] != 24*60 {
			t.Fatalf("%s must derive only its fixed 24-hour brake: arguments=%#v err=%v", key, arguments, err)
		}
		if _, err := DefaultToolCatalog().DeriveArguments(key, []string{"any-policy-id"}); err == nil {
			t.Fatalf("%s must reject browser-controlled pause targets", key)
		}
	}
}

func TestCatalogAllowsStaleEvidenceOnlyForContainmentPause(t *testing.T) {
	now := time.Now().UTC()
	packet := DecisionPacket{
		SchemaVersion: ContractVersion, PacketID: "pipeline-packet", Fingerprint: "pipeline-hash", TenantID: "tenant-a", ActorID: "admin-a",
		VisibleContext:      VisibleContext{SchemaVersion: ContractVersion, Domain: "pipeline", View: "cockpit", Filters: map[string]any{}, Subjects: []SubjectRef{{Type: "tenant", ID: "current"}}, Selection: &ExplicitSelection{Mode: "explicit", IDs: []string{"current"}, Count: 1}, AvailableIntents: []Intent{IntentResolve}},
		CollectionStartedAt: now, CollectionEndedAt: now, Completeness: "complete",
		Evidence: []EvidenceRef{{EvidenceID: "pipeline-policy", Authority: EvidenceLive, Domain: "pipeline", AdapterKey: "operator.pipeline.state", AdapterVersion: "v1", TenantID: "tenant-a", RequiredPermission: "aggregation:read", DeepLink: "/platform/pipeline", ObservedAt: now.Add(-2 * time.Minute), FetchedAt: now, MaxAgeSeconds: 60, ExpiresAt: now.Add(time.Minute), ContentHash: "hash", SourceVersion: "old", Availability: EvidenceStale}},
		Facts:    []Fact{{Key: "pipeline.policy", Value: map[string]any{"paused": false}, EvidenceIDs: []string{"pipeline-policy"}}},
	}
	access := AccessSnapshot{UserID: "admin-a", TenantID: "tenant-a", Active: true, Permissions: []string{"aggregation:manage"}, AccessVersion: "v1"}
	arguments, err := DefaultToolCatalog().DeriveArguments("pipeline.pause.24h", []string{"current"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DefaultToolCatalog().BuildCanonicalPlan(packet, access, "pipeline.pause.24h", []string{"current"}, arguments); err != nil {
		t.Fatalf("fixed containment pause should remain preparable with stale status evidence: %v", err)
	}
	if _, err := DefaultToolCatalog().BuildCanonicalPlan(packet, access, "feed_integrity.refresh_snapshot", []string{"current"}, map[string]any{"window": "current"}); err == nil {
		t.Fatal("stale evidence must remain blocked for non-containment actions")
	}
}

func TestDomainActionCatalogCoversEveryLaunchDomainAndPinsNegativeBoundary(t *testing.T) {
	if !ValidateDomainActionCatalog() {
		t.Fatal("domain action catalog must contain one explicit admission decision per domain")
	}
	domains := map[string]bool{}
	for _, admission := range DefaultDomainActionCatalog() {
		domains[admission.Domain] = true
	}
	for _, domain := range []string{"global_ops", "system_health", "feed_integrity", "feed_recovery", "retention", "real_experience", "ai_economics", "sources", "content", "news", "news_finding", "news_circulation", "media_sources", "atomization", "media_circulation", "redundancy", "media_library", "storage_quality", "pipeline", "enrichment", "intelligence", "embeddings", "topics_preferences", "moderation", "auth_center", "operator"} {
		if !domains[domain] {
			t.Fatalf("missing explicit action admission for Console domain %q", domain)
		}
	}
	for _, key := range []string{"sources.delete", "retention.owner_execution", "iam.role_update", "operator.restart_service", "pipeline.arbitrary_queue"} {
		if !PermanentlyForbiddenOperatorTool(key) {
			t.Fatalf("permanent negative capability escaped catalog: %q", key)
		}
	}
	if RetentionOperatorToolForbidden("retention.pause.24h") {
		t.Fatal("the reviewed fixed retention pause must remain the only admitted retention control")
	}
	if !RetentionOperatorToolForbidden("retention.execution_control") {
		t.Fatal("retention execution control must remain permanently denied")
	}
}

func TestCatalogRejectsForbiddenToolAndUnsafeOrFilteredInputs(t *testing.T) {
	packet := catalogPacket()
	_, err := DefaultToolCatalog().BuildCanonicalPlan(packet, catalogAccess(), "feed_recovery.purge_reseed", []string{"today"}, nil)
	if err == nil {
		t.Fatal("forbidden tool must not enter catalog")
	}
	_, err = DefaultToolCatalog().BuildCanonicalPlan(packet, catalogAccess(), "feed_integrity.refresh_snapshot", []string{"today"}, map[string]any{"queue_name": "anything"})
	if err == nil || !strings.Contains(err.Error(), "unsafe") {
		t.Fatalf("expected unsafe argument rejection, got %v", err)
	}
	packet.VisibleContext.Selection = &ExplicitSelection{Mode: "filtered", Count: 1}
	_, err = DefaultToolCatalog().BuildCanonicalPlan(packet, catalogAccess(), "feed_integrity.refresh_snapshot", []string{"today"}, nil)
	if err == nil || !strings.Contains(err.Error(), "filtered") {
		t.Fatalf("expected filtered selection rejection, got %v", err)
	}
	packet.VisibleContext.Selection = nil
	_, err = DefaultToolCatalog().BuildCanonicalPlan(packet, catalogAccess(), "feed_integrity.refresh_snapshot", []string{"today"}, map[string]any{"window": "today"})
	if err == nil {
		t.Fatal("missing explicit selection must block action planning")
	}
}
