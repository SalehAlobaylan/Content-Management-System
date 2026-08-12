package operator

import (
	"errors"
	"testing"
	"time"
)

func validVisibleContext() VisibleContext {
	return VisibleContext{
		SchemaVersion:    ContractVersion,
		Domain:           "media_sources",
		View:             "list",
		Filters:          map[string]any{"status": "pending"},
		Subjects:         []SubjectRef{{Type: "content_source", ID: "source-1"}},
		AvailableIntents: []Intent{IntentExplain, IntentInvestigate},
	}
}

func TestVisibleContextRejectsFilteredMutationSelection(t *testing.T) {
	context := validVisibleContext()
	context.Selection = &ExplicitSelection{Mode: "filtered", Count: 25}
	if err := context.Validate(); err != nil {
		t.Fatalf("filtered selections remain valid for read-only investigation: %v", err)
	}
	context.Selection = &ExplicitSelection{Mode: "explicit", IDs: []string{"one"}, Count: 2}
	if err := context.Validate(); err == nil {
		t.Fatal("expected explicit selection count mismatch to fail")
	}
}

func TestDecisionPacketRequiresTenantScopedEvidenceAndFactProof(t *testing.T) {
	now := time.Now().UTC()
	packet := DecisionPacket{
		SchemaVersion: ContractVersion, PacketID: "packet-1", Fingerprint: "hash", TenantID: "tenant-a", ActorID: "actor-a",
		VisibleContext: validVisibleContext(), CollectionStartedAt: now, CollectionEndedAt: now, Completeness: "complete",
		Evidence: []EvidenceRef{{EvidenceID: "ev-1", Authority: EvidenceLive, Domain: "media_sources", AdapterKey: "media_sources", AdapterVersion: "v1", TenantID: "tenant-a", RequiredPermission: "source:read", DeepLink: "/platform/media/sources", ObservedAt: now, FetchedAt: now, MaxAgeSeconds: 60, ExpiresAt: now.Add(time.Minute), ContentHash: "hash", SourceVersion: "1", Availability: EvidenceAvailable}},
		Facts:    []Fact{{Key: "pending_count", Value: 1, EvidenceIDs: []string{"ev-1"}}},
	}
	if err := packet.Validate(); err != nil {
		t.Fatalf("expected valid packet: %v", err)
	}
	packet.Evidence[0].TenantID = "tenant-b"
	if err := packet.Validate(); err == nil {
		t.Fatal("expected tenant mismatch to fail")
	}
}

func TestInternalDeepLinkRejectsProtocolRelativeAndMalformedValues(t *testing.T) {
	for _, value := range []string{"https://unsafe.example", "//unsafe.example", "/\\unsafe", "/platform/ok\nnext", "/platform/../operator", "/platform/%2e%2e/operator", "/platform/%2f%2funsafe"} {
		if IsInternalDeepLink(value) {
			t.Fatalf("%q must not be accepted as an internal link", value)
		}
	}
	if !IsInternalDeepLink("/platform/media/sources?source_id=abc") {
		t.Fatal("canonical Console path must remain valid")
	}
}

func TestToolDescriptorRejectsForbiddenCapabilities(t *testing.T) {
	descriptor := ToolDescriptor{Key: "content.delete", Version: "v1", OwnerDomain: "content", TargetType: "content_item", ArgumentSchema: "{}", RequiredPermission: "content:delete", RiskTier: RiskHigh, TargetCap: 1, Executor: "content.delete", Monitor: "monitor", Verifier: "verify", Idempotency: "key", Cancellation: "stop", Rollback: "not_available", Contingencies: []string{"stop"}, AffectedDomains: []string{"content"}, LocalizedActionKey: "delete"}
	if err := descriptor.Validate(); !errors.Is(err, ErrForbiddenTool) {
		t.Fatalf("expected forbidden capability, got %v", err)
	}
}

func TestToolDescriptorAllowsBoundedReversibleTool(t *testing.T) {
	descriptor := ToolDescriptor{Key: "feed_integrity.refresh_snapshot", Version: "v1", OwnerDomain: "feed_integrity", TargetType: "feed_window", ArgumentSchema: "feed-window-v1", RequiredPermission: "feed:manage", RiskTier: RiskRoutine, TargetCap: 1, Executor: "refresh_snapshot", Monitor: "refresh_monitor", Verifier: "freshness_verify", Idempotency: "plan_step", Cancellation: "before-start", Rollback: "not_required", Contingencies: []string{"verification_failed:stop"}, AffectedDomains: []string{"feed_integrity", "news"}, LocalizedActionKey: "operator.action.refresh_snapshot"}
	if err := descriptor.Validate(); err != nil {
		t.Fatalf("expected bounded reversible tool to validate: %v", err)
	}
}

func TestFeedRecoveryOperatorMatrixKeepsNativeAndDestructiveControlsOutOfTools(t *testing.T) {
	for _, key := range []string{"feed_recovery.repair", "feed_recovery.rotate_single_lane", "feed_recovery.rotate_both_lanes", "feed_recovery.purge_reseed", "feed_recovery.no_full_rollback", "feed_recovery.cancel", "feed_recovery.rollback", "feed_recovery.unreviewed_future_handler"} {
		if !FeedRecoveryOperatorToolForbidden(key) {
			t.Fatalf("%s must remain outside the Operator mutation catalog", key)
		}
	}
}

func TestRetentionOperatorMatrixKeepsCustodyAndOwnerExecutionOutOfTools(t *testing.T) {
	for _, key := range []string{"retention.compaction", "retention.historical_retirement", "retention.physical_rewrite", "retention.execution_control", "retention.owner_execution", "retention.unreviewed_future_handler"} {
		if !RetentionOperatorToolForbidden(key) {
			t.Fatalf("%s must remain outside the Operator mutation catalog", key)
		}
	}
	if RetentionOperatorToolForbidden("retention.refresh_news_snapshots") {
		t.Fatal("the bounded derived snapshot candidate must remain available for later dedicated tool admission")
	}
}
