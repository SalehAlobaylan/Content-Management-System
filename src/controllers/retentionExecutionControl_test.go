package controllers

import (
	"testing"

	"content-management-system/src/models"
)

func TestRetentionCapabilityDefaultsFailClosed(t *testing.T) {
	control := models.RetentionExecutionControl{TenantID: "default"}
	for _, capability := range []string{
		retentionCapabilityCanonicalCompaction,
		retentionCapabilityHistorical,
		retentionCapabilityOwnerRuns,
		retentionCapabilityRecoveryRotate,
		retentionCapabilityRecoveryPurge,
		retentionCapabilityPodsReset,
	} {
		if retentionCapabilityEnabled(control, capability) {
			t.Fatalf("capability %q unexpectedly enabled by zero-value control", capability)
		}
	}
}

func TestRetentionCapabilityMapsOnlyItsOwnGate(t *testing.T) {
	control := models.RetentionExecutionControl{
		CanonicalCompactionEnabled: true,
		HistoricalEnabled:          true,
		OwnerRunsEnabled:           true,
		FeedRecoveryRotateEnabled:  true,
		FeedRecoveryPurgeEnabled:   true,
		PodsResetEnabled:           true,
	}
	for _, capability := range []string{
		retentionCapabilityCanonicalCompaction,
		retentionCapabilityHistorical,
		retentionCapabilityOwnerRuns,
		retentionCapabilityRecoveryRotate,
	} {
		if !retentionCapabilityEnabled(control, capability) {
			t.Fatalf("capability %q was not enabled", capability)
		}
	}
	if retentionCapabilityEnabled(control, retentionCapabilityPodsReset) {
		t.Fatal("Pods Reset must remain hard-gated until scoped disposable qualification passes")
	}
	if retentionCapabilityEnabled(control, "unknown") {
		t.Fatal("unknown capability must fail closed")
	}
	if retentionCapabilityEnabled(control, retentionCapabilityRecoveryPurge) {
		t.Fatal("legacy Purge & Reseed must remain disabled even if an old row was armed")
	}
}
