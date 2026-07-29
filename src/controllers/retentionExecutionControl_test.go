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
	}
	for _, capability := range []string{
		retentionCapabilityCanonicalCompaction,
		retentionCapabilityHistorical,
		retentionCapabilityOwnerRuns,
		retentionCapabilityRecoveryRotate,
		retentionCapabilityRecoveryPurge,
	} {
		if !retentionCapabilityEnabled(control, capability) {
			t.Fatalf("capability %q was not enabled", capability)
		}
	}
	if retentionCapabilityEnabled(control, "unknown") {
		t.Fatal("unknown capability must fail closed")
	}
}
