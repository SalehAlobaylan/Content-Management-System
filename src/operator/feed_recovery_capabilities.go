package operator

import "strings"

// FeedRecoveryOperatorCapability is the permanent admission matrix for the
// recovery domain. Native recovery retains authority over reauthentication,
// exact manifests, lane leases, execution controls, cancellation, and
// rollback; Operator may explain and deep-link those records but never turn a
// manual capability into a tool by name alone.
type FeedRecoveryOperatorCapability struct {
	Key        string
	Operator   string
	NativeOnly bool
	Reason     string
}

func FeedRecoveryOperatorCapabilityMatrix() []FeedRecoveryOperatorCapability {
	return []FeedRecoveryOperatorCapability{
		{Key: "feed_recovery.inspect", Operator: "explain_monitor_deep_link", Reason: "bounded CMS evidence only"},
		{Key: "feed_recovery.repair", Operator: "explain_monitor_deep_link", NativeOnly: true, Reason: "native confirmation, fresh preflight, and recovery lease are required"},
		{Key: "feed_recovery.rotate_single_lane", Operator: "explain_monitor_deep_link", NativeOnly: true, Reason: "native reauthentication, cutover verification, and rollback window are required"},
		{Key: "feed_recovery.rotate_both_lanes", Operator: "manual_only", NativeOnly: true, Reason: "both-lane recovery remains a native sequential workflow"},
		{Key: "feed_recovery.purge_reseed", Operator: "manual_only", NativeOnly: true, Reason: "destructive frozen manifest and reauthentication are never Operator input"},
		{Key: "feed_recovery.no_full_rollback", Operator: "manual_only", NativeOnly: true, Reason: "irreversible scope cannot be prepared or approved by Operator"},
		{Key: "feed_recovery.cancel", Operator: "manual_only", NativeOnly: true, Reason: "native run cancellation is bound to its run phase and lease"},
		{Key: "feed_recovery.rollback", Operator: "manual_only", NativeOnly: true, Reason: "native rollback is bound to the generation head and rollback deadline"},
	}
}

func FeedRecoveryOperatorToolForbidden(key string) bool {
	for _, capability := range FeedRecoveryOperatorCapabilityMatrix() {
		if capability.Key == key {
			return capability.Operator != "tool"
		}
	}
	// Default-deny prevents a future convenience handler from bypassing the
	// reviewed matrix with a newly invented feed_recovery.* key.
	return strings.HasPrefix(key, "feed_recovery.")
}
