package operator

import "strings"

// RetentionOperatorCapability is the reviewed admission matrix. A tool may
// eventually be admitted only for the listed bounded derived-state actions;
// all deletion, custody, owner-execution, and control-arming paths remain
// native/manual regardless of a model suggestion or console context.
type RetentionOperatorCapability struct {
	Key          string
	ToolEligible bool
	Reason       string
}

func RetentionOperatorCapabilityMatrix() []RetentionOperatorCapability {
	return []RetentionOperatorCapability{
		{Key: "retention.inspect", Reason: "bounded evidence, explanations, and deep links"},
		{Key: "retention.pause.24h", ToolEligible: true, Reason: "fixed 24-hour scheduling pause; it cannot arm retention execution or alter custody policy"},
		{Key: "retention.policy", ToolEligible: true, Reason: "validated policy patch after dedicated admission"},
		{Key: "retention.prepare_compaction", ToolEligible: true, Reason: "exact manifest preparation only after dedicated admission"},
		{Key: "retention.monthly_review", ToolEligible: true, Reason: "derived Month in Review build/verification after dedicated admission"},
		{Key: "retention.hold", ToolEligible: true, Reason: "additive protection hold/override after dedicated admission"},
		{Key: "retention.refresh_news_snapshots", ToolEligible: true, Reason: "bounded noncanonical derived snapshot refresh after dedicated admission"},
		{Key: "retention.compaction", Reason: "canonical mutation stays native/manual"},
		{Key: "retention.historical_retirement", Reason: "old-month deletion stays native/manual"},
		{Key: "retention.physical_rewrite", Reason: "database rewrite stays native/manual"},
		{Key: "retention.execution_control", Reason: "destructive control arming stays native/manual"},
		{Key: "retention.owner_execution", Reason: "owner may delete or move media and stays native/manual"},
	}
}

func RetentionOperatorToolForbidden(key string) bool {
	for _, capability := range RetentionOperatorCapabilityMatrix() {
		if capability.Key == key {
			return !capability.ToolEligible
		}
	}
	// Retention mutation names are default-denied until the catalog has an
	// explicit reviewed entry for a bounded safe capability.
	return strings.HasPrefix(key, "retention.")
}
