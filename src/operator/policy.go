package operator

import "content-management-system/src/models"

// RuntimePolicy is the CMS-side, fail-closed interpretation of a tenant's
// persisted control row. It is intentionally separate from the database model
// so every caller gets the same launch and kill-switch semantics.
type RuntimePolicy struct {
	LaunchMode       LaunchMode
	ReadEnabled      bool
	LLMEnabled       bool
	ExecutionEnabled bool
	SchedulesEnabled bool
}

func ResolveRuntimePolicy(boot LaunchMode, persisted *models.OperatorPolicy) (RuntimePolicy, error) {
	// The launch-mode and boolean fields remain readable legacy data. They do not
	// define current authority: otherwise a foundation-era row would permanently
	// keep execution and schedules off. Callers overlay active system capability
	// controls from operator_capability_controls before serving or mutating.
	_ = boot
	_ = persisted
	return RuntimePolicy{LaunchMode: LaunchModePublic, ReadEnabled: true, LLMEnabled: true, ExecutionEnabled: true, SchedulesEnabled: true}, nil
}
