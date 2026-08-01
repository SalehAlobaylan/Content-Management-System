package operator

import (
	"testing"

	"content-management-system/src/models"
)

func TestResolveRuntimePolicyDefaultsOperationalAndIgnoresLegacyLaunchFields(t *testing.T) {
	for _, legacy := range []string{"off", "shadow", "public", "enabled"} {
		resolved, err := ResolveRuntimePolicy(LaunchModeOff, &models.OperatorPolicy{LaunchMode: legacy})
		if err != nil || resolved.LaunchMode != LaunchModePublic || !resolved.ReadEnabled || !resolved.LLMEnabled || !resolved.ExecutionEnabled || !resolved.SchedulesEnabled {
			t.Fatalf("legacy mode %q must not alter independent controls: %+v err=%v", legacy, resolved, err)
		}
	}
}
