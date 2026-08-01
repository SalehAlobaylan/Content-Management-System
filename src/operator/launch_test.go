package operator

import "testing"

func TestLegacyLaunchModeNeverChangesOperationalAuthority(t *testing.T) {
	for _, legacy := range []string{"", "off", "shadow", "public", "invalid"} {
		if got := EffectiveLaunchMode(LaunchModeOff, legacy); got != LaunchModePublic {
			t.Fatalf("legacy mode %q changed operational authority to %q", legacy, got)
		}
	}
}

func TestLaunchModeCompatibilityHelperDefaultsOperational(t *testing.T) {
	mode, err := LaunchModeFromEnv()
	if err != nil || mode != LaunchModePublic || !mode.AdminSurfaceEnabled() {
		t.Fatalf("expected operational compatibility mode, got %q err=%v", mode, err)
	}
}
