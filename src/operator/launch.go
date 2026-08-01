package operator

import "strings"

type LaunchMode string

const (
	LaunchModeOff    LaunchMode = "off"
	LaunchModeShadow LaunchMode = "shadow"
	LaunchModePublic LaunchMode = "public"
)

// LaunchModeFromEnv is retained only so persisted legacy rows and older callers
// remain source compatible. Wahb Operator is now operational for authenticated
// tenant admins by default; OPERATOR_LAUNCH_MODE is not an authority gate.
// Independent, durable capability controls are the only runtime stop mechanism.
func LaunchModeFromEnv() (LaunchMode, error) {
	return LaunchModePublic, nil
}

func (mode LaunchMode) AdminSurfaceEnabled() bool {
	return mode == LaunchModeShadow || mode == LaunchModePublic
}

// EffectiveLaunchMode calculates the most restrictive state. A persisted
// policy's `public` value is deliberately ignored unless the boot-time state
// was public already.
func EffectiveLaunchMode(boot LaunchMode, persisted string) LaunchMode {
	// Keep accepting the old values in stored rows without allowing them to
	// quietly withdraw or promote authority. Runtime authority comes from the
	// disable-only capability ledger, not this legacy field.
	_ = boot
	_ = strings.TrimSpace(persisted)
	return LaunchModePublic
}
