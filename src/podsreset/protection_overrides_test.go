package podsreset

import "testing"

func TestProtectionOverridesRejectLiveEffectsStorageAndUnknownCodes(t *testing.T) {
	for _, option := range ProtectionOverrides() {
		if codes, err := ValidateProtectionOverrides([]string{option.Code, option.Code}); err != nil || len(codes) != 1 || option.Consequence == "" {
			t.Fatalf("content protection option is invalid: %+v %v", option, err)
		}
	}
	for _, code := range []string{"storage_inventory_incomplete", "storage_inventory_invalid", "artifact_external_reference", "active_stage_effect", "active_artifact_upload", "active_pipeline_lease", "surviving_child", "missing_retirement_identity", "legacy_campaign_reference", "future_unknown", ""} {
		if _, err := ValidateProtectionOverrides([]string{code}); err == nil {
			t.Fatalf("unsupported override accepted: %s", code)
		}
	}
}
