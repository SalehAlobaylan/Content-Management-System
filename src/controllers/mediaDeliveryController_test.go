package controllers

import (
	"testing"

	"content-management-system/src/models"
)

func TestValidateDeliveryPolicyRejectsUnqualifiedHLS(t *testing.T) {
	policy := models.MediaDeliveryPolicy{
		Name:                        "mobile video",
		MediaKind:                   "video",
		PrimaryMode:                 "hls",
		AllowHLS:                    true,
		GenerateProgressiveFallback: true,
		HLSSegmentDurationSec:       6,
		HLSSegmentFormat:            "cmaf",
		HLSMinVariants:              2,
		Variants: []models.MediaDeliveryPolicyVariant{
			{RenditionType: "hls", QualityTier: "standard", Enabled: true},
		},
	}
	if validateDeliveryPolicy(&policy) == nil {
		t.Fatal("one HLS access tier must not qualify an adaptive policy")
	}
	policy.Variants = append(policy.Variants, models.MediaDeliveryPolicyVariant{RenditionType: "hls", QualityTier: "high", Enabled: true})
	if err := validateDeliveryPolicy(&policy); err != nil {
		t.Fatalf("qualified HLS policy rejected: %v", err)
	}
}

func TestSelectPrimaryAndFallbackHonorsExactFallbackIdentity(t *testing.T) {
	rendered := []map[string]any{
		{"id": "standard", "type": "hls", "is_primary": true, "fallback_rendition_id": "low"},
		{"id": "high", "type": "hls"},
		{"id": "low", "type": "mp4"},
		{"id": "audio", "type": "audio"},
	}
	primary, fallback := selectPrimaryAndFallback(rendered)
	if primary["id"] != "standard" || fallback["id"] != "low" {
		t.Fatalf("unexpected primary/fallback: %v / %v", primary["id"], fallback["id"])
	}
}

func TestValidateV3AudioRenditionContractEnforcesTierCeilings(t *testing.T) {
	valid := map[string]any{
		"quality_tier": "standard",
		"bitrate_kbps": float64(128),
		"mime_type":    "audio/mp4",
		"container":    "m4a",
		"codec":        "aac",
	}
	if err := validateV3AudioRenditionContract(valid); err != nil {
		t.Fatalf("valid Standard AAC rendition rejected: %v", err)
	}
	overCeiling := map[string]any{}
	for key, value := range valid {
		overCeiling[key] = value
	}
	overCeiling["bitrate_kbps"] = float64(129)
	if validateV3AudioRenditionContract(overCeiling) == nil {
		t.Fatal("audio rendition above its declared ceiling was accepted")
	}
	invalidTuple := map[string]any{}
	for key, value := range valid {
		invalidTuple[key] = value
	}
	invalidTuple["codec"] = "opus"
	if validateV3AudioRenditionContract(invalidTuple) == nil {
		t.Fatal("unsupported v3 audio tuple was accepted")
	}
}

func TestValidateV3AudioRenditionContractAcceptsMeasuredMP3(t *testing.T) {
	if err := validateV3AudioRenditionContract(map[string]any{
		"quality_tier": "data_saver",
		"bitrate_kbps": float64(48),
		"mime_type":    "audio/mpeg",
		"container":    "mp3",
		"codec":        "mp3",
	}); err != nil {
		t.Fatalf("valid MP3 source passthrough rejected: %v", err)
	}
}

func TestValidateTypedAudioTierSetRequiresUniqueDataSaverFloor(t *testing.T) {
	standard := map[string]any{
		"schema_version": float64(3),
		"type":           "audio",
		"quality_tier":   "standard",
		"bitrate_kbps":   float64(96),
		"mime_type":      "audio/mp4",
		"container":      "m4a",
		"codec":          "aac",
	}
	if validateTypedAudioTierSet([]map[string]any{standard}) == nil {
		t.Fatal("v3 native audio without Data Saver floor was accepted")
	}
	dataSaver := map[string]any{}
	for key, value := range standard {
		dataSaver[key] = value
	}
	dataSaver["quality_tier"] = "data_saver"
	dataSaver["bitrate_kbps"] = float64(64)
	if err := validateTypedAudioTierSet([]map[string]any{dataSaver, standard}); err != nil {
		t.Fatalf("source-bounded Data Saver/Standard set rejected: %v", err)
	}
	dataSaver["schema_version"] = float64(2)
	standard["schema_version"] = float64(2)
	if err := validateTypedAudioTierSet([]map[string]any{dataSaver, standard}); err != nil {
		t.Fatalf("typed atomization audio set rejected: %v", err)
	}
	if validateTypedAudioTierSet([]map[string]any{dataSaver, dataSaver}) == nil {
		t.Fatal("duplicate v3 audio tier was accepted")
	}
}
