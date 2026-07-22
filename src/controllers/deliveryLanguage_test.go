package controllers

import "testing"

func TestParseDeliveryLanguage(t *testing.T) {
	for raw, expected := range map[string]deliveryLanguage{
		"":     deliveryLanguageBoth,
		"both": deliveryLanguageBoth,
		" AR ": deliveryLanguageArabic,
		"en":   deliveryLanguageEnglish,
	} {
		got, ok := parseDeliveryLanguage(raw)
		if !ok || got != expected {
			t.Fatalf("parseDeliveryLanguage(%q) = %q, %v; want %q, true", raw, got, ok, expected)
		}
	}
	if _, ok := parseDeliveryLanguage("fr"); ok {
		t.Fatal("unknown delivery language must be rejected")
	}
}
