package controllers

import (
	"testing"

	"content-management-system/src/models"
)

func TestConsumptionClassForEvidenceDoesNotTreatSeekingAsCompletion(t *testing.T) {
	if got := consumptionClassForEvidence(4, 270, 300); got != models.InteractionTypeQuickSkip {
		t.Fatalf("expected seek-only evidence to be quick skip, got %s", got)
	}
	if got := consumptionClassForEvidence(30, 270, 300); got != models.InteractionTypeComplete {
		t.Fatalf("expected sufficient end evidence to complete, got %s", got)
	}
}

func TestValidateConsumptionEvidenceRejectsForgedClassAndPreservesLegacyCompletion(t *testing.T) {
	forged := []byte(`{"consumption_contract_version":1,"actual_played_seconds":4,"furthest_position_seconds":270,"consumption_classification":"completed"}`)
	if err := validateConsumptionEvidence(models.InteractionTypeComplete, forged, 300); err == nil {
		t.Fatal("expected forged completion to be rejected")
	}
	valid := []byte(`{"consumption_contract_version":1,"actual_played_seconds":30,"furthest_position_seconds":270,"consumption_classification":"completed"}`)
	if err := validateConsumptionEvidence(models.InteractionTypeComplete, valid, 300); err != nil {
		t.Fatalf("expected valid completion evidence, got %v", err)
	}
	if err := validateConsumptionEvidence(models.InteractionTypeComplete, nil, 300); err != nil {
		t.Fatalf("legacy completion must remain accepted, got %v", err)
	}
}
