package controllers

import (
	"testing"

	"content-management-system/src/models"
)

// TestCampaignIsTerminal covers the terminal-state predicate that guards
// transitions.
func TestCampaignIsTerminal(t *testing.T) {
	terminal := []string{
		models.EmbeddingCampaignCompleted,
		models.EmbeddingCampaignCompletedWithWaiver,
		models.EmbeddingCampaignAborted,
	}
	nonTerminal := []string{
		models.EmbeddingCampaignDraft, models.EmbeddingCampaignRunning,
		models.EmbeddingCampaignPaused, models.EmbeddingCampaignBlocked,
		models.EmbeddingCampaignVerifying,
	}
	for _, s := range terminal {
		if !(models.EmbeddingCampaign{State: s}).IsTerminal() {
			t.Errorf("state %q should be terminal", s)
		}
	}
	for _, s := range nonTerminal {
		if (models.EmbeddingCampaign{State: s}).IsTerminal() {
			t.Errorf("state %q should not be terminal", s)
		}
	}
}

func TestFirstPositiveNonNeg(t *testing.T) {
	if firstPositive(0, 200) != 200 {
		t.Error("firstPositive(0,200) should default")
	}
	if firstPositive(5, 200) != 5 {
		t.Error("firstPositive(5,200) should keep 5")
	}
	if firstNonNeg(0, 5000) != 5000 {
		t.Error("firstNonNeg(0,5000) should default")
	}
}
