package controllers

import (
	"testing"

	"content-management-system/src/models"
	"github.com/google/uuid"
)

// Golden fixture for the locked V1 selector: identical evidence must produce
// the same ordered, diversity-bounded archive on every regeneration.
func TestMonthlyReviewV1GoldenSelection(t *testing.T) {
	policy := defaultMonthlyReviewPolicy()
	makeCandidate := func(id string, category, source string, sources, bookmarks int) monthlyCandidate {
		return monthlyCandidate{Story: models.Story{PublicID: uuid.MustParse(id)}, Category: category, LeadSource: source, Sources: sources, CoverageHours: 12, CoverageDays: 2, Bookmarks: bookmarks, Shares: 2, Likes: 1, Comments: 1, Meaningful: 1}
	}
	candidates := []monthlyCandidate{
		makeCandidate("00000000-0000-0000-0000-000000000001", "politics", "a", 4, 10),
		makeCandidate("00000000-0000-0000-0000-000000000002", "politics", "a", 3, 8),
		makeCandidate("00000000-0000-0000-0000-000000000003", "economy", "b", 3, 6),
		makeCandidate("00000000-0000-0000-0000-000000000004", "technology", "c", 2, 4),
	}
	selected, qualified := selectMonthlyCandidates(scoreMonthlyCandidates(candidates), policy, nil)
	if qualified != 4 {
		t.Fatalf("qualified = %d, want 4", qualified)
	}
	if len(selected) != 3 {
		t.Fatalf("selected = %d, want 3 because the 20%% source cap rounds to one per source", len(selected))
	}
	if selected[0].Story.PublicID.String() != "00000000-0000-0000-0000-000000000001" || selected[1].Story.PublicID.String() != "00000000-0000-0000-0000-000000000003" || selected[2].Story.PublicID.String() != "00000000-0000-0000-0000-000000000004" {
		t.Fatalf("unexpected deterministic order: %#v", selected)
	}
}

func TestMonthlyReviewNormalizationAndOverrideGoldenFixture(t *testing.T) {
	values := make([]float64, 20)
	for i := range values {
		values[i] = float64(i + 1)
	}
	if got := monthlyP95(values); got != 19 {
		t.Fatalf("p95 = %v, want 19", got)
	}
	policy := defaultMonthlyReviewPolicy()
	a := monthlyCandidate{Story: models.Story{PublicID: uuid.MustParse("00000000-0000-0000-0000-000000000010")}, Category: "a", LeadSource: "a", Sources: 1, CoverageDays: 1}
	b := monthlyCandidate{Story: models.Story{PublicID: uuid.MustParse("00000000-0000-0000-0000-000000000011")}, Category: "b", LeadSource: "b", Sources: 1, CoverageDays: 1}
	selected, qualified := selectMonthlyCandidates([]monthlyCandidate{a, b}, policy, map[uuid.UUID]string{a.Story.PublicID: "include", b.Story.PublicID: "exclude"})
	if qualified != 1 || len(selected) != 1 || selected[0].Story.PublicID != a.Story.PublicID {
		t.Fatalf("override fixture produced %#v qualified=%d", selected, qualified)
	}
}
