package controllers

import (
	"testing"

	"content-management-system/src/intelligence"
	"content-management-system/src/models"

	"github.com/google/uuid"
)

func scoredFixture(n int) ([]ScoredItem, []uuid.UUID) {
	scored := make([]ScoredItem, n)
	ids := make([]uuid.UUID, n)
	for i := 0; i < n; i++ {
		id := uuid.New()
		ids[i] = id
		scored[i] = ScoredItem{
			Item:       models.ContentItem{PublicID: id},
			FinalScore: float64(n - i), // descending scores, position i is rank i
		}
	}
	return scored, ids
}

func TestInjectExplorationSliceReservesTenthSlot(t *testing.T) {
	scored, ids := scoredFixture(25)
	// The two exploring items sit deep below the fold (ranks 20 and 22).
	signals := map[uuid.UUID]intelligence.FeedSignal{}
	for _, id := range ids {
		signals[id] = intelligence.FeedSignal{Exploring: false, Demotion: 1}
	}
	signals[ids[20]] = intelligence.FeedSignal{Exploring: true, Demotion: 1}
	signals[ids[22]] = intelligence.FeedSignal{Exploring: true, Demotion: 1}

	out := injectExplorationSlice(scored, signals, 10)

	if len(out) != len(scored) {
		t.Fatalf("item count changed: %d -> %d", len(scored), len(out))
	}
	seen := map[uuid.UUID]bool{}
	for _, s := range out {
		if seen[s.Item.PublicID] {
			t.Fatalf("duplicate item after injection: %s", s.Item.PublicID)
		}
		seen[s.Item.PublicID] = true
	}
	// Slot 10 (index 9) and slot 20 (index 19) carry the exploring items.
	if out[9].Item.PublicID != ids[20] {
		t.Fatalf("slot 10 must hold the highest-ranked exploring item, got rank of %v", out[9].Item.PublicID)
	}
	if out[19].Item.PublicID != ids[22] {
		t.Fatalf("slot 20 must hold the next exploring item")
	}
	// Non-slice positions keep their relative order.
	if out[0].Item.PublicID != ids[0] || out[1].Item.PublicID != ids[1] {
		t.Fatalf("top of the page must be untouched")
	}
}

func TestInjectExplorationSliceNoExploring(t *testing.T) {
	scored, ids := scoredFixture(15)
	signals := map[uuid.UUID]intelligence.FeedSignal{}
	for _, id := range ids {
		signals[id] = intelligence.FeedSignal{Exploring: false, Demotion: 1}
	}
	out := injectExplorationSlice(scored, signals, 10)
	for i := range out {
		if out[i].Item.PublicID != ids[i] {
			t.Fatalf("with no exploring items the order must be unchanged (pos %d)", i)
		}
	}
}

func TestInjectExplorationSliceExploringAlreadyHigh(t *testing.T) {
	// An exploring item already at rank 3 is consumed by normal order before
	// slot 10; the next exploring item below the fold fills the slice slot.
	scored, ids := scoredFixture(15)
	signals := map[uuid.UUID]intelligence.FeedSignal{}
	for _, id := range ids {
		signals[id] = intelligence.FeedSignal{Exploring: false, Demotion: 1}
	}
	signals[ids[2]] = intelligence.FeedSignal{Exploring: true, Demotion: 1}
	signals[ids[12]] = intelligence.FeedSignal{Exploring: true, Demotion: 1}

	out := injectExplorationSlice(scored, signals, 10)
	if out[2].Item.PublicID != ids[2] {
		t.Fatalf("high-ranked exploring item must keep its earned position")
	}
	if out[9].Item.PublicID != ids[12] {
		t.Fatalf("slice slot must pull the remaining below-fold exploring item")
	}
}
