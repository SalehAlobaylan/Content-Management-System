package controllers

import (
	"fmt"
	"testing"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/utils"
	"github.com/google/uuid"
	"gorm.io/datatypes"
)

func scoredPodsFixture(id uuid.UUID, source string, published time.Time, score float64) ScoredItem {
	return ScoredItem{
		Item:       models.ContentItem{PublicID: id, SourceName: &source, PublishedAt: &published},
		FinalScore: score,
	}
}

func TestTypedFallbackMetadataRequiresAuthoritativeRenditionTruth(t *testing.T) {
	fallback := "https://cdn.example.test/fallback.mp4"
	item := models.ContentItem{
		FallbackPlaybackURL: &fallback,
		MediaRenditions: datatypes.JSON(`[
			{"type":"hls","url":"https://cdn.example.test/primary.m3u8","has_video":true},
			{"type":"mp4","url":"https://cdn.example.test/fallback.mp4","has_video":true}
		]`),
	}
	typeName, hasVideo := typedFallbackMetadata(item)
	if typeName == nil || *typeName != "mp4" || hasVideo == nil || !*hasVideo {
		t.Fatalf("expected typed MP4 video fallback, got type=%v has_video=%v", typeName, hasVideo)
	}

	item.MediaRenditions = datatypes.JSON(`[{"type":"mp4","url":"https://cdn.example.test/fallback.mp4"}]`)
	if typeName, hasVideo := typedFallbackMetadata(item); typeName != nil || hasVideo != nil {
		t.Fatalf("fallback without stored has_video must be omitted, got type=%v has_video=%v", typeName, hasVideo)
	}
}

func TestPrioritizePodsForSessionRecyclesSoftSuppressionButNeverHides(t *testing.T) {
	unseenID, viewedID, hiddenID := uuid.New(), uuid.New(), uuid.New()
	items := []models.ContentItem{{PublicID: viewedID}, {PublicID: hiddenID}, {PublicID: unseenID}}

	got := prioritizePodsForSession(items, []uuid.UUID{viewedID, hiddenID}, []uuid.UUID{hiddenID})
	if len(got) != 2 || got[0].PublicID != unseenID || got[1].PublicID != viewedID {
		t.Fatalf("session order = %#v, want unseen then softly suppressed with hidden removed", got)
	}
}

func TestPrioritizeScoredPodsForSessionPreservesScoresAndOrder(t *testing.T) {
	unseenID, completedID := uuid.New(), uuid.New()
	items := []ScoredItem{
		{Item: models.ContentItem{PublicID: completedID}, FinalScore: 0.9},
		{Item: models.ContentItem{PublicID: unseenID}, FinalScore: 0.4},
	}

	got := prioritizeScoredPodsForSession(items, []uuid.UUID{completedID}, nil)
	if len(got) != 2 || got[0].Item.PublicID != unseenID || got[1].Item.PublicID != completedID || got[1].FinalScore != 0.9 {
		t.Fatalf("ranked session order = %#v, want unseen first and score-preserving recycle", got)
	}
}

func TestReserveFreshPodsFirstPagePromotesNewestWithoutReorderingRemainder(t *testing.T) {
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	items := make([]ScoredItem, 0, 40)
	originalOldIDs := make([]uuid.UUID, 0, 36)
	for i := 0; i < 36; i++ {
		id := uuid.New()
		originalOldIDs = append(originalOldIDs, id)
		items = append(items, scoredPodsFixture(id, "established", now.Add(-30*24*time.Hour-time.Duration(i)*time.Minute), float64(100-i)))
	}
	freshIDs := []uuid.UUID{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	for i, id := range freshIDs {
		source := []string{"fresh-a", "fresh-b", "fresh-c", "fresh-d"}[i]
		items = append(items, scoredPodsFixture(id, source, now.Add(-time.Duration(i+1)*time.Hour), .01))
	}

	got := reserveFreshPodsFirstPage(items, now)
	wantPositions := []int{1, 6, 11, 16}
	for i, position := range wantPositions {
		if got[position].Item.PublicID != freshIDs[i] {
			t.Fatalf("fresh item %d landed at %d, got %s", i, position, got[position].Item.PublicID)
		}
		if !got[position].ScoreBreakdown.FreshnessReserved {
			t.Fatalf("fresh item %d lacks reservation diagnostics", i)
		}
	}
	remainingOld := make([]uuid.UUID, 0, len(originalOldIDs))
	for _, item := range got {
		for _, oldID := range originalOldIDs {
			if item.Item.PublicID == oldID {
				remainingOld = append(remainingOld, oldID)
				break
			}
		}
	}
	for i := range originalOldIDs {
		if remainingOld[i] != originalOldIDs[i] {
			t.Fatalf("non-reserved ranking changed at %d", i)
		}
	}
}

func TestReserveFreshPodsFirstPageBoundsOneSourceAndSkipsSuppressedOrFuture(t *testing.T) {
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	items := []ScoredItem{
		scoredPodsFixture(uuid.New(), "old", now.Add(-30*24*time.Hour), 10),
		scoredPodsFixture(uuid.New(), "same", now.Add(-time.Hour), 1),
		scoredPodsFixture(uuid.New(), "same", now.Add(-2*time.Hour), .9),
		scoredPodsFixture(uuid.New(), "same", now.Add(-3*time.Hour), .8),
		scoredPodsFixture(uuid.New(), "other", now.Add(-4*time.Hour), .7),
		scoredPodsFixture(uuid.New(), "future", now.Add(time.Hour), .6),
	}
	suppressed := scoredPodsFixture(uuid.New(), "third", now.Add(-30*time.Minute), .5)
	suppressed.ScoreBreakdown.Flags = "suppressed"
	items = append(items, suppressed)

	got := reserveFreshPodsFirstPage(items, now)
	reservedSources := map[string]int{}
	for _, item := range got {
		if !item.ScoreBreakdown.FreshnessReserved {
			continue
		}
		reservedSources[podsFreshnessSourceKey(item.Item)]++
		if item.Item.PublicID == suppressed.Item.PublicID {
			t.Fatal("suppressed item entered a freshness reservation")
		}
		if podsPublicationTime(item.Item).After(now.Add(15 * time.Minute)) {
			t.Fatal("future-skewed item entered a freshness reservation")
		}
	}
	if reservedSources["name:same"] > podsFreshnessReservationMaxPerSource {
		t.Fatalf("same source used %d reserved positions", reservedSources["name:same"])
	}
}

func TestReserveFreshPodsFirstPageIsStableForCursorReconstruction(t *testing.T) {
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	items := make([]ScoredItem, 0, 30)
	for i := 0; i < 26; i++ {
		items = append(items, scoredPodsFixture(uuid.New(), "old", now.Add(-30*24*time.Hour-time.Duration(i)*time.Minute), float64(30-i)))
	}
	for i := 0; i < 4; i++ {
		items = append(items, scoredPodsFixture(uuid.New(), "fresh-"+string(rune('a'+i)), now.Add(-time.Duration(i+1)*time.Hour), .01))
	}

	firstAssembly := reserveFreshPodsFirstPage(items, now)
	cursorID := firstAssembly[19].Item.PublicID
	secondAssembly := reserveFreshPodsFirstPage(items, now)
	cursorIndex := -1
	for i := range secondAssembly {
		if secondAssembly[i].Item.PublicID == cursorID {
			cursorIndex = i
			break
		}
	}
	if cursorIndex != 19 {
		t.Fatalf("cursor moved to %d after deterministic reconstruction", cursorIndex)
	}
	seen := map[uuid.UUID]struct{}{}
	for _, item := range firstAssembly[:20] {
		seen[item.Item.PublicID] = struct{}{}
	}
	for _, item := range secondAssembly[cursorIndex+1:] {
		if _, duplicate := seen[item.Item.PublicID]; duplicate {
			t.Fatalf("cursor continuation repeated first-page item %s", item.Item.PublicID)
		}
	}
}

func TestReserveFreshPodsFirstPageEnforcesTenSourceDiversity(t *testing.T) {
	now := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	items := make([]ScoredItem, 0, 40)
	// Deliberately rank each source in a block. The assembler must pull the
	// earliest legal alternatives without losing any item or reservation slot.
	for sourceIndex := 0; sourceIndex < 10; sourceIndex++ {
		for itemIndex := 0; itemIndex < 4; itemIndex++ {
			published := now.Add(-30 * 24 * time.Hour)
			if itemIndex == 3 {
				published = now.Add(-time.Duration(sourceIndex+1) * time.Hour)
			}
			items = append(items, scoredPodsFixture(
				uuid.New(),
				fmt.Sprintf("source-%02d", sourceIndex),
				published,
				float64(100-sourceIndex*4-itemIndex),
			))
		}
	}

	got := reserveFreshPodsFirstPage(items, now)
	if len(got) != len(items) {
		t.Fatalf("assembled %d items, want %d", len(got), len(items))
	}
	counts := map[string]int{}
	seen := map[uuid.UUID]struct{}{}
	for index, item := range got {
		if _, duplicate := seen[item.Item.PublicID]; duplicate {
			t.Fatalf("duplicate item %s", item.Item.PublicID)
		}
		seen[item.Item.PublicID] = struct{}{}
		if index >= 20 {
			continue
		}
		source := podsFreshnessSourceKey(item.Item)
		counts[source]++
		if index > 0 && source == podsFreshnessSourceKey(got[index-1].Item) {
			t.Fatalf("adjacent source %s at first-page position %d", source, index+1)
		}
	}
	for source, count := range counts {
		if count > 4 {
			t.Fatalf("source %s occupies %d first-page positions", source, count)
		}
	}
	for _, position := range podsFreshnessReservationPositions {
		if !got[position].ScoreBreakdown.FreshnessReserved {
			t.Fatalf("position %d lost freshness reservation", position+1)
		}
	}
}

func TestReserveFreshPodsFirstPagePreservesCapacityForSameSourceReservations(t *testing.T) {
	now := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	items := make([]ScoredItem, 0, 42)
	for index := 0; index < 20; index++ {
		items = append(items, scoredPodsFixture(uuid.New(), "dominant", now.Add(-30*24*time.Hour-time.Duration(index)*time.Minute), float64(100-index)))
	}
	for sourceIndex, source := range []string{"other-a", "other-b", "other-c", "other-d"} {
		for itemIndex := 0; itemIndex < 5; itemIndex++ {
			items = append(items, scoredPodsFixture(uuid.New(), source, now.Add(-20*24*time.Hour), float64(50-sourceIndex*5-itemIndex)))
		}
	}
	items = append(items,
		scoredPodsFixture(uuid.New(), "dominant", now.Add(-time.Hour), .2),
		scoredPodsFixture(uuid.New(), "dominant", now.Add(-2*time.Hour), .1),
	)

	got := reserveFreshPodsFirstPage(items, now)
	counts := map[string]int{}
	for index := 0; index < 20; index++ {
		source := podsFreshnessSourceKey(got[index].Item)
		counts[source]++
		if index > 0 && source == podsFreshnessSourceKey(got[index-1].Item) {
			t.Fatalf("adjacent source %s at first-page position %d", source, index+1)
		}
	}
	if counts["name:dominant"] > 4 {
		pageSources := make([]string, 0, 20)
		for _, item := range got[:20] {
			pageSources = append(pageSources, podsFreshnessSourceKey(item.Item))
		}
		t.Fatalf("dominant source occupies %d first-page positions: %v", counts["name:dominant"], pageSources)
	}
	for _, position := range []int{1, 6} {
		if !got[position].ScoreBreakdown.FreshnessReserved {
			t.Fatalf("position %d lost same-source freshness reservation", position+1)
		}
	}
}

func TestPodsFirstPageConstraintStatusDescribesActualPage(t *testing.T) {
	now := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	makeItems := func(groups []string, pinnedSource string, pinnedCount int) []ScoredItem {
		items := make([]ScoredItem, 0)
		seenBySource := map[string]int{}
		for _, source := range groups {
			index := seenBySource[source]
			item := scoredPodsFixture(uuid.New(), source, now.Add(-time.Duration(len(items)+1)*time.Minute), float64(100-len(items)))
			if source == pinnedSource && index < pinnedCount {
				item.ScoreBreakdown.Flags = "pinned"
			}
			items = append(items, item)
			seenBySource[source] = index + 1
		}
		return items
	}

	feasible := makeItems([]string{"a", "a", "a", "a", "b", "b", "b", "b", "c", "c", "c", "c", "d", "d", "d", "d", "e", "e", "e", "e"}, "", 0)
	if applied, reason := podsFirstPageConstraintStatus(feasible); !applied || reason != "" {
		t.Fatalf("feasible source cap = applied %t reason %q", applied, reason)
	}

	sparse := makeItems([]string{"a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "b", "b", "b", "b", "b", "b", "b", "b", "b", "b"}, "", 0)
	if applied, reason := podsFirstPageConstraintStatus(sparse); applied || reason != "fewer_than_five_eligible_sources" {
		t.Fatalf("sparse source cap = applied %t reason %q", applied, reason)
	}

	orderRelaxed := makeItems([]string{"a", "a", "a", "a", "a", "a", "b", "b", "b", "b", "c", "c", "c", "c", "d", "d", "d", "d", "e", "e", "e", "e"}, "", 0)
	if applied, reason := podsFirstPageConstraintStatus(orderRelaxed); applied || reason != "source_cap_relaxed_by_order_constraints" {
		t.Fatalf("order-relaxed source cap = applied %t reason %q", applied, reason)
	}

	pinned := makeItems([]string{"editorial", "editorial", "editorial", "editorial", "editorial", "b", "b", "b", "b", "c", "c", "c", "c", "d", "d", "d", "d", "e", "e", "e", "e"}, "editorial", 5)
	if applied, reason := podsFirstPageConstraintStatus(pinned); !applied || reason != "pinned_editorial_override" {
		t.Fatalf("pinned source cap = applied %t reason %q", applied, reason)
	}
}

func TestPodsCursorV2AndLegacyCursorPreserveBoundaries(t *testing.T) {
	now := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	filterDigest := "filter-digest"
	lastID := uuid.New()
	v2 := encodePodsCursorV2(podsCursorV2{AssemblyTime: now, LastID: lastID, Mode: podsAssemblyRanked, FilterDigest: filterDigest})
	_, asOf, parsedID, legacyTimestamp, hasCursor, err := parsePodsCursor(v2, "20", podsAssemblyRanked, filterDigest, now.Add(time.Minute))
	if err != nil || !hasCursor || parsedID != lastID || !asOf.Equal(now) || !legacyTimestamp.IsZero() {
		t.Fatalf("v2 cursor did not round-trip: as_of=%v id=%v legacy=%v has=%v err=%v", asOf, parsedID, legacyTimestamp, hasCursor, err)
	}

	legacyBoundary := now.Add(-time.Hour)
	legacy := utils.EncodeCursor(legacyBoundary, uuid.New())
	_, legacyAsOf, _, parsedTimestamp, hasCursor, err := parsePodsCursor(legacy, "20", podsAssemblyChronological, podsAssemblyFilterDigest(podsAssemblyRequest{Mode: podsAssemblyChronological}), now)
	if err != nil || !hasCursor || !legacyAsOf.Equal(now) || !parsedTimestamp.Equal(legacyBoundary) {
		t.Fatalf("legacy cursor did not preserve its timestamp: as_of=%v timestamp=%v has=%v err=%v", legacyAsOf, parsedTimestamp, hasCursor, err)
	}

	items := []ScoredItem{
		scoredPodsFixture(uuid.New(), "source-a", now.Add(-30*time.Minute), 3),
		scoredPodsFixture(uuid.New(), "source-b", now.Add(-2*time.Hour), 2),
	}
	page, _ := paginatePodsAssemblyWithBoundary(items, uuid.New(), legacyBoundary, 20)
	if len(page) != 1 || page[0].Item.PublicID != items[1].Item.PublicID {
		t.Fatalf("legacy timestamp fallback returned %#v, want the first older item", page)
	}
}
