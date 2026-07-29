package controllers

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"content-management-system/src/models"

	"github.com/google/uuid"
)

func TestDecodeRetentionManifestRejectsTamperingAndKeepsExactScope(t *testing.T) {
	storyID := uuid.MustParse("00000000-0000-0000-0000-000000000101")
	leadID := uuid.MustParse("00000000-0000-0000-0000-000000000102")
	retireID := uuid.MustParse("00000000-0000-0000-0000-000000000103")
	payload := retentionManifestPayload{
		TenantID: "default", PolicyVersion: 3, Timezone: "Asia/Riyadh",
		Stories: []retentionManifestStory{{StoryID: storyID, LeadID: leadID, RetireIDs: []uuid.UUID{retireID}}},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	manifest := models.RetentionCompactionManifest{TenantID: "default", PolicyVersion: 3, Timezone: "Asia/Riyadh", ManifestHash: hex.EncodeToString(sum[:]), Evidence: raw}
	decoded, err := decodeRetentionManifest(manifest)
	if err != nil {
		t.Fatalf("decode valid manifest: %v", err)
	}
	stories, anchors, _, retire := compactManifestIDs(decoded)
	if len(stories) != 1 || stories[0] != storyID || len(anchors) != 1 || anchors[0] != leadID || len(retire) != 1 || retire[0] != retireID {
		t.Fatalf("unexpected exact scope: stories=%v anchors=%v retire=%v", stories, anchors, retire)
	}
	manifest.ManifestHash = "0000000000000000000000000000000000000000000000000000000000000000"
	if _, err := decodeRetentionManifest(manifest); err == nil {
		t.Fatal("tampered manifest hash was accepted")
	}
}

func TestRetentionItemIsFullTreatsLegacyNullAsFull(t *testing.T) {
	if !retentionItemIsFull(models.ContentItem{}) {
		t.Fatal("legacy NULL retention fields must preserve full/full_member semantics")
	}
	full := "full"
	if retentionItemIsFull(models.ContentItem{NewsRetentionState: &full}) {
		t.Fatal("partially stamped retention identity must fail closed")
	}
	state, role := "compact", "lead"
	if retentionItemIsFull(models.ContentItem{NewsRetentionState: &state, NewsFeedRole: &role}) {
		t.Fatal("compact lead must not be treated as a full legacy item")
	}
}

func TestSnapshotContainsRetiredIDDetectsFeaturedAndRelatedMembers(t *testing.T) {
	retired := uuid.MustParse("00000000-0000-0000-0000-000000000201")
	member := uuid.MustParse("00000000-0000-0000-0000-000000000202")
	slides := []StorySlide{{
		Featured: StoryFeatured{StorySummary: StorySummary{LeadID: member}, Members: []StoryMember{{ID: retired}}},
	}}
	raw, _ := json.Marshal(slides)
	found, ok := snapshotContainsRetiredID(models.NewsSnapshot{Slides: raw}, map[uuid.UUID]bool{retired: true})
	if !ok || found != retired {
		t.Fatalf("retired member = %s, found=%t; want snapshot detection", found, ok)
	}
}

func TestRetentionTombstoneIdentityIsStableAndSourceScoped(t *testing.T) {
	key := "rss:https://example.test/article"
	url := "HTTPS://EXAMPLE.TEST/Article "
	sourceName := "Example"
	feed := "https://example.test/feed"
	item := models.ContentItem{IdempotencyKey: &key, OriginalURL: &url, SourceName: &sourceName, SourceFeedURL: &feed, Source: models.SourceTypeRSS}
	identityA, sourceA, urlA, err := retentionTombstoneIdentity("default", item)
	if err != nil {
		t.Fatal(err)
	}
	item.SourceName = ptrString("Different attribution")
	identityB, sourceB, urlB, err := retentionTombstoneIdentity("default", item)
	if err != nil {
		t.Fatal(err)
	}
	if identityA != identityB || urlA != urlB {
		t.Fatal("canonical ingest identity changed when only source attribution changed")
	}
	if sourceA == sourceB {
		t.Fatal("source identity did not preserve attribution distinction")
	}
}

func compactTestItem(id string, source string, likes, shares, comments, views int, published time.Time) models.ContentItem {
	parsed := uuid.MustParse(id)
	return models.ContentItem{PublicID: parsed, SourceName: &source, LikeCount: likes, ShareCount: shares, CommentCount: comments, ViewCount: views, PublishedAt: &published}
}

func TestSelectCompactMembersFreezesLeadDiverseRepresentativesAndProtectedRows(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	members := []models.ContentItem{
		compactTestItem("00000000-0000-0000-0000-000000000001", "alpha", 10, 0, 0, 0, now.Add(-4*time.Hour)),
		compactTestItem("00000000-0000-0000-0000-000000000002", "beta", 0, 1, 0, 0, now.Add(-time.Hour)),
		compactTestItem("00000000-0000-0000-0000-000000000003", "gamma", 0, 0, 0, 0, now.Add(-2*time.Hour)),
		compactTestItem("00000000-0000-0000-0000-000000000004", "delta", 0, 0, 0, 0, now.Add(-3*time.Hour)),
		compactTestItem("00000000-0000-0000-0000-000000000005", "epsilon", 0, 0, 0, 0, now.Add(-5*time.Hour)),
	}
	protectedID := members[4].PublicID
	lead, reps, protectedOnly, retire, ok := selectCompactMembers(members, map[uuid.UUID]bool{protectedID: true})
	if !ok || lead.PublicID != members[0].PublicID {
		t.Fatalf("lead = %s, want highest-engagement alpha", lead.PublicID)
	}
	if len(reps) != 3 || reps[0].PublicID != members[1].PublicID {
		t.Fatalf("representatives = %#v, want newest beta first and exactly three", reps)
	}
	if len(protectedOnly) != 1 || protectedOnly[0].PublicID != protectedID {
		t.Fatalf("protected-only = %#v, want epsilon", protectedOnly)
	}
	if len(retire) != 0 {
		t.Fatalf("retire = %#v, want no unprotected surplus after 1+3 selection", retire)
	}
}

func TestSelectCompactMembersUsesStableUUIDTieBreak(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	members := []models.ContentItem{
		compactTestItem("00000000-0000-0000-0000-00000000000b", "alpha", 1, 0, 0, 0, now),
		compactTestItem("00000000-0000-0000-0000-00000000000a", "beta", 1, 0, 0, 0, now),
	}
	lead, _, _, _, ok := selectCompactMembers(members, nil)
	if !ok || lead.PublicID.String() != "00000000-0000-0000-0000-00000000000a" {
		t.Fatalf("tie lead = %s, want lexically smallest UUID", lead.PublicID)
	}
}

func TestRetentionStoryCursorRoundTripAndRejectsTampering(t *testing.T) {
	candidate := retentionStoryCandidate{
		StoryID:      uuid.MustParse("00000000-0000-0000-0000-0000000000aa"),
		LastMemberAt: time.Date(2026, 7, 28, 12, 34, 56, 0, time.FixedZone("Riyadh", 3*60*60)),
	}
	raw := encodeRetentionStoryCursor(candidate)
	decoded, err := decodeRetentionStoryCursor(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.StoryID != candidate.StoryID || !decoded.LastMemberAt.Equal(candidate.LastMemberAt.UTC()) {
		t.Fatalf("decoded cursor = %#v, want %#v", decoded, candidate)
	}
	if _, err := decodeRetentionStoryCursor(raw + "tampered"); err == nil {
		t.Fatal("tampered retention cursor was accepted")
	}
	if _, err := decodeRetentionStoryCursor("not-a-cursor"); err == nil {
		t.Fatal("malformed retention cursor was accepted")
	}
}

func TestHistoricalCursorRoundTripAndRejectsTampering(t *testing.T) {
	published := time.Date(2026, 6, 30, 23, 59, 0, 0, time.UTC)
	item := models.ContentItem{PublicID: uuid.MustParse("00000000-0000-0000-0000-0000000000bb"), PublishedAt: &published, CreatedAt: published.Add(-time.Hour)}
	raw := encodeHistoricalCursor(item)
	decoded, err := decodeHistoricalCursor(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.ContentID != item.PublicID || !decoded.OrderedAt.Equal(published) {
		t.Fatalf("decoded historical cursor = %#v", decoded)
	}
	if _, err := decodeHistoricalCursor(raw + "tampered"); err == nil {
		t.Fatal("tampered historical cursor was accepted")
	}
}
