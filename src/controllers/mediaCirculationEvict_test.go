package controllers

import (
	"content-management-system/src/models"
	"testing"
	"time"
)

func TestCirculationMediaValueEngagementOrdering(t *testing.T) {
	high := models.ContentItem{LikeCount: 500, ViewCount: 10000, ShareCount: 200, CommentCount: 150}
	low := models.ContentItem{LikeCount: 1, ViewCount: 10, ShareCount: 0, CommentCount: 0}
	if circulationMediaValue(high) <= circulationMediaValue(low) {
		t.Errorf("high-engagement value %.3f should exceed low-engagement value %.3f",
			circulationMediaValue(high), circulationMediaValue(low))
	}
}

func TestCirculationMediaValueFreshnessDemoted(t *testing.T) {
	// Two items with identical engagement/quality/suitability but very different
	// ages must score (nearly) identically — media value is durable, not recency.
	old := time.Now().Add(-365 * 24 * time.Hour)
	fresh := time.Now()
	a := models.ContentItem{LikeCount: 50, ViewCount: 1000, PublishedAt: &old}
	b := models.ContentItem{LikeCount: 50, ViewCount: 1000, PublishedAt: &fresh}
	if got := circulationMediaValue(a) - circulationMediaValue(b); got > 1e-9 || got < -1e-9 {
		t.Errorf("value must be age-independent (freshness demoted); delta=%v", got)
	}
}

func TestCirculationMediaValueUnsuitablePenalty(t *testing.T) {
	base := models.ContentItem{LikeCount: 50, ViewCount: 1000, MediaSuitability: models.MediaSuitabilityAudioFirstShow}
	unsuitable := models.ContentItem{LikeCount: 50, ViewCount: 1000, MediaSuitability: models.MediaSuitabilityUnsuitable}
	if circulationMediaValue(unsuitable) >= circulationMediaValue(base) {
		t.Errorf("unsuitable media (%.3f) must score below audio-first (%.3f)",
			circulationMediaValue(unsuitable), circulationMediaValue(base))
	}
}

func TestMapRoleToEvictVerdict(t *testing.T) {
	policy := models.StoragePolicy{ArchiveAction: "re_encode"}
	tests := []struct {
		name        string
		role        string
		coldEnabled bool
		wantVerdict string
		wantOK      bool
	}{
		{"parent cold-enabled -> move_to_cold", storageRoleAtomizedParentSource, true, mediaCircVerdictMoveToCold, true},
		{"parent no-cold -> recoverable_delete", storageRoleAtomizedParentSource, false, mediaCircVerdictRecoverableDelete, true},
		{"failed/orphan -> recoverable_delete", storageRoleFailedOrOrphanArtifact, false, mediaCircVerdictRecoverableDelete, true},
		{"unsuitable -> recoverable_delete", storageRoleUnsuitableMedia, true, mediaCircVerdictRecoverableDelete, true},
		{"dormant -> re_encode", storageRoleDormantFeedUnit, true, mediaCircVerdictReEncode, true},
		{"hot -> not a candidate", storageRoleHotFeedUnit, true, "", false},
		{"normal -> not a candidate", storageRoleNormalFeedUnit, true, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			verdict, action, ok := mapRoleToEvictVerdict(tt.role, policy, tt.coldEnabled)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && (verdict != tt.wantVerdict || action != tt.wantVerdict) {
				t.Errorf("verdict/action = %q/%q, want %q", verdict, action, tt.wantVerdict)
			}
		})
	}
}

func TestEvictVerdictCounts(t *testing.T) {
	recs := []evictRecommendation{
		{Verdict: mediaCircVerdictProtect},
		{Verdict: mediaCircVerdictProtect},
		{Verdict: mediaCircVerdictRankDown},
		{Verdict: mediaCircVerdictReEncode},
	}
	counts := evictVerdictCounts(recs)
	if counts[mediaCircVerdictProtect] != 2 || counts[mediaCircVerdictRankDown] != 1 || counts[mediaCircVerdictReEncode] != 1 {
		t.Errorf("unexpected counts: %+v", counts)
	}
}
