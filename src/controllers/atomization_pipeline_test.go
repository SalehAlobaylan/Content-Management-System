package controllers

import (
	"fmt"
	"testing"
	"time"

	"content-management-system/src/models"
	"github.com/google/uuid"
)

func TestWorkflowProjectionCountsFullPopulationAndReplacesStaleErrors(t *testing.T) {
	rows := make([]mediaAtomizationPipelineItem, 421)
	for i := range rows {
		rows[i] = mediaAtomizationPipelineItem{ID: fmt.Sprint(i), MediaStageState: pipelineStatus("verified"), TranscriptID: pipelineStatus("transcript"), AtomizationStageState: pipelineStatus("queued"), LatestError: pipelineStatus("old failure"), FailedOrStuck: true}
	}
	for _, lane := range projectMediaPipelineRows(rows) {
		if lane.Key == "failed" && lane.Count != 0 {
			t.Fatal("historical failures inflated current failure count")
		}
		if lane.Key == "planning" {
			if lane.Count != 421 || len(lane.Items) != 421 {
				t.Fatalf("projection truncated to %d", lane.Count)
			}
			for _, item := range lane.Items {
				if item.LatestError != nil {
					t.Fatal("old run error survived queued durable projection")
				}
			}
		}
	}
}

func pipelineStatus(value string) *string { return &value }

func TestPipelineStageForItemSurfacesMediaPreparationAndFailures(t *testing.T) {
	tests := []struct {
		name string
		item mediaAtomizationPipelineItem
		want string
	}{
		{
			name: "current queued atomization overrides historical failed run",
			item: mediaAtomizationPipelineItem{MediaStageState: pipelineStatus("verified"), TranscriptID: pipelineStatus("transcript"), AtomizationStageState: pipelineStatus("queued"), ChapteringStatus: pipelineStatus("failed")},
			want: "planning",
		},
		{
			name: "manual media remains admission lane despite historical failure",
			item: mediaAtomizationPipelineItem{MediaStageState: pipelineStatus("awaiting_approval"), FailedOrStuck: true},
			want: "awaiting_download",
		},
		{
			name: "verified cuts with pending children are not published",
			item: mediaAtomizationPipelineItem{MediaStageState: pipelineStatus("verified"), TranscriptID: pipelineStatus("transcript"), AtomizationStageState: pipelineStatus("verified"), ChildCount: 3, PublishedCount: 1, EmbeddingPendingCount: 2, ChapteringStatus: pipelineStatus("completed")},
			want: "embedding",
		},
		{
			name: "waiting media has an explicit lane",
			item: mediaAtomizationPipelineItem{ChapteringStatus: pipelineStatus("waiting_media")},
			want: "media",
		},
		{
			name: "failed content is visible in failed lane",
			item: mediaAtomizationPipelineItem{Status: string(models.ContentStatusFailed), ChapteringStatus: pipelineStatus("waiting_media"), FailedOrStuck: true},
			want: "failed",
		},
		{
			name: "stuck work wins over disabled presentation",
			item: mediaAtomizationPipelineItem{FailedOrStuck: true, AtomizationOverride: pipelineStatus(atomizationOverrideDisabled)},
			want: "failed",
		},
		{
			name: "waiting transcript remains transcript lane",
			item: mediaAtomizationPipelineItem{ChapteringStatus: pipelineStatus("waiting_transcript")},
			want: "transcript",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := pipelineStageForItem(tt.item); got != tt.want {
				t.Fatalf("pipelineStageForItem() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestWorkflowProjectionCarriesCurrentAuthorityAndParkedDisposition(t *testing.T) {
	currentFailure := "effect_unknown"
	transcriptState := models.ContentStageAwaitingApproval
	rows := projectMediaPipelineRows([]mediaAtomizationPipelineItem{{
		ID: "root-1", CurrentFailure: &currentFailure, TranscriptStageState: &transcriptState,
		AtomizationStageState: pipelineStatus(models.ContentStageFailed), UnresolvedEffects: 0, UpdatedAt: time.Now().UTC(),
	}})
	var got mediaAtomizationPipelineItem
	for _, column := range rows {
		if len(column.Items) == 1 {
			got = column.Items[0]
			break
		}
	}
	if got.Lane != "failed" || got.Disposition != "parked_failed" || got.BlockingReason != currentFailure || got.ParkedReason == "" {
		t.Fatalf("projection lost durable failure authority: %+v", got)
	}
}

func TestImmutableManifestIntentKeepsNestedAndOuterFencesIndependent(t *testing.T) {
	unitFence, outerFence := uuid.New(), uuid.New()
	base := models.MediaArtifactManifest{
		TenantID: "tenant-a", StorageTier: "primary", Bucket: "media", ObjectKey: "root/generation/unit.mp4",
		ArtifactRole: "delivery_progressive", CreatorRole: "aggregation-media-executor", InputDigest: "digest",
		UnitFenceToken: &unitFence, OuterFenceToken: &outerFence, FenceToken: &unitFence,
	}
	candidate := base
	if !artifactManifestMatchesImmutableIntent(&base, &candidate) {
		t.Fatal("same immutable ownership should replay")
	}
	differentOuter := uuid.New()
	candidate.OuterFenceToken = &differentOuter
	if artifactManifestMatchesImmutableIntent(&base, &candidate) {
		t.Fatal("unrelated outer fence must not be interchangeable")
	}
}
