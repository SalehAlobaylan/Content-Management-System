package controllers

import (
	"testing"

	"content-management-system/src/models"
)

func pipelineStatus(value string) *string { return &value }

func TestPipelineStageForItemSurfacesMediaPreparationAndFailures(t *testing.T) {
	tests := []struct {
		name string
		item mediaAtomizationPipelineItem
		want string
	}{
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
