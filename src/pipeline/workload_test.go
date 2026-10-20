package pipeline

import (
	"testing"

	"content-management-system/src/models"
	"content-management-system/src/workload"
)

func TestRepairCapacityTracksTheUnderlyingExecutionOwner(t *testing.T) {
	for _, stage := range []string{models.PipelineStageMediaDownload, models.PipelineStageMediaTranscode, models.PipelineStageMediaThumbnail, models.PipelineStageMediaDeliveryGeneration, models.PipelineStageTextEmbedding} {
		class, ok := workload.Class(pipelineWorkloadClass(stage))
		if !ok {
			t.Errorf("repair stage %s lacks a governor class", stage)
			continue
		}
		resource := "aggregation_svc"
		if stage == models.PipelineStageTextEmbedding {
			resource = "enrichment_svc"
		}
		if class.Weights[resource] < 1 {
			t.Errorf("repair stage %s reserves the wrong execution owner", stage)
		}
	}
	if pipelineWorkloadClass("not_a_native_stage") != "" {
		t.Fatal("unregistered stage was assigned a generic capacity class")
	}
}
