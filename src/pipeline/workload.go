package pipeline

import (
	"content-management-system/src/models"
	"content-management-system/src/workload"
)

const workloadOwner = "pipeline_repair"

func pipelineWorkloadClass(stage string) string {
	switch stage {
	case models.PipelineStageTextEmbedding:
		return "text_embedding"
	case models.PipelineStageMediaDownload, models.PipelineStageMediaTranscode, models.PipelineStageMediaThumbnail, models.PipelineStageMediaDeliveryGeneration:
		return "pods_stage"
	default:
		return ""
	}
}

func pipelineWorkloadRequest(request models.PipelineRepairRequest) workload.Request {
	return workload.Request{
		TenantID: request.TenantID, System: "pipeline_repair", Class: pipelineWorkloadClass(request.Stage), Units: 1,
		RunID: request.PublicID.String(), Origin: workload.OriginAutomation, OwnerKind: workloadOwner, OwnerID: request.PublicID.String(),
	}
}
