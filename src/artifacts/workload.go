package artifacts

import (
	"content-management-system/src/models"
	"content-management-system/src/workload"
)

const workloadOwner = "artifact_coverage"

func artifactWorkloadClass(artifact string) string {
	switch artifact {
	case ArtifactTranscript:
		return "stt_batch"
	case ArtifactImageEmbedding:
		return "image_embedding"
	case ArtifactTextEmbedding:
		return "text_embedding"
	case ArtifactLLMMetadata:
		return "llm_batch"
	default:
		return ""
	}
}

func artifactWorkloadRequest(request models.ArtifactCoverageRequest) workload.Request {
	return workload.Request{
		TenantID: request.TenantID, System: "artifact_coverage", Class: artifactWorkloadClass(request.Artifact), Units: 1,
		RunID: request.PublicID.String(), Origin: workload.OriginAutomation, OwnerKind: workloadOwner, OwnerID: request.PublicID.String(),
	}
}
