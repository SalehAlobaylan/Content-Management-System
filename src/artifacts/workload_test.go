package artifacts

import (
	"testing"

	"content-management-system/src/workload"
)

func TestEveryArtifactOwnerUsesRegisteredCapacityClass(t *testing.T) {
	for artifact, descriptor := range descriptors {
		class, ok := workload.Class(artifactWorkloadClass(artifact))
		if !ok {
			t.Errorf("artifact %s has no governor class", artifact)
			continue
		}
		ownerResource := "media_svc"
		if descriptor.Owner == EnrichmentOwner {
			ownerResource = "enrichment_svc"
		}
		if class.Weights[ownerResource] < 1 {
			t.Errorf("artifact %s does not reserve its %s owner", artifact, descriptor.Owner)
		}
	}
}
