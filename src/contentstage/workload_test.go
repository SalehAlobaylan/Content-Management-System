package contentstage

import (
	"testing"

	"content-management-system/src/models"
	"content-management-system/src/workload"
	"github.com/google/uuid"
)

func TestGovernorCoversEveryExternalStageAndExemptsCMSProjection(t *testing.T) {
	for key, descriptor := range descriptors {
		class := stageWorkloadClass(key)
		if descriptor.Owner == models.ContentStageOwnerCMS {
			if class != "" {
				t.Errorf("deterministic CMS projection %s consumes external capacity", key)
			}
			continue
		}
		if _, registered := workload.Class(class); !registered {
			t.Errorf("external stage %s has no registered capacity class (%q)", key, class)
		}
	}
}

func TestStageRetryUsesNewPermitIdentityWithoutHumanBypass(t *testing.T) {
	r := models.ContentStageRequest{PublicID: uuid.New(), TenantID: "tenant-a", Stage: models.ContentStagePodsTranscript, ClaimEpoch: 4}
	before := stageWorkloadRequest(r)
	r.ClaimEpoch++
	after := stageWorkloadRequest(r)
	if before.RequestKey == after.RequestKey || before.OwnerID != after.OwnerID || before.TenantID != after.TenantID {
		t.Fatal("native retry must retain owner correlation with a fresh permit identity")
	}
	if before.Origin != workload.OriginAutomation || before.Units != 1 || before.Class != "stt_batch" {
		t.Fatalf("transcript stage changed its native execution contract: %#v", before)
	}
}
