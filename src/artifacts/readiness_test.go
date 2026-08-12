package artifacts

import (
	"strings"
	"testing"

	"content-management-system/src/supply"

	"github.com/google/uuid"
)

func TestArtifactOwnerStepFailsBeforeDatabaseWhenMediaIsStale(t *testing.T) {
	restore := supply.SetSupplyOwnerReadinessForTest(map[string]supply.SupplyOwnerReadiness{
		"media": {State: "stale", Detail: "ARQ worker stale"},
	})
	defer restore()
	if _, err := Begin(nil, "not-a-request", MediaOwner, uuid.New()); err == nil || !strings.Contains(err.Error(), "readiness") {
		t.Fatalf("expected readiness denial before owner effect, got %v", err)
	}
}
