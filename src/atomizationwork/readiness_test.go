package atomizationwork

import (
	"strings"
	"testing"

	"content-management-system/src/supply"

	"github.com/google/uuid"
)

func TestAtomizationOwnerStepFailsBeforeDatabaseWhenAggregationIsStale(t *testing.T) {
	restore := supply.SetSupplyOwnerReadinessForTest(map[string]supply.SupplyOwnerReadiness{
		"aggregation": {State: "stale", Detail: "dispatcher stale"},
	})
	defer restore()
	if err := Begin(nil, "not-a-request", "aggregation-atomization", uuid.New()); err == nil || !strings.Contains(err.Error(), "readiness") {
		t.Fatalf("expected readiness denial before owner effect, got %v", err)
	}
}
