package supply

import (
	"testing"
	"time"
)

func TestAggregationOwnerProtocolsAreClosedAndBounded(t *testing.T) {
	for _, protocol := range []string{SupplyActionOwnerAggregationDispatcher, SupplyActionOwnerAggregationReceipt} {
		if _, _, err := ClaimNextSupplyActionForOwner(nil, protocol, "aggregation", time.Minute); err == nil {
			t.Fatalf("%s must still require a database", protocol)
		}
	}
	if _, _, err := ClaimNextSupplyActionForOwner(nil, "aggregation_anything", "aggregation", time.Minute); err == nil {
		t.Fatal("unregistered owner protocol must be rejected")
	}
	if _, err := BeginSupplyActionOwnerStep(nil, "tenant", "request", "aggregation", "token", "queue_admin"); err == nil {
		t.Fatal("unregistered owner step must be rejected before database access")
	}
}

func TestUnitAdoptionPreparationRejectsUntrustedInputs(t *testing.T) {
	if _, _, err := PrepareUnitAdoption(nil, "action", "aggregation", "token", time.Minute, time.Minute); err == nil {
		t.Fatal("unit adoption accepted a missing CMS store")
	}
	if err := MarkUnitAdoptionQueued(nil, "action", "aggregation", "token"); err == nil {
		t.Fatal("unit adoption acknowledgement accepted a missing CMS store")
	}
}
