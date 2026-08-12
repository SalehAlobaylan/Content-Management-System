package supply

import "testing"

func TestSupplyActionRegistryIsStaticAndBounded(t *testing.T) {
	for _, key := range SupplyActionKeys() {
		descriptor, ok := SupplyAction(key)
		if !ok || descriptor.Key != key || descriptor.TargetCap != 1 || descriptor.ExecutionOwner == "" || descriptor.Verification == "" {
			t.Fatalf("invalid static descriptor for %q", key)
		}
	}
	if _, ok := SupplyAction("source_run.retry_everything"); ok {
		t.Fatal("generic/bulk retry must never be admitted")
	}
}
