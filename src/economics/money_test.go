package economics

import (
	"fmt"
	"math/rand"
	"testing"
)

func mustAmount(t *testing.T, s string) Amount {
	t.Helper()
	a, e := ParseAmount(s)
	if e != nil {
		t.Fatal(e)
	}
	return a
}
func TestAllocateConservesCorrectionsAndTinyAmounts(t *testing.T) {
	for _, s := range []string{"0", "0.000000000001", "-0.000000000001", "123456789.123456789012", "-90.01"} {
		total := mustAmount(t, s)
		parts, e := Allocate(total, map[string]int64{"a": 7, "b": 18, "c": 81})
		if e != nil {
			t.Fatal(e)
		}
		sum := Zero
		for _, a := range parts {
			sum = Add(sum, a)
		}
		if sum != total {
			t.Fatalf("%s != %s", sum, total)
		}
	}
}
func TestAmountValidation(t *testing.T) {
	for _, s := range []string{"NaN", "1e3", "0.0000000000001", "", " 1", "+1"} {
		if _, e := ParseAmount(s); e == nil {
			t.Fatalf("accepted %q", s)
		}
	}
}
func TestAllocationProperties(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 1000; i++ {
		amount := mustAmount(t, fmt.Sprintf("%d.%012d", rng.Intn(200)-100, rng.Int63n(1e12)))
		weights := map[string]int64{"a": rng.Int63n(1000), "b": rng.Int63n(1000), "c": rng.Int63n(1000)}
		parts, e := Allocate(amount, weights)
		if e != nil {
			t.Fatal(e)
		}
		sum := Zero
		for _, n := range parts {
			sum = Add(sum, n)
		}
		if sum != amount {
			t.Fatal("allocation does not conserve")
		}
	}
}
func TestMissingProfileDoesNotAllocateEntireBill(t *testing.T) {
	c := Cost{Resource: "database", Metric: "requests"}
	_, qualified := infrastructureWeights(c, []Usage{{Unit: "requests", Operation: "search", Value: 1}}, nil)
	if qualified {
		t.Fatal("unmeasured profile became qualified")
	}
}
func BenchmarkCollectorObserve(b *testing.B) {
	c := NewCollector()
	c.SetPolicies([]Policy{{TenantID: "a", CollectionEnabled: true}})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Observe("a", "public", "pods_page", "requests", "ok", 1, testTime)
	}
}
func TestSignedReceiptCorrectionInvoiceConservation(t *testing.T) {
	total := mustAmount(t, "0.4")
	parts, e := ScaleSigned(total, map[string]Amount{"initial": mustAmount(t, "1"), "refund": mustAmount(t, "-0.2")})
	if e != nil {
		t.Fatal(e)
	}
	if Add(parts["initial"], parts["refund"]) != total || parts["refund"] != mustAmount(t, "-0.1") {
		t.Fatal(parts)
	}
}
