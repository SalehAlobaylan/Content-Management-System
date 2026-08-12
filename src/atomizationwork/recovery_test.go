package atomizationwork

import (
	"testing"

	"gorm.io/datatypes"
)

func TestAtomizationCrashMatrixNeverBlindlyRestarts(t *testing.T) {
	tests := []struct {
		name, checkpoints, want string
		effectStarted           bool
	}{
		{"before effect", `{}`, "adopt_same_job", false},
		{"after plan", `{"plan_persisted":{"digest":"a"}}`, "verify_from_plan_persisted", true},
		{"after first cut", `{"first_cut":{"object":"one"}}`, "verify_from_first_cut", true},
		{"after uploads", `{"uploads_complete":{"count":2}}`, "verify_from_uploads_complete", true},
		{"after children", `{"children_persisted":{"count":2}}`, "verify_from_children_persisted", true},
		{"after embedding handoff", `{"embedding_handoff":{"count":2}}`, "verify_from_embedding_handoff", true},
		{"after final receipt", `{"owner_complete":{"child_ids":[]}}`, "verify_from_owner_complete", true},
		{"unknown post effect", `{}`, "verify_effect_unknown", true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := AtomizationRecoveryDisposition(datatypes.JSON([]byte(test.checkpoints)), test.effectStarted)
			if err != nil || got != test.want {
				t.Fatalf("got %q, %v; want %q", got, err, test.want)
			}
			if test.effectStarted && got == "adopt_same_job" {
				t.Fatal("post-effect recovery cannot re-enter execution")
			}
		})
	}
}

func TestAtomizationMalformedCheckpointRequiresManualAttention(t *testing.T) {
	got, err := AtomizationRecoveryDisposition(datatypes.JSON([]byte(`{"broken"`)), true)
	if err == nil || got != "manual_attention" {
		t.Fatalf("got %q, %v", got, err)
	}
}
