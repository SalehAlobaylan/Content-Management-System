package spaceid

import "testing"

// TestGoldenCrossLanguage pins the exact hashes the Python implementation
// (Enrichment-Service/src/common/spaceid.py) produces for the same inputs. If
// this fails, the two languages have diverged and every vector one service
// writes would be uncomparable to what CMS expects. Regenerate BOTH sides
// deliberately, never just to make the test pass.
func TestGoldenCrossLanguage(t *testing.T) {
	b := Basis{Model: "test-model", Revision: "abc123", Dimensions: 4, Normalized: true, Pooling: "mean"}
	const wantSpace = "58ce573ed10df8af2a0197fde7f7114cd26f844de65b14f0bc95633d36d8a70f"
	const wantProducer = "dd5e4491b5dfb06a6696e765a2a5e813de7e61ae043ebd9bc9585e3f132f0791"

	if got := b.SpaceID(); got != wantSpace {
		t.Fatalf("SpaceID mismatch with Python golden:\n got  %s\n want %s", got, wantSpace)
	}
	if got := ProducerID(wantSpace, "r:v1"); got != wantProducer {
		t.Fatalf("ProducerID mismatch with Python golden:\n got  %s\n want %s", got, wantProducer)
	}
}

// TestUnresolvedRevisionYieldsEmpty proves an unresolved (blank) revision never
// produces a stable-looking identity — the fail-closed rule.
func TestUnresolvedRevisionYieldsEmpty(t *testing.T) {
	b := Basis{Model: "m", Revision: "  ", Dimensions: 1024, Normalized: true, Pooling: "p"}
	if b.Resolved() {
		t.Fatal("blank revision must not be Resolved()")
	}
	if b.SpaceID() != "" {
		t.Fatal("blank revision must yield empty space_id")
	}
	if ProducerID("", "r:v1") != "" {
		t.Fatal("empty space_id must yield empty producer_id")
	}
}

// TestRecipeChangesProducerNotSpace: a recipe-only change keeps space_id but
// changes producer_id ("same space, must recompute this surface").
func TestRecipeChangesProducerNotSpace(t *testing.T) {
	b := Basis{Model: "m", Revision: "rev", Dimensions: 8, Normalized: false, Pooling: "cls"}
	space := b.SpaceID()
	if space == "" {
		t.Fatal("resolved basis must yield a space_id")
	}
	if ProducerID(space, "a:v1") == ProducerID(space, "b:v1") {
		t.Fatal("different recipes must produce different producer_ids")
	}
}

// TestRevisionChangesSpace: same model label, different immutable revision, must
// be a different space.
func TestRevisionChangesSpace(t *testing.T) {
	a := Basis{Model: "m", Revision: "rev1", Dimensions: 8, Normalized: true, Pooling: "p"}
	b := Basis{Model: "m", Revision: "rev2", Dimensions: 8, Normalized: true, Pooling: "p"}
	if a.SpaceID() == b.SpaceID() {
		t.Fatal("different revisions must produce different space_ids")
	}
}
