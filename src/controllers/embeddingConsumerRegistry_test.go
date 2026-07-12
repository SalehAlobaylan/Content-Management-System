package controllers

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Embedding & Model Lifecycle System (stage 10) — static comparability guard.
//
// Every code path that runs a pgvector `<=>` cosine comparison compares two
// vectors and is therefore a comparability risk: if the two operands come from
// different vector spaces the cosine is silently meaningless. This test
// inventories the source tree for real `<=>` usage and fails when a file uses it
// that is not registered below with a mapped consumer key. A developer adding a
// new similarity query must register it (and, per §5, add its comparability
// test) rather than silently introducing an unguarded cross-space comparison.
//
// This is the "a newly added `<=>` consumer not registered in the surface
// registry fails a static test" guarantee from the plan (§5 / Slice 1).

// knownSemanticConsumers maps each source file that legitimately runs a `<=>`
// comparison to the registry consumer key(s) it implements. Every value here
// MUST exist in registeredConsumerKeys() (asserted below).
var knownSemanticConsumers = map[string][]string{
	"storyClassifier.go":             {"story_classify"},
	"adminStoriesController.go":      {"story_related"},
	"feedStories.go":                 {"story_related"},
	"internalDiscoveryController.go": {"discovery_dense"},
	"internalContentController.go":   {"knn_dense", "related_dense"},
	"intelligenceController.go":      {"related_dense"},
}

// semanticExemptFiles use `<=>` only in comments or as pure string/literal
// helpers, not as a live cross-space comparison. Listed explicitly so the test
// stays honest — an exemption is a decision, not an accident.
var semanticExemptFiles = map[string]bool{
	"pgvector_literal.go":               true, // vector literal formatting helper
	"embeddingSurfaceRegistry.go":       true, // this system's registry (doc comment)
	"embeddingConsumerRegistry_test.go": true,
}

// lineUsesOperatorInCode reports whether `<=>` appears in code (not after a //).
func lineUsesOperatorInCode(line string) bool {
	idx := strings.Index(line, "<=>")
	if idx < 0 {
		return false
	}
	if c := strings.Index(line, "//"); c >= 0 && c < idx {
		return false
	}
	return true
}

func TestNoUnregisteredSimilarityConsumers(t *testing.T) {
	// Assert every mapped consumer key is a real registry key first.
	registered := registeredConsumerKeys()
	for file, keys := range knownSemanticConsumers {
		for _, k := range keys {
			if !registered[k] {
				t.Errorf("%s maps to consumer key %q which is not in the surface registry", file, k)
			}
		}
	}

	// Walk the CMS source (controllers + intelligence) for `<=>` in code.
	roots := []string{".", "../intelligence"}
	offenders := map[string]bool{}
	for _, root := range roots {
		err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			base := filepath.Base(path)
			if strings.HasSuffix(base, "_test.go") {
				return nil
			}
			data, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			if knownSemanticConsumers[base] != nil && strings.Contains(string(data), "<=>") &&
				!strings.Contains(string(data), "space_id") && !strings.Contains(string(data), "SpaceID") {
				t.Errorf("file %q has a registered similarity consumer but no visible space comparability guard", base)
			}
			for _, line := range strings.Split(string(data), "\n") {
				if lineUsesOperatorInCode(line) {
					if !semanticExemptFiles[base] && knownSemanticConsumers[base] == nil {
						offenders[base] = true
					}
					break
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}

	for f := range offenders {
		t.Errorf("file %q runs a pgvector `<=>` comparison but is not registered in "+
			"knownSemanticConsumers — register it with a comparability guard (stage 10 §5), "+
			"or add it to semanticExemptFiles if it is genuinely not a cross-space comparison", f)
	}
}
