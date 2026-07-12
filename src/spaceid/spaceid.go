// Package spaceid computes the two immutable vector-space identities the
// Embedding & Model Lifecycle System (stage 10) depends on. Model name alone is
// not a comparability key — a mutable repository label can serve different
// weights — so every stored vector carries:
//
//   - space_id:    may these vectors be compared? SHA-256 over the immutable
//     basis contract (artifact revision/digest, projection/pooling, dimensions,
//     normalization). Equal space_id ⇒ cosine is meaningful.
//   - producer_id: must this surface be recomputed? SHA-256 over space_id plus
//     the surface's input/preprocessing recipe version. Content text, topic
//     labels, and discovery profiles have different recipes but stay comparable
//     when their space_id matches.
//
// The hash inputs and their canonical JSON serialization MUST stay byte-identical
// to the Python implementations in Enrichment-Service and Media-Service, or a
// vector embedded by a service will never match the identity CMS expects. The
// canonical form is: compact JSON (no spaces), object keys sorted ascending,
// UTF-8, no trailing newline. Golden fixtures pin cross-language agreement.
package spaceid

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
)

// Basis is the immutable comparability contract of a vector space. Two vectors
// are comparable iff their Basis hashes to the same space_id.
type Basis struct {
	// Model is the display label (e.g. "Qwen/Qwen3-Embedding-0.6B"). Included in
	// the hash because a different model is a different space, but it is never
	// the sole identity — Revision is what defends against a mutable label.
	Model string `json:"model"`
	// Revision is the immutable artifact digest/commit the weights resolve to at
	// service boot. A bare branch label ("main") is NOT a revision; when a
	// service cannot resolve one it must report Revision="" and the descriptor is
	// treated as not lifecycle-ready.
	Revision string `json:"revision"`
	// Dimensions is the output vector length (1024 text, 512 image).
	Dimensions int `json:"dimensions"`
	// Normalized records whether the space is L2-normalized (cosine == dot).
	Normalized bool `json:"normalized"`
	// Pooling is the projection/pooling contract identifier
	// (e.g. "sentence-transformers-config", "clip-visual-proj").
	Pooling string `json:"pooling"`
}

// canonicalJSON serializes v as compact JSON with object keys sorted ascending
// and HTML escaping DISABLED, so the byte output matches Python's
// json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).
// encoding/json sorts map keys automatically; SetEscapeHTML(false) stops Go from
// turning "/", "<", ">", "&" into \uXXXX (Python does not escape those), which is
// what keeps the two languages' space_id hashes identical.
func canonicalJSON(v map[string]any) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
	// Encoder.Encode appends a trailing newline; strip it so the hashed bytes
	// match Python's newline-free json.dumps output.
	return bytes.TrimRight(buf.Bytes(), "\n")
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// SpaceID returns the 64-char hex SHA-256 identity of a vector space. Returns ""
// when the basis is not resolvable to an immutable revision — an unresolved
// revision must never masquerade as a stable identity.
func (b Basis) SpaceID() string {
	if strings.TrimSpace(b.Revision) == "" {
		return ""
	}
	return sha256Hex(canonicalJSON(map[string]any{
		"model":      b.Model,
		"revision":   b.Revision,
		"dimensions": b.Dimensions,
		"normalized": b.Normalized,
		"pooling":    b.Pooling,
	}))
}

// Resolved reports whether the basis pins an immutable revision (and therefore
// yields a usable space_id). A descriptor whose Resolved()==false may be shown
// in an audit as last-known identity but must never gate a write or a campaign.
func (b Basis) Resolved() bool { return strings.TrimSpace(b.Revision) != "" }

// ProducerID returns the 64-char hex identity of a specific surface's producer:
// the space it targets plus the input-recipe version that surface uses. A
// recipe-only change keeps space_id but changes producer_id, which is exactly
// "same space, must recompute this surface".
//
// recipe is a stable version constant per surface (e.g.
// "content-title-excerpt-body:v1", "topic-bilingual-label:v1"). Returns "" when
// the underlying space is unresolved.
func ProducerID(spaceID, recipe string) string {
	if strings.TrimSpace(spaceID) == "" {
		return ""
	}
	return sha256Hex(canonicalJSON(map[string]any{
		"recipe":   recipe,
		"space_id": spaceID,
	}))
}

// SortedRecipes is a test/debug helper that returns recipe keys in the same
// deterministic order the hash sees, so golden fixtures read predictably.
func SortedRecipes(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
