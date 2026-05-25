package utils

import "fmt"

// PgvectorToLiteral converts a float32 slice to a PostgreSQL vector literal
// suitable for inlining into a raw SQL ORDER BY clause (e.g. `embedding <=> '[...]'`).
//
// Used by:
//   - feedController.fetchRelatedItems (anchor-driven related items for News feed)
//   - internalContentController.InternalKNNDense (raw kNN endpoint for /v1/related)
//
// %g is correct for round-tripping float32 → string → vector literal because
// pgvector parses standard decimal notation; the precision loss is negligible
// for cosine similarity at retrieval time.
func PgvectorToLiteral(v []float32) string {
	if len(v) == 0 {
		return "[]"
	}
	s := "["
	for i, f := range v {
		if i > 0 {
			s += ","
		}
		s += fmt.Sprintf("%g", f)
	}
	s += "]"
	return s
}
