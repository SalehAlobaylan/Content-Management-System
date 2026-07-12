package controllers

import (
	"strings"

	"content-management-system/src/spaceid"
)

// stampOrNil trims a provenance value and returns a *string, or nil when empty.
// Used by every vector write-back so an absent identity clears the column
// (visible unstamped debt) instead of silently inheriting the previous stamp.
func stampOrNil(v string) *string {
	if t := strings.TrimSpace(v); t != "" {
		return &t
	}
	return nil
}

// Embedding & Model Lifecycle System (stage 10) — surface registry.
//
// Every stored-vector surface is declared once here, as data (mirroring the
// Feed Integrity check-registry pattern). The audit lanes, comparability guards,
// campaign scope, and cockpit all render from this slice — nothing hardcodes a
// table or column name elsewhere. Table/column identifiers are COMPILE-TIME
// constants and are never accepted from policy or requests (no SQL-injection
// surface via the registry).
//
// Adding a future vector surface = one entry here + fixtures. `Kind` drives lane
// behavior: `item` surfaces are campaign-re-embeddable directly; `centroid` and
// `cache` surfaces refresh only through registered owner adapters (§6).

// EmbeddingSpace is the comparability domain a surface belongs to. Two surfaces
// may be compared only within the same space (and only when both operands carry
// the current space_id — enforced by the comparability guards).
const (
	EmbeddingSpaceText  = "text"
	EmbeddingSpaceImage = "image"
)

// Surface kinds — how a surface is refreshed during a campaign.
const (
	SurfaceKindItem     = "item"     // directly re-embeddable via owner inference adapter
	SurfaceKindCentroid = "centroid" // recomputed only via owner rebuild adapter
	SurfaceKindCache    = "cache"    // owner force-refresh adapter
)

// Owner labels — the service/lane that owns computing this surface's vector.
const (
	OwnerEnrichment  = "enrichment"
	OwnerMedia       = "media"
	OwnerNews        = "news"
	OwnerPreferences = "preferences"
	OwnerDiscovery   = "discovery"
)

// EmbeddingSurface declares one stored-vector surface.
type EmbeddingSurface struct {
	Key   string // stable ID, e.g. "content_text"
	Label string // human name for the cockpit
	Space string // EmbeddingSpaceText | EmbeddingSpaceImage

	Table         string // physical table
	VecCol        string // vector column
	ModelCol      string // display model-label column
	SpaceIDCol    string // char(64) space identity column
	ProducerIDCol string // char(64) producer identity column
	TenantCol     string // tenant scoping column (all six surfaces have one)
	IDCol         string // stable target identity column (public_id except proposal id)

	Dim   int    // schema dimension, cross-checked against service-reported dim
	Kind  string // SurfaceKind*
	Owner string // Owner*

	// Recipe is the producer-recipe version constant this surface's writer uses.
	// Combined with the space's space_id it yields the expected producer_id.
	Recipe string

	// ConsumerKeys names the similarity paths that read this surface. Every key
	// must have a comparability test proving it rejects unknown/other space_id.
	// A newly-added `<=>`/cosine consumer not listed here fails the static test
	// in embeddingConsumerRegistry_test.go.
	ConsumerKeys []string
}

// embeddingSurfaceRegistry is the single source of truth. Order is stable so the
// cockpit and inventory render deterministically.
var embeddingSurfaceRegistry = []EmbeddingSurface{
	{
		Key: "content_text", Label: "Content text embeddings", Space: EmbeddingSpaceText,
		Table: "content_items", VecCol: "embedding",
		ModelCol: "embedding_model", SpaceIDCol: "embedding_space_id", ProducerIDCol: "embedding_producer_id",
		TenantCol: "tenant_id", Dim: 1024, Kind: SurfaceKindItem, Owner: OwnerEnrichment,
		IDCol:        "public_id",
		Recipe:       spaceid.RecipeContentText,
		ConsumerKeys: []string{"knn_dense", "related_dense", "story_classify", "discovery_dense"},
	},
	{
		Key: "content_image", Label: "Content image embeddings", Space: EmbeddingSpaceImage,
		Table: "content_items", VecCol: "image_embedding",
		ModelCol: "image_embedding_model", SpaceIDCol: "image_embedding_space_id", ProducerIDCol: "image_embedding_producer_id",
		TenantCol: "tenant_id", Dim: 512, Kind: SurfaceKindItem, Owner: OwnerMedia,
		IDCol:        "public_id",
		Recipe:       spaceid.RecipeContentImage,
		ConsumerKeys: []string{}, // no live image-similarity consumer yet
	},
	{
		Key: "story_centroid", Label: "Story centroids", Space: EmbeddingSpaceText,
		Table: "stories", VecCol: "embedding",
		ModelCol: "embedding_model", SpaceIDCol: "embedding_space_id", ProducerIDCol: "embedding_producer_id",
		TenantCol: "tenant_id", Dim: 1024, Kind: SurfaceKindCentroid, Owner: OwnerNews,
		IDCol:        "public_id",
		Recipe:       spaceid.RecipeStoryCentroid,
		ConsumerKeys: []string{"story_classify", "story_related"},
	},
	{
		Key: "topic_centroid", Label: "Topic centroids", Space: EmbeddingSpaceText,
		Table: "topics", VecCol: "centroid",
		ModelCol: "centroid_model", SpaceIDCol: "centroid_space_id", ProducerIDCol: "centroid_producer_id",
		TenantCol: "tenant_id", Dim: 1024, Kind: SurfaceKindCentroid, Owner: OwnerPreferences,
		IDCol:        "public_id",
		Recipe:       spaceid.RecipeTopicCentroid,
		ConsumerKeys: []string{"topic_map", "topic_affinity"},
	},
	{
		Key: "topic_proposal", Label: "Topic proposal seeds", Space: EmbeddingSpaceText,
		Table: "topic_proposals", VecCol: "embedding",
		ModelCol: "embedding_model", SpaceIDCol: "embedding_space_id", ProducerIDCol: "embedding_producer_id",
		TenantCol: "tenant_id", Dim: 1024, Kind: SurfaceKindCache, Owner: OwnerPreferences,
		IDCol:        "id",
		Recipe:       spaceid.RecipeTopicProposal,
		ConsumerKeys: []string{"proposal_dedup"},
	},
	{
		Key: "discovery_profile", Label: "Discovery profiles", Space: EmbeddingSpaceText,
		Table: "discovery_profiles", VecCol: "embedding",
		ModelCol: "embedding_model", SpaceIDCol: "embedding_space_id", ProducerIDCol: "embedding_producer_id",
		TenantCol: "tenant_id", Dim: 1024, Kind: SurfaceKindCache, Owner: OwnerDiscovery,
		IDCol:        "public_id",
		Recipe:       spaceid.RecipeDiscoveryProfile,
		ConsumerKeys: []string{"discovery_dense"},
	},
}

// EmbeddingSurfaces returns the registry (read-only copy of the slice header).
func EmbeddingSurfaces() []EmbeddingSurface { return embeddingSurfaceRegistry }

// EmbeddingSurfaceByKey looks up one surface; ok=false when unknown.
func EmbeddingSurfaceByKey(key string) (EmbeddingSurface, bool) {
	for _, s := range embeddingSurfaceRegistry {
		if s.Key == key {
			return s, true
		}
	}
	return EmbeddingSurface{}, false
}

// registeredConsumerKeys is the flat set of similarity-consumer keys the
// registry declares. The static test in embeddingConsumerRegistry_test.go
// asserts every code path that runs a `<=>`/cosine comparison maps to one of
// these — a new unregistered consumer must fail the build's test stage rather
// than silently comparing across spaces.
func registeredConsumerKeys() map[string]bool {
	set := map[string]bool{}
	for _, s := range embeddingSurfaceRegistry {
		for _, ck := range s.ConsumerKeys {
			set[ck] = true
		}
	}
	return set
}
