package spaceid

// Producer-recipe version constants — one per embedding surface. These are the
// golden strings hashed into producer_id. They MUST match the recipe strings the
// owning writer uses when it stamps a vector (Enrichment for text/content,
// Media for image, and the CMS-side owner adapters for centroids/caches), or a
// freshly-written vector would report a producer_id CMS never expects and the
// inventory lane would mark healthy rows stale.
//
// Rule: bump the version suffix whenever a surface's canonical INPUT builder or
// preprocessing changes, even if the model weights are identical. A recipe
// change is a new producer (must recompute) while space_id stays equal
// (still comparable).
const (
	// RecipeContentText — content_items.embedding. Input = title + excerpt + body
	// assembled by Enrichment's canonical builder.
	RecipeContentText = "content-title-excerpt-body:v1"
	// RecipeContentImage — content_items.image_embedding. CLIP over hero image /
	// video thumbnail, Media preprocessing.
	RecipeContentImage = "content-hero-thumbnail:v1"
	// RecipeStoryCentroid — stories.embedding. Running mean of member text vectors.
	RecipeStoryCentroid = "story-member-mean:v1"
	// RecipeTopicCentroid — topics.centroid. Embedding of the approved bilingual
	// (AR+EN) label seed.
	RecipeTopicCentroid = "topic-bilingual-label:v1"
	// RecipeTopicProposal — topic_proposals.embedding. Embedding of the proposal
	// suggested-label seed.
	RecipeTopicProposal = "topic-proposal-seed:v1"
	// RecipeDiscoveryProfile — discovery_profiles.embedding. Embedding of the
	// canonical profile input text.
	RecipeDiscoveryProfile = "discovery-profile-seed:v1"
)
