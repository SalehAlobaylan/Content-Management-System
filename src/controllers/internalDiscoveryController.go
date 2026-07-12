package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/spaceid"
	"content-management-system/src/utils"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/pgvector/pgvector-go"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Scoring thresholds (min-confidence floor + novelty dup-threshold/penalty) come
// from the tenant's discovery_config (admin-tunable via the settings page).
// loadDiscoveryConfig falls back to models.DefaultDiscoveryConfig when no row
// exists, so behaviour is unchanged out of the box.

type suggestionCandidate struct {
	Name          string                   `json:"name"`
	Type          string                   `json:"type"`
	FeedURL       string                   `json:"feed_url"`
	SiteURL       *string                  `json:"site_url"`
	ImageURL      *string                  `json:"image_url"`
	Language      *string                  `json:"language"`
	CanonicalKey  string                   `json:"canonical_key"`
	Confidence    float64                  `json:"confidence"`
	Health        map[string]interface{}   `json:"health"`
	SampleItems   []map[string]interface{} `json:"sample_items"`
	DiscoveredVia string                   `json:"discovered_via"`
}

type internalSourceSuggestionsRequest struct {
	TenantID   string                `json:"tenant_id"`
	ProfileID  string                `json:"profile_id"` // discovery profile public id (optional)
	Candidates []suggestionCandidate `json:"candidates"`
}

// InternalCreateSourceSuggestions handles POST /internal/source-suggestions —
// Aggregation posts discovered candidates here. Bulk upsert on
// (tenant_id, canonical_key); skips weak candidates and ones that already exist
// as a registered source.
func InternalCreateSourceSuggestions(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	var req internalSourceSuggestionsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	tenantID := strings.TrimSpace(req.TenantID)
	if tenantID == "" {
		tenantID = "default"
	}

	cfg := loadDiscoveryConfig(db, tenantID)

	// Resolve the discovery profile (optional).
	var profileID *uint
	if pid, found := resolveProfileInternalID(db, tenantID, req.ProfileID); found {
		profileID = pid
	}

	// Existing registered feed URLs — skip candidates that are already sources.
	var existingFeeds []string
	db.Model(&models.ContentSource{}).
		Where("tenant_id = ? AND feed_url IS NOT NULL AND feed_url != ''", tenantID).
		Pluck("feed_url", &existingFeeds)
	feedSet := make(map[string]struct{}, len(existingFeeds))
	for _, f := range existingFeeds {
		feedSet[f] = struct{}{}
	}

	rows := make([]models.SourceSuggestion, 0, len(req.Candidates))
	skipped := 0
	for _, cand := range req.Candidates {
		canonical := strings.TrimSpace(cand.CanonicalKey)
		feedURL := strings.TrimSpace(cand.FeedURL)
		if canonical == "" || feedURL == "" || cand.Confidence < cfg.MinConfidence {
			skipped++
			continue
		}
		if _, exists := feedSet[feedURL]; exists {
			skipped++
			continue
		}

		sourceType := strings.ToUpper(strings.TrimSpace(cand.Type))
		if sourceType == "" {
			sourceType = string(models.SourceTypeRSS)
		}

		row := models.SourceSuggestion{
			TenantID:      tenantID,
			ProfileID:     profileID,
			Name:          strings.TrimSpace(cand.Name),
			Type:          models.SourceType(sourceType),
			FeedURL:       feedURL,
			SiteURL:       cand.SiteURL,
			ImageURL:      cand.ImageURL,
			Language:      cand.Language,
			CanonicalKey:  canonical,
			Confidence:    cand.Confidence,
			DiscoveredVia: strings.TrimSpace(cand.DiscoveredVia),
			Category:      models.DefaultCategoryForType(models.SourceType(sourceType)),
			Status:        models.SuggestionStatusPending,
		}
		if cand.Health != nil {
			if raw, err := json.Marshal(cand.Health); err == nil {
				row.Health = datatypes.JSON(raw)
			}
		}
		if cand.SampleItems != nil {
			if raw, err := json.Marshal(cand.SampleItems); err == nil {
				row.SampleItems = datatypes.JSON(raw)
			}
		}
		if row.Name == "" {
			row.Name = canonical
		}
		rows = append(rows, row)
	}

	// Best-effort semantic relevance + novelty scoring (no-op if Enrichment is
	// down or there's no profile — rows still insert with a null score).
	scoreSuggestionRows(db, tenantID, profileID, rows, cfg.DupThreshold, cfg.DupPenalty)

	if len(rows) > 0 {
		// Upsert on (tenant_id, canonical_key). Refresh candidate data but do NOT
		// touch status — so REJECTED/APPROVED rows stay out of the pending queue.
		if err := db.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "tenant_id"}, {Name: "profile_id"}, {Name: "canonical_key"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"name", "type", "feed_url", "site_url", "image_url", "language",
				"confidence", "relevance_score", "health", "sample_items", "discovered_via", "category", "updated_at",
			}),
		}).Create(&rows).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to upsert suggestions"})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{"upserted": len(rows), "skipped": skipped})
}

// InternalGetDiscoveryConfig handles GET /internal/discovery/config — the
// Aggregation sweeper reads interval/automation/provider/recency/max from here.
func InternalGetDiscoveryConfig(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	tenantID := strings.TrimSpace(c.Query("tenant_id"))
	if tenantID == "" {
		tenantID = "default"
	}
	c.JSON(http.StatusOK, loadDiscoveryConfig(db, tenantID))
}

// InternalListEnabledProfiles handles GET /internal/discovery/profiles?enabled=true
// — the sweep's fan-out list.
func InternalListEnabledProfiles(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	tenantID := strings.TrimSpace(c.Query("tenant_id"))
	if tenantID == "" {
		tenantID = "default"
	}
	var profiles []models.DiscoveryProfile
	if err := db.Where("tenant_id = ? AND enabled = ?", tenantID, true).Find(&profiles).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list profiles"})
		return
	}
	data := make([]discoveryProfileResponse, 0, len(profiles))
	for _, p := range profiles {
		data = append(data, mapDiscoveryProfileResponse(p))
	}
	c.JSON(http.StatusOK, gin.H{"data": data, "tenant_id": tenantID})
}

// scoreSuggestionRows fills relevance_score on each row: cosine(profile, sample)
// adjusted by a novelty penalty for near-duplicate (mirror) content. Best-effort
// — leaves scores null on any failure so the loop never breaks.
func scoreSuggestionRows(db *gorm.DB, tenantID string, profileID *uint, rows []models.SourceSuggestion, dupThreshold, dupPenalty float64) {
	if profileID == nil || len(rows) == 0 {
		return
	}
	var profile models.DiscoveryProfile
	if err := db.Where("id = ?", *profileID).First(&profile).Error; err != nil {
		return
	}
	profileVec, ok := ensureProfileEmbedding(db, &profile)
	if !ok {
		return
	}
	for i := range rows {
		titles := suggestionSampleTitleList(rows[i].SampleItems)
		if len(titles) == 0 {
			continue
		}
		vecs, sampleSpaceID, err := embedBatchViaEnrichmentWithSpace(titles)
		if err != nil || len(vecs) == 0 || profile.EmbeddingSpaceID == nil || sampleSpaceID != *profile.EmbeddingSpaceID {
			continue
		}
		// Mean of the per-item cosines vs the profile. Averaging over ALL recent
		// items (not the top few) rewards FOCUSED feeds: a sports-only feed
		// averages high for a sports profile but low for economy, while a broad
		// general feed stays middling everywhere. Top-K would wrongly let any
		// feed cherry-pick its few least-off-topic items.
		var sum float64
		n := 0
		for _, v := range vecs {
			if len(v) == len(profileVec) {
				sum += clamp01(dotProduct(profileVec, v))
				n++
			}
		}
		if n == 0 {
			continue
		}
		relevance := sum / float64(n)
		score := relevance * noveltyFactor(db, tenantID, meanVector(vecs), dupThreshold, dupPenalty)
		s := score
		rows[i].RelevanceScore = &s
	}
}

// meanVector returns the element-wise mean of equal-length vectors.
func meanVector(vecs [][]float32) []float32 {
	if len(vecs) == 0 {
		return nil
	}
	dim := len(vecs[0])
	out := make([]float32, dim)
	n := 0
	for _, v := range vecs {
		if len(v) != dim {
			continue
		}
		for d := 0; d < dim; d++ {
			out[d] += v[d]
		}
		n++
	}
	if n == 0 {
		return nil
	}
	for d := 0; d < dim; d++ {
		out[d] /= float32(n)
	}
	return out
}

// ensureProfileEmbedding returns the profile's cached embedding, computing and
// persisting it on first use. Returns (vec, true) on success.
func ensureProfileEmbedding(db *gorm.DB, profile *models.DiscoveryProfile) ([]float32, bool) {
	expectedProducer := ""
	if _, _, producer := textSurfaceStamp(spaceid.RecipeDiscoveryProfile); producer != nil {
		expectedProducer = *producer
	}
	if profile.Embedding != nil && expectedProducer != "" && profile.EmbeddingProducerID != nil && *profile.EmbeddingProducerID == expectedProducer {
		if s := profile.Embedding.Slice(); len(s) > 0 {
			return s, true
		}
	}
	text := strings.TrimSpace(profile.Name + ". " + profile.Description + ". " + strings.Join([]string(profile.Keywords), ", "))
	if text == "" {
		return nil, false
	}
	vec, observedSpace, err := embedQueryViaEnrichmentWithSpace(text)
	if err != nil || len(vec) == 0 {
		return nil, false
	}
	pv := pgvector.NewVector(vec)
	profile.Embedding = &pv
	upd := map[string]interface{}{"embedding": &pv}
	// Stamp vector-space provenance (stage 10); NULL when text space unresolved.
	if model, producer, ok := textStampForObservedSpace(spaceid.RecipeDiscoveryProfile, observedSpace); ok {
		upd["embedding_model"], upd["embedding_space_id"], upd["embedding_producer_id"] = model, observedSpace, producer
		profile.EmbeddingModel, profile.EmbeddingSpaceID, profile.EmbeddingProducerID = &model, &observedSpace, &producer
	}
	_ = db.Model(&models.DiscoveryProfile{}).Where("id = ?", profile.ID).Updates(upd).Error
	return vec, true
}

// noveltyFactor returns dupPenalty when the candidate sample is near-identical
// to existing ingested content (a mirror/aggregator), else 1.0.
func noveltyFactor(db *gorm.DB, tenantID string, vec []float32, dupThreshold, dupPenalty float64) float64 {
	spaceID := currentTextSpaceIDForSimilarity()
	if spaceID == "" {
		return 1.0
	}
	lit := utils.PgvectorToLiteral(vec)
	var row struct{ Sim float64 }
	err := db.Model(&models.ContentItem{}).
		Where("tenant_id = ? AND embedding IS NOT NULL AND embedding_space_id = ?", tenantID, spaceID).
		Select("1 - (embedding <=> '" + lit + "') AS sim").
		Order("embedding <=> '" + lit + "'").
		Limit(1).Scan(&row).Error
	if err != nil {
		return 1.0
	}
	if row.Sim >= dupThreshold {
		return dupPenalty
	}
	return 1.0
}

func suggestionSampleTitleList(raw datatypes.JSON) []string {
	if len(raw) == 0 {
		return nil
	}
	var items []struct {
		Title string `json:"title"`
	}
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, it := range items {
		if t := strings.TrimSpace(it.Title); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func dotProduct(a, b []float32) float64 {
	if len(a) != len(b) {
		return 0
	}
	var s float64
	for i := range a {
		s += float64(a[i]) * float64(b[i])
	}
	return s
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}
