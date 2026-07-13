package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/spaceid"
	"content-management-system/src/utils"
	"fmt"
	"hash/fnv"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/pgvector/pgvector-go"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	topicMappingThreshold          = 0.55
	topicMappingTopK               = 3
	topicMineMinMembers            = 5
	affinityK                      = 3.0
	maxPreferenceMutationBodyBytes = 16 * 1024
	maxDeclaredTopicCount          = 100
)

var slugCleaner = regexp.MustCompile(`[^\p{L}\p{N}]+`)

type topicSubject struct {
	ID           uuid.UUID `json:"id"`
	Slug         string    `json:"slug"`
	LabelAR      string    `json:"label_ar"`
	LabelEN      string    `json:"label_en"`
	CategorySlug string    `json:"category_slug,omitempty"`
	Score        float64   `json:"score,omitempty"`
	State        string    `json:"state,omitempty"`
}

type topicCatalogRequest struct {
	Slug         string `json:"slug"`
	LabelAR      string `json:"label_ar"`
	LabelEN      string `json:"label_en"`
	CategorySlug string `json:"category_slug"`
	Active       *bool  `json:"active"`
	Featured     *bool  `json:"featured"`
}

type topicProposalApproveRequest struct {
	Slug         string `json:"slug"`
	LabelAR      string `json:"label_ar"`
	LabelEN      string `json:"label_en"`
	CategorySlug string `json:"category_slug"`
	Featured     *bool  `json:"featured"`
}

type topicProposalMergeRequest struct {
	Into string `json:"into"`
}

type replaceDeclaredTopicsRequest struct {
	TopicIDs []string `json:"topic_ids"`
}

type preferenceSettingDomain struct {
	Min float64
	Max float64
}

// Preference controls are product policy, not deployment tuning. Zero disables
// a weight/discount/prior; half-life stays strictly positive so decay math is
// always defined. Keep this table aligned with the Console and migration.
var preferenceSettingDomains = map[string]preferenceSettingDomain{
	"w_foryou":             {Min: 0, Max: 1},
	"w_news":               {Min: 0, Max: 1},
	"weight_complete":      {Min: 0, Max: 5},
	"weight_bookmark":      {Min: 0, Max: 5},
	"weight_share":         {Min: 0, Max: 5},
	"weight_like":          {Min: 0, Max: 5},
	"weight_comment":       {Min: 0, Max: 5},
	"weight_view":          {Min: 0, Max: 5},
	"decay_half_life_days": {Min: 0.25, Max: 365},
	"declared_prior":       {Min: 0, Max: 5},
	"category_discount":    {Min: 0, Max: 1},
}

func defaultPreferenceSettings(tenantID string) models.PreferenceSettings {
	return models.PreferenceSettings{
		TenantID:          tenantID,
		WForYou:           0.30,
		WNews:             0.15,
		WeightComplete:    1.0,
		WeightBookmark:    0.9,
		WeightShare:       0.9,
		WeightLike:        0.7,
		WeightComment:     0.5,
		WeightView:        0.2,
		DecayHalfLifeDays: 30,
		DeclaredPrior:     3.0,
		CategoryDiscount:  0.5,
	}
}

func loadPreferenceSettings(db *gorm.DB, tenantID string) models.PreferenceSettings {
	cfg := defaultPreferenceSettings(tenantID)
	if err := db.Where("tenant_id = ?", tenantID).First(&cfg).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			_ = db.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "tenant_id"}},
				DoNothing: true,
			}).Create(&cfg).Error
		}
	}
	if !validPreferenceSettingValue("decay_half_life_days", cfg.DecayHalfLifeDays) {
		cfg.DecayHalfLifeDays = 30
	}
	if !validPreferenceSettingValue("declared_prior", cfg.DeclaredPrior) {
		cfg.DeclaredPrior = 3
	}
	if !validPreferenceSettingValue("category_discount", cfg.CategoryDiscount) {
		cfg.CategoryDiscount = 0.5
	}
	return cfg
}

func validPreferenceSettingValue(key string, value float64) bool {
	domain, ok := preferenceSettingDomains[key]
	return ok && !math.IsNaN(value) && !math.IsInf(value, 0) && value >= domain.Min && value <= domain.Max
}

// Per-tenant settings cache. The feed hot path reads settings on every
// authenticated request only to discover the flags are OFF; without a cache
// that is one DB round trip per feed request. TTL is short and writes
// invalidate immediately, so the kill switch still flips "instantly".
type cachedPrefSettings struct {
	cfg models.PreferenceSettings
	exp time.Time
}

const prefSettingsCacheTTL = 30 * time.Second

var (
	prefSettingsCache   = map[string]cachedPrefSettings{}
	prefSettingsCacheMu sync.RWMutex
)

// loadPreferenceSettingsCached is the feed-path loader — cheap and DB-free on the
// common (disabled/unchanged) case. Admin read/write paths use the uncached
// loadPreferenceSettings so they always see fresh values.
func loadPreferenceSettingsCached(db *gorm.DB, tenantID string) models.PreferenceSettings {
	prefSettingsCacheMu.RLock()
	c, ok := prefSettingsCache[tenantID]
	prefSettingsCacheMu.RUnlock()
	if ok && time.Now().Before(c.exp) {
		return c.cfg
	}
	cfg := loadPreferenceSettings(db, tenantID)
	prefSettingsCacheMu.Lock()
	prefSettingsCache[tenantID] = cachedPrefSettings{cfg: cfg, exp: time.Now().Add(prefSettingsCacheTTL)}
	prefSettingsCacheMu.Unlock()
	return cfg
}

func invalidatePrefSettingsCache(tenantID string) {
	prefSettingsCacheMu.Lock()
	delete(prefSettingsCache, tenantID)
	prefSettingsCacheMu.Unlock()
}

func defaultTopicCategories() []models.TopicCategory {
	return []models.TopicCategory{
		{Slug: "politics", LabelAR: "سياسة", LabelEN: "Politics", SortOrder: 10, Active: true},
		{Slug: "economy", LabelAR: "اقتصاد", LabelEN: "Economy", SortOrder: 20, Active: true},
		{Slug: "sports", LabelAR: "رياضة", LabelEN: "Sports", SortOrder: 30, Active: true},
		{Slug: "technology", LabelAR: "تقنية", LabelEN: "Technology", SortOrder: 40, Active: true},
		{Slug: "culture", LabelAR: "ثقافة", LabelEN: "Culture", SortOrder: 50, Active: true},
		{Slug: "society", LabelAR: "مجتمع", LabelEN: "Society", SortOrder: 60, Active: true},
		{Slug: "religion", LabelAR: "دين", LabelEN: "Religion", SortOrder: 70, Active: true},
		{Slug: "health", LabelAR: "صحة", LabelEN: "Health", SortOrder: 80, Active: true},
		{Slug: "science", LabelAR: "علوم", LabelEN: "Science", SortOrder: 90, Active: true},
		{Slug: "environment", LabelAR: "بيئة", LabelEN: "Environment", SortOrder: 100, Active: true},
		{Slug: "general", LabelAR: "عام", LabelEN: "General", SortOrder: 999, Active: true},
	}
}

func ensureDefaultTopicCategories(db *gorm.DB, tenantID string) {
	for _, cat := range defaultTopicCategories() {
		cat.TenantID = tenantID
		_ = db.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "tenant_id"}, {Name: "slug"}},
			DoNothing: true,
		}).Create(&cat).Error
	}
}

func slugifyTopic(s string) string {
	original := strings.TrimSpace(s)
	slug := normalizedTopicSlug(original)
	if slug != "" {
		return slug
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(original))
	return fmt.Sprintf("topic-%08x", h.Sum32())
}

func normalizedTopicSlug(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, "_", "-")
	s = slugCleaner.ReplaceAllString(s, "-")
	return strings.Trim(s, "-")
}

func canonicalTopicLabels(slug, ar, en string) (string, string, string) {
	slug = slugifyTopic(topicFirstNonEmpty(slug, en, ar))
	if en = strings.TrimSpace(en); en == "" {
		en = strings.ReplaceAll(slug, "-", " ")
	}
	if ar = strings.TrimSpace(ar); ar == "" {
		ar = en
	}
	return slug, ar, en
}

func topicFirstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func cosine(a, b []float32) float64 {
	if len(a) == 0 || len(a) != len(b) {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		av, bv := float64(a[i]), float64(b[i])
		dot += av * bv
		na += av * av
		nb += bv * bv
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

func interactionWeight(t models.InteractionType, cfg models.PreferenceSettings) float64 {
	switch t {
	case models.InteractionTypeComplete:
		return cfg.WeightComplete
	case models.InteractionTypeBookmark:
		return cfg.WeightBookmark
	case models.InteractionTypeShare:
		return cfg.WeightShare
	case models.InteractionTypeLike:
		return cfg.WeightLike
	case models.InteractionTypeComment:
		return cfg.WeightComment
	case models.InteractionTypeView:
		return cfg.WeightView
	default:
		return 0
	}
}

func decayMultiplier(ts time.Time, cfg models.PreferenceSettings) float64 {
	ageDays := time.Since(ts).Hours() / 24
	if ageDays < 0 {
		ageDays = 0
	}
	return math.Pow(0.5, ageDays/cfg.DecayHalfLifeDays)
}

// AdminListTopicCatalog handles GET /admin/topics/catalog.
func AdminListTopicCatalog(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	ensureDefaultTopicCategories(db, principal.TenantID)

	q := db.Where("tenant_id = ?", principal.TenantID)
	if v := strings.TrimSpace(c.Query("category")); v != "" {
		q = q.Where("category_slug = ?", v)
	}
	if v := strings.TrimSpace(c.Query("q")); v != "" {
		q = q.Where("slug ILIKE ? OR label_ar ILIKE ? OR label_en ILIKE ?", "%"+v+"%", "%"+v+"%", "%"+v+"%")
	}
	if v := strings.TrimSpace(c.Query("active")); v != "" {
		q = q.Where("active = ?", strings.EqualFold(v, "true") || v == "1")
	}
	if v := strings.TrimSpace(c.Query("featured")); v != "" {
		q = q.Where("featured = ?", strings.EqualFold(v, "true") || v == "1")
	}

	var topics []models.Topic
	if err := q.Order("featured DESC, category_slug ASC, slug ASC").Find(&topics).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to list topics", Code: "TOPICS_LIST_FAILED"})
		return
	}
	var categories []models.TopicCategory
	db.Where("tenant_id = ?", principal.TenantID).Order("sort_order ASC, slug ASC").Find(&categories)
	c.JSON(http.StatusOK, gin.H{"data": topics, "categories": categories})
}

// AdminCreateTopic handles POST /admin/topics/catalog.
func AdminCreateTopic(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var req topicCatalogRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request", Code: "INVALID_REQUEST"})
		return
	}
	slug, ar, en := canonicalTopicLabels(req.Slug, req.LabelAR, req.LabelEN)
	active, featured := true, false
	if req.Active != nil {
		active = *req.Active
	}
	if req.Featured != nil {
		featured = *req.Featured
	}
	topic := models.Topic{
		TenantID: principal.TenantID, Slug: slug, LabelAR: ar, LabelEN: en,
		CategorySlug: strings.TrimSpace(req.CategorySlug), Active: active, Featured: featured, CreatedFrom: "manual",
		NeedsRemap: true,
	}
	if emb, observedSpace, err := embedQueryViaEnrichmentWithSpace(en + " " + ar); err == nil && len(emb) == 1024 {
		vec := pgvector.NewVector(emb)
		topic.Centroid = &vec
		if model, producer, ok := textStampForObservedSpace(spaceid.RecipeTopicCentroid, observedSpace); ok {
			topic.CentroidModel, topic.CentroidSpaceID, topic.CentroidProducerID = &model, &observedSpace, &producer
		}
	}
	if err := db.Create(&topic).Error; err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Failed to create topic: " + err.Error(), Code: "CREATE_FAILED"})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"data": topic})
}

// AdminUpdateTopic handles PUT /admin/topics/catalog/:id.
func AdminUpdateTopic(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid topic id", Code: "INVALID_ID"})
		return
	}
	var req topicCatalogRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request", Code: "INVALID_REQUEST"})
		return
	}
	updates := map[string]interface{}{}
	if strings.TrimSpace(req.Slug) != "" {
		updates["slug"] = slugifyTopic(req.Slug)
	}
	if strings.TrimSpace(req.LabelAR) != "" {
		updates["label_ar"] = strings.TrimSpace(req.LabelAR)
	}
	if strings.TrimSpace(req.LabelEN) != "" {
		updates["label_en"] = strings.TrimSpace(req.LabelEN)
	}
	if req.CategorySlug != "" {
		updates["category_slug"] = strings.TrimSpace(req.CategorySlug)
	}
	if req.Active != nil {
		updates["active"] = *req.Active
	}
	if req.Featured != nil {
		updates["featured"] = *req.Featured
	}
	if _, changesMapping := updates["slug"]; changesMapping {
		updates["needs_remap"] = true
	}
	if _, changesMapping := updates["label_ar"]; changesMapping {
		updates["needs_remap"] = true
	}
	if _, changesMapping := updates["label_en"]; changesMapping {
		updates["needs_remap"] = true
	}
	if _, changesMapping := updates["category_slug"]; changesMapping {
		updates["needs_remap"] = true
	}
	if _, changesMapping := updates["active"]; changesMapping {
		updates["needs_remap"] = true
	}
	if err := db.Model(&models.Topic{}).Where("tenant_id = ? AND public_id = ?", principal.TenantID, id).Updates(updates).Error; err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Failed to update topic: " + err.Error(), Code: "UPDATE_FAILED"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "Topic updated"})
}

// AdminListTopicProposals handles GET /admin/topics/proposals.
func AdminListTopicProposals(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	status := strings.TrimSpace(c.DefaultQuery("status", "pending"))
	var proposals []models.TopicProposal
	q := db.Where("tenant_id = ?", principal.TenantID)
	if status != "" && status != "all" {
		q = q.Where("status = ?", status)
	}
	if err := q.Order("created_at DESC").Find(&proposals).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to list proposals", Code: "PROPOSALS_FAILED"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": proposals})
}

func topicProposalByID(db *gorm.DB, tenant string, id string) (models.TopicProposal, bool, error) {
	var p models.TopicProposal
	if err := db.Where("tenant_id = ? AND id = ?", tenant, id).First(&p).Error; err != nil {
		return p, false, err
	}
	return p, true, nil
}

// approveTopicProposalCore performs the shared approve transaction: upsert the
// topic on (tenant, slug), mark the proposal approved, and reload by slug so
// PublicID is populated even when the upsert resolved to an existing row. Both
// the human handler and the autopilot's earned auto-approve tier call this —
// keeping ONE canonical write path. The bounded dirty sweep performs the remap
// (NeedsRemap must be true on the passed topic).
func approveTopicProposalCore(db *gorm.DB, tenantID string, p models.TopicProposal, topic models.Topic, resolvedBy string) (models.Topic, error) {
	now := time.Now()
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "tenant_id"}, {Name: "slug"}},
			DoUpdates: clause.AssignmentColumns([]string{"label_ar", "label_en", "category_slug", "active", "featured", "needs_remap"}),
		}).Create(&topic).Error; err != nil {
			return err
		}
		return tx.Model(&models.TopicProposal{}).Where("id = ?", p.ID).Updates(map[string]interface{}{
			"status": "approved", "resolved_by": resolvedBy, "resolved_at": now,
		}).Error
	})
	if err != nil {
		return topic, err
	}
	var saved models.Topic
	if err := db.Where("tenant_id = ? AND slug = ?", tenantID, topic.Slug).First(&saved).Error; err == nil {
		return saved, nil
	}
	return topic, nil
}

// AdminApproveTopicProposal handles POST /admin/topics/proposals/:id/approve.
func AdminApproveTopicProposal(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	p, _, err := topicProposalByID(db, principal.TenantID, c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Proposal not found", Code: "NOT_FOUND"})
		return
	}
	var req topicProposalApproveRequest
	_ = c.ShouldBindJSON(&req)
	slug, ar, en := canonicalTopicLabels(topicFirstNonEmpty(req.Slug, p.SuggestedSlug), topicFirstNonEmpty(req.LabelAR, p.SuggestedLabelAR), topicFirstNonEmpty(req.LabelEN, p.SuggestedLabelEN))
	featured := true
	if req.Featured != nil {
		featured = *req.Featured
	}
	topic := models.Topic{TenantID: principal.TenantID, Slug: slug, LabelAR: ar, LabelEN: en, CategorySlug: topicFirstNonEmpty(req.CategorySlug, p.SuggestedCategory), Featured: featured, Active: true, CreatedFrom: "mined", NeedsRemap: true}
	if emb, observedSpace, err := embedQueryViaEnrichmentWithSpace(en + " " + ar); err == nil && len(emb) == 1024 {
		vec := pgvector.NewVector(emb)
		topic.Centroid = &vec
		if model, producer, ok := textStampForObservedSpace(spaceid.RecipeTopicCentroid, observedSpace); ok {
			topic.CentroidModel, topic.CentroidSpaceID, topic.CentroidProducerID = &model, &observedSpace, &producer
		}
	}
	topic, err = approveTopicProposalCore(db, principal.TenantID, p, topic, principal.UserID)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Approval failed: " + err.Error(), Code: "APPROVE_FAILED"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "Proposal approved", "data": topic})
}

// AdminRejectTopicProposal handles POST /admin/topics/proposals/:id/reject.
func AdminRejectTopicProposal(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	now := time.Now()
	if err := db.Model(&models.TopicProposal{}).
		Where("tenant_id = ? AND id = ?", principal.TenantID, c.Param("id")).
		Updates(map[string]interface{}{"status": "rejected", "resolved_by": principal.UserID, "resolved_at": now}).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Reject failed", Code: "REJECT_FAILED"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "Proposal rejected"})
}

// AdminMergeTopicProposal handles POST /admin/topics/proposals/:id/merge.
func AdminMergeTopicProposal(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var req topicProposalMergeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request", Code: "INVALID_REQUEST"})
		return
	}
	into, err := uuid.Parse(req.Into)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid target topic", Code: "INVALID_ID"})
		return
	}
	var target models.Topic
	if err := db.Where("tenant_id = ? AND public_id = ? AND active = ?", principal.TenantID, into, true).First(&target).Error; err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Target topic is not available in this tenant", Code: "INVALID_TARGET"})
		return
	}
	now := time.Now()
	if err := db.Model(&models.TopicProposal{}).
		Where("tenant_id = ? AND id = ?", principal.TenantID, c.Param("id")).
		Updates(map[string]interface{}{"status": "merged", "merged_into": into, "resolved_by": principal.UserID, "resolved_at": now}).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Merge failed", Code: "MERGE_FAILED"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "Proposal merged"})
}

// AdminMineTopics handles POST /admin/topics/mine.
func AdminMineTopics(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	created, err := mineTopicProposals(db, principal.TenantID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Mining failed: " + err.Error(), Code: "MINE_FAILED"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"created": created})
}

// mineTopicProposals + the capped variant now live in
// preferenceMappingMaintenance.go so the Autopilot can share the new-proposal cap
// and richer demand/sample evidence.

// AdminRemapTopics handles POST /admin/topics/remap.
func AdminRemapTopics(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	items, stories, err := remapCatalogTopics(db, principal.TenantID, true)
	if err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Remap failed: " + err.Error(), Code: "REMAP_FAILED"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"mapped_items": items, "mapped_stories": stories})
}

type topicVector struct {
	ID           uuid.UUID
	CategorySlug string
	Vec          []float32
}

func activeTopicVectors(db *gorm.DB, tenantID string) ([]topicVector, error) {
	spaceID := currentTextSpaceIDForSimilarity()
	if spaceID == "" {
		return nil, nil
	}
	var topics []models.Topic
	if err := db.Where("tenant_id = ? AND active = ? AND centroid IS NOT NULL AND centroid_space_id = ?", tenantID, true, spaceID).Find(&topics).Error; err != nil {
		return nil, err
	}
	out := make([]topicVector, 0, len(topics))
	for _, t := range topics {
		if t.Centroid == nil {
			continue
		}
		out = append(out, topicVector{ID: t.PublicID, CategorySlug: t.CategorySlug, Vec: t.Centroid.Slice()})
	}
	return out, nil
}

func topTopicMatches(vec []float32, topics []topicVector) []models.ContentItemTopic {
	type match struct {
		id    uuid.UUID
		score float64
	}
	matches := make([]match, 0, topicMappingTopK)
	for _, t := range topics {
		score := cosine(vec, t.Vec)
		if score < topicMappingThreshold {
			continue
		}
		matches = append(matches, match{id: t.ID, score: score})
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i].score > matches[j].score })
	if len(matches) > topicMappingTopK {
		matches = matches[:topicMappingTopK]
	}
	out := make([]models.ContentItemTopic, 0, len(matches))
	for _, m := range matches {
		out = append(out, models.ContentItemTopic{TopicID: m.id, Score: m.score})
	}
	return out
}

// remapCatalogBatchSize bounds how many rows we pull into Go per page. Paging by
// ascending primary key means a full remap covers the WHOLE corpus, not just the
// first page (the old Limit(1000) silently stranded everything past 1000 after a
// full-mode DELETE).
const remapCatalogBatchSize = 500

func remapCatalogTopics(db *gorm.DB, tenantID string, full bool) (int, int, error) {
	topics, err := activeTopicVectors(db, tenantID)
	if err != nil || len(topics) == 0 {
		return 0, 0, err
	}
	if full {
		db.Exec("DELETE FROM content_item_topics WHERE content_item_id IN (SELECT public_id FROM content_items WHERE tenant_id = ?)", tenantID)
		db.Exec("DELETE FROM story_topics WHERE story_id IN (SELECT public_id FROM stories WHERE tenant_id = ?)", tenantID)
	}

	mappedItems := 0
	var lastItemID uint
	for {
		var items []models.ContentItem
		q := db.Select("id, public_id, embedding").
			Where("tenant_id = ? AND status = ? AND embedding IS NOT NULL AND embedding_space_id = ? AND id > ?", tenantID, models.ContentStatusReady, currentTextSpaceIDForSimilarity(), lastItemID)
		if !full {
			q = q.Where("NOT EXISTS (SELECT 1 FROM content_item_topics cit WHERE cit.content_item_id = content_items.public_id)")
		}
		if err := q.Order("id ASC").Limit(remapCatalogBatchSize).Find(&items).Error; err != nil {
			return mappedItems, 0, err
		}
		if len(items) == 0 {
			break
		}
		for _, item := range items {
			lastItemID = item.ID
			if item.Embedding == nil {
				continue
			}
			matches := topTopicMatches(item.Embedding.Slice(), topics)
			if len(matches) == 0 {
				continue
			}
			db.Where("content_item_id = ?", item.PublicID).Delete(&models.ContentItemTopic{})
			for i := range matches {
				matches[i].ContentItemID = item.PublicID
			}
			if err := db.Create(&matches).Error; err == nil {
				mappedItems++
			}
		}
		if len(items) < remapCatalogBatchSize {
			break
		}
	}

	mappedStories := 0
	var lastStoryID uint
	for {
		var stories []models.Story
		q := db.Select("id, public_id, embedding").
			Where("tenant_id = ? AND embedding IS NOT NULL AND embedding_space_id = ? AND id > ?", tenantID, currentTextSpaceIDForSimilarity(), lastStoryID)
		if !full {
			q = q.Where("NOT EXISTS (SELECT 1 FROM story_topics st WHERE st.story_id = stories.public_id)")
		}
		if err := q.Order("id ASC").Limit(remapCatalogBatchSize).Find(&stories).Error; err != nil {
			return mappedItems, mappedStories, err
		}
		if len(stories) == 0 {
			break
		}
		for _, story := range stories {
			lastStoryID = story.ID
			if story.Embedding == nil {
				continue
			}
			matches := topTopicMatches(story.Embedding.Slice(), topics)
			if len(matches) == 0 {
				continue
			}
			db.Where("story_id = ?", story.PublicID).Delete(&models.StoryTopic{})
			rows := make([]models.StoryTopic, 0, len(matches))
			for _, m := range matches {
				rows = append(rows, models.StoryTopic{StoryID: story.PublicID, TopicID: m.TopicID, Score: m.Score})
			}
			if err := db.Create(&rows).Error; err == nil {
				mappedStories++
			}
		}
		if len(stories) < remapCatalogBatchSize {
			break
		}
	}
	if err := refreshTopicMemberCounts(db, tenantID); err != nil {
		return mappedItems, mappedStories, err
	}
	return mappedItems, mappedStories, nil
}

func refreshTopicMemberCounts(db *gorm.DB, tenantID string) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(`
		UPDATE topics t
		SET member_count = COALESCE(counts.member_count, 0)
		FROM (
			SELECT cit.topic_id, COUNT(*)::integer AS member_count
			FROM content_item_topics cit
			JOIN content_items ci ON ci.public_id = cit.content_item_id
			WHERE ci.tenant_id = ?
			GROUP BY cit.topic_id
		) counts
		WHERE t.tenant_id = ? AND t.public_id = counts.topic_id
	`, tenantID, tenantID).Error; err != nil {
			return err
		}
		return tx.Exec(`
		UPDATE topics t SET member_count = 0
		WHERE t.tenant_id = ? AND NOT EXISTS (
			SELECT 1 FROM content_item_topics cit
			JOIN content_items ci ON ci.public_id = cit.content_item_id
			WHERE cit.topic_id = t.public_id AND ci.tenant_id = ?
		)
	`, tenantID, tenantID).Error
	})
}

func hydrateMissingTopicCentroids(db *gorm.DB, tenantID string) {
	var topics []models.Topic
	if err := db.Where("tenant_id = ? AND active = ? AND centroid IS NULL", tenantID, true).Find(&topics).Error; err != nil {
		return
	}
	for _, topic := range topics {
		emb, observedSpace, err := embedQueryViaEnrichmentWithSpace(topic.LabelEN + " " + topic.LabelAR)
		if err != nil || len(emb) != 1024 {
			continue
		}
		vec := pgvector.NewVector(emb)
		updates := map[string]interface{}{"centroid": vec}
		if model, producer, ok := textStampForObservedSpace(spaceid.RecipeTopicCentroid, observedSpace); ok {
			updates["centroid_model"], updates["centroid_space_id"], updates["centroid_producer_id"] = model, observedSpace, producer
		}
		_ = db.Model(&models.Topic{}).Where("tenant_id = ? AND public_id = ?", tenantID, topic.PublicID).Updates(updates).Error
	}
}

// remapSingleTopic maps ONE topic against already-mapped corpus rows, so a topic
// approved/created/edited after initial seeding earns mappings on existing content
// instead of only matching content ingested afterwards. Semantics are
// delete-then-insert scoped to this one topic: it never touches other topics' rows
// and is NOT the forbidden full remap. Bounded by paging.
//
// It deliberately augments only items/stories that ALREADY have a mapping row.
// Truly-unmapped rows are left to the incremental sweep, which evaluates them
// against ALL active topics (this one included) and writes their full top-K — if
// remapSingleTopic first-mapped an unmapped item to this single topic, the
// incremental sweep's NOT-EXISTS filter would then skip it and it would never get
// its other topics.
func remapSingleTopic(db *gorm.DB, tenantID string, topic models.Topic) {
	if topic.Centroid == nil {
		return
	}
	tv := []topicVector{{ID: topic.PublicID, CategorySlug: topic.CategorySlug, Vec: topic.Centroid.Slice()}}
	db.Where("topic_id = ?", topic.PublicID).Delete(&models.ContentItemTopic{})
	db.Where("topic_id = ?", topic.PublicID).Delete(&models.StoryTopic{})

	var lastItemID uint
	for {
		var items []models.ContentItem
		if err := db.Select("id, public_id, embedding").
			Where("tenant_id = ? AND status = ? AND embedding IS NOT NULL AND embedding_space_id = ? AND id > ?", tenantID, models.ContentStatusReady, currentTextSpaceIDForSimilarity(), lastItemID).
			Where("EXISTS (SELECT 1 FROM content_item_topics cit WHERE cit.content_item_id = content_items.public_id)").
			Order("id ASC").Limit(remapCatalogBatchSize).Find(&items).Error; err != nil || len(items) == 0 {
			break
		}
		rows := make([]models.ContentItemTopic, 0, len(items))
		for _, item := range items {
			lastItemID = item.ID
			if item.Embedding == nil {
				continue
			}
			if m := topTopicMatches(item.Embedding.Slice(), tv); len(m) > 0 {
				rows = append(rows, models.ContentItemTopic{ContentItemID: item.PublicID, TopicID: topic.PublicID, Score: m[0].Score})
			}
		}
		if len(rows) > 0 {
			db.Create(&rows)
		}
		if len(items) < remapCatalogBatchSize {
			break
		}
	}

	var lastStoryID uint
	for {
		var stories []models.Story
		if err := db.Select("id, public_id, embedding").
			Where("tenant_id = ? AND embedding IS NOT NULL AND embedding_space_id = ? AND id > ?", tenantID, currentTextSpaceIDForSimilarity(), lastStoryID).
			Where("EXISTS (SELECT 1 FROM story_topics st WHERE st.story_id = stories.public_id)").
			Order("id ASC").Limit(remapCatalogBatchSize).Find(&stories).Error; err != nil || len(stories) == 0 {
			break
		}
		rows := make([]models.StoryTopic, 0, len(stories))
		for _, story := range stories {
			lastStoryID = story.ID
			if story.Embedding == nil {
				continue
			}
			if m := topTopicMatches(story.Embedding.Slice(), tv); len(m) > 0 {
				rows = append(rows, models.StoryTopic{StoryID: story.PublicID, TopicID: topic.PublicID, Score: m[0].Score})
			}
		}
		if len(rows) > 0 {
			db.Create(&rows)
		}
		if len(stories) < remapCatalogBatchSize {
			break
		}
	}
}

// GetTopicPicker handles GET /api/v1/topics/picker.
func GetTopicPicker(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	tenantID := "default"
	ensureDefaultTopicCategories(db, tenantID)
	var categories []models.TopicCategory
	db.Where("tenant_id = ? AND active = ?", tenantID, true).Order("sort_order ASC, slug ASC").Find(&categories)
	var topics []models.Topic
	db.Where("tenant_id = ? AND active = ? AND featured = ?", tenantID, true, true).Order("category_slug ASC, slug ASC").Find(&topics)
	c.JSON(http.StatusOK, gin.H{"categories": categories, "topics": topics})
}

// GetPreferences handles GET /api/v1/preferences.
func GetPreferences(c *gin.Context) {
	uid, ok := authedUserID(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, utils.HTTPError{Code: http.StatusUnauthorized, Message: "Authentication required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	resp, err := buildPreferenceResponse(db, "default", uid)
	if err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Failed to load preferences"})
		return
	}
	c.JSON(http.StatusOK, resp)
}

func buildPreferenceResponse(db *gorm.DB, tenantID string, uid uuid.UUID) (gin.H, error) {
	var prefs []models.UserTopicPref
	if err := db.Where("tenant_id = ? AND user_id = ?", tenantID, uid).Find(&prefs).Error; err != nil {
		return nil, err
	}
	var aff []models.UserTopicAffinity
	if err := db.Where("tenant_id = ? AND user_id = ? AND score >= ?", tenantID, uid, 0.05).Order("score DESC").Limit(20).Find(&aff).Error; err != nil {
		return nil, err
	}
	topicIDs := make([]uuid.UUID, 0, len(prefs)+len(aff))
	seen := map[uuid.UUID]bool{}
	for _, p := range prefs {
		if !seen[p.TopicID] {
			topicIDs = append(topicIDs, p.TopicID)
			seen[p.TopicID] = true
		}
	}
	for _, a := range aff {
		if !seen[a.TopicID] {
			topicIDs = append(topicIDs, a.TopicID)
			seen[a.TopicID] = true
		}
	}
	var topics []models.Topic
	if len(topicIDs) > 0 {
		db.Where("tenant_id = ? AND public_id IN ?", tenantID, topicIDs).Find(&topics)
	}
	byID := map[uuid.UUID]models.Topic{}
	for _, t := range topics {
		byID[t.PublicID] = t
	}
	declared, muted, learned := []topicSubject{}, []topicSubject{}, []topicSubject{}
	for _, p := range prefs {
		t, ok := byID[p.TopicID]
		if !ok {
			continue
		}
		s := topicSubject{ID: t.PublicID, Slug: t.Slug, LabelAR: t.LabelAR, LabelEN: t.LabelEN, CategorySlug: t.CategorySlug, State: p.State}
		if p.State == "muted" {
			muted = append(muted, s)
		} else {
			declared = append(declared, s)
		}
	}
	declaredSet, mutedSet := map[uuid.UUID]bool{}, map[uuid.UUID]bool{}
	for _, p := range prefs {
		if p.State == "declared" {
			declaredSet[p.TopicID] = true
		}
		if p.State == "muted" {
			mutedSet[p.TopicID] = true
		}
	}
	for _, a := range aff {
		if declaredSet[a.TopicID] || mutedSet[a.TopicID] {
			continue
		}
		t, ok := byID[a.TopicID]
		if !ok {
			continue
		}
		learned = append(learned, topicSubject{ID: t.PublicID, Slug: t.Slug, LabelAR: t.LabelAR, LabelEN: t.LabelEN, CategorySlug: t.CategorySlug, Score: a.Score})
	}
	return gin.H{"declared": declared, "learned": learned, "muted": muted}, nil
}

// PutPreferenceTopics handles PUT /api/v1/preferences/topics.
func PutPreferenceTopics(c *gin.Context) {
	uid, ok := authedUserID(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, utils.HTTPError{Code: http.StatusUnauthorized, Message: "Authentication required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	if c.Request.ContentLength > maxPreferenceMutationBodyBytes {
		c.JSON(http.StatusRequestEntityTooLarge, utils.HTTPError{Code: http.StatusRequestEntityTooLarge, Message: "Preference request body is too large"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxPreferenceMutationBodyBytes)
	var req replaceDeclaredTopicsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid request"})
		return
	}
	rawUnique := make(map[string]struct{}, len(req.TopicIDs))
	for _, raw := range req.TopicIDs {
		rawUnique[raw] = struct{}{}
	}
	if len(rawUnique) > maxDeclaredTopicCount {
		c.JSON(http.StatusUnprocessableEntity, utils.HTTPError{Code: http.StatusUnprocessableEntity, Message: "Too many declared topics"})
		return
	}
	ids := make([]uuid.UUID, 0, len(req.TopicIDs))
	seen := make(map[uuid.UUID]bool, len(req.TopicIDs))
	for _, raw := range req.TopicIDs {
		id, err := uuid.Parse(raw)
		if err != nil {
			c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid topic id"})
			return
		}
		if !seen[id] {
			ids = append(ids, id)
			seen[id] = true
		}
	}
	if len(ids) > 0 {
		var allowed int64
		db.Model(&models.Topic{}).Where("tenant_id = ? AND active = ? AND featured = ? AND public_id IN ?", "default", true, true, ids).Count(&allowed)
		if allowed != int64(len(ids)) {
			c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Topics must be active picker topics"})
			return
		}
	}
	err := db.Transaction(func(tx *gorm.DB) error {
		// The picker intentionally excludes inactive/de-featured catalog topics.
		// Preserve existing hidden declarations while replacing only declarations
		// that were visible in the current picker; otherwise a catalog change can
		// make the consumer editor permanently unsavable.
		visibleTopicIDs := tx.Model(&models.Topic{}).
			Select("public_id").Where("tenant_id = ? AND active = ? AND featured = ?", "default", true, true)
		if err := tx.Where("tenant_id = ? AND user_id = ? AND state = ? AND topic_id IN (?)", "default", uid, "declared", visibleTopicIDs).
			Delete(&models.UserTopicPref{}).Error; err != nil {
			return err
		}
		for _, id := range ids {
			p := models.UserTopicPref{TenantID: "default", UserID: uid, TopicID: id, State: "declared"}
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "tenant_id"}, {Name: "user_id"}, {Name: "topic_id"}},
				DoUpdates: clause.Assignments(map[string]interface{}{"state": "declared", "updated_at": time.Now()}),
			}).Create(&p).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Failed to save preferences"})
		return
	}
	respondPreferenceMutation(c, db, uid, "default")
}

// MutePreferenceTopic handles POST /api/v1/preferences/topics/:id/mute.
func MutePreferenceTopic(c *gin.Context) {
	setPreferenceMute(c, true)
}

// UnmutePreferenceTopic handles DELETE /api/v1/preferences/topics/:id/mute.
func UnmutePreferenceTopic(c *gin.Context) {
	setPreferenceMute(c, false)
}

func setPreferenceMute(c *gin.Context, muted bool) {
	uid, ok := authedUserID(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, utils.HTTPError{Code: http.StatusUnauthorized, Message: "Authentication required"})
		return
	}
	tid, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid topic id"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var topic models.Topic
	if err := db.Where("tenant_id = ? AND public_id = ? AND active = ?", "default", tid, true).First(&topic).Error; err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{Code: http.StatusNotFound, Message: "Topic not found"})
		return
	}
	if muted {
		p := models.UserTopicPref{TenantID: "default", UserID: uid, TopicID: tid, State: "muted"}
		if err := db.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "tenant_id"}, {Name: "user_id"}, {Name: "topic_id"}},
			DoUpdates: clause.Assignments(map[string]interface{}{"state": "muted", "updated_at": time.Now()}),
		}).Create(&p).Error; err != nil {
			c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Failed to save mute preference"})
			return
		}
	} else {
		if err := db.Where("tenant_id = ? AND user_id = ? AND topic_id = ? AND state = ?", "default", uid, tid, "muted").Delete(&models.UserTopicPref{}).Error; err != nil {
			c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Failed to remove mute preference"})
			return
		}
	}
	respondPreferenceMutation(c, db, uid, "default")
}

// respondPreferenceMutation keeps the user-owned preference write durable even
// when derived affinities need a retry. A successful preference mutation must
// never be reported as if the feed cache were already refreshed.
func respondPreferenceMutation(c *gin.Context, db *gorm.DB, uid uuid.UUID, tenantID string) {
	status := http.StatusOK
	queued := false
	if err := recomputeUserAffinity(db, uid, tenantID); err != nil {
		if enqueueErr := enqueueAffinityRecompute(db, tenantID, uid, models.PreferenceRecomputeReasonFailed); enqueueErr != nil {
			c.JSON(http.StatusServiceUnavailable, utils.HTTPError{Code: http.StatusServiceUnavailable, Message: "Preference saved, but feed recomputation could not be queued"})
			return
		}
		status, queued = http.StatusAccepted, true
	}
	resp, err := buildPreferenceResponse(db, tenantID, uid)
	if err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Preference saved, but the authoritative response could not be loaded"})
		return
	}
	if queued {
		resp["recompute_status"] = "queued"
	}
	c.JSON(status, resp)
}

// affinityDecayHorizons bounds how far back the interaction scan reaches, in
// multiples of the decay half-life. At 5 half-lives an interaction's weight has
// decayed to ~3%, so ignoring older rows barely moves the score while keeping the
// scan from growing unbounded with account age.
const affinityDecayHorizons = 5

func recomputeUserAffinity(db *gorm.DB, uid uuid.UUID, tenantID string) error {
	return recomputeUserAffinityCfg(db, uid, tenantID, loadPreferenceSettings(db, tenantID))
}

// recomputeUserAffinityCfg takes settings as a parameter so batch callers (the
// heartbeat) load them once instead of once per user.
func recomputeUserAffinityCfg(db *gorm.DB, uid uuid.UUID, tenantID string, cfg models.PreferenceSettings) error {
	start := time.Now()
	err := db.Transaction(func(tx *gorm.DB) error {
		cutoff := time.Now().AddDate(0, 0, -int(cfg.DecayHalfLifeDays*affinityDecayHorizons))
		type signalRow struct {
			TopicID   uuid.UUID
			MapScore  float64
			Type      models.InteractionType
			CreatedAt time.Time
		}
		var signals []signalRow
		if err := tx.Raw(`
		SELECT cit.topic_id, cit.score AS map_score, ui.type, ui.created_at
		FROM user_interactions ui
		JOIN content_item_topics cit ON cit.content_item_id = ui.content_item_id
		JOIN content_items ci ON ci.public_id = ui.content_item_id
		JOIN topics t ON t.public_id = cit.topic_id
		WHERE ui.user_id = ? AND ui.created_at > ? AND ci.tenant_id = ? AND t.tenant_id = ? AND t.active = true
	`, uid, cutoff, tenantID, tenantID).Scan(&signals).Error; err != nil {
			return err
		}
		raw := map[uuid.UUID]float64{}
		for _, s := range signals {
			raw[s.TopicID] += interactionWeight(s.Type, cfg) * s.MapScore * decayMultiplier(s.CreatedAt, cfg)
		}
		var prefs []models.UserTopicPref
		if err := tx.Where("tenant_id = ? AND user_id = ?", tenantID, uid).Find(&prefs).Error; err != nil {
			return err
		}
		muted := map[uuid.UUID]bool{}
		declared := map[uuid.UUID]bool{}
		for _, p := range prefs {
			if p.State == "muted" {
				muted[p.TopicID] = true
				delete(raw, p.TopicID)
				continue
			}
			if p.State == "declared" {
				declared[p.TopicID] = true
				raw[p.TopicID] += cfg.DeclaredPrior
			}
		}

		categoryRaw := map[string]float64{}
		if len(raw) > 0 {
			ids := make([]uuid.UUID, 0, len(raw))
			for tid := range raw {
				ids = append(ids, tid)
			}
			var topics []models.Topic
			if err := tx.Where("tenant_id = ? AND public_id IN ?", tenantID, ids).Find(&topics).Error; err != nil {
				return err
			}
			for _, t := range topics {
				if t.CategorySlug == "" || muted[t.PublicID] {
					continue
				}
				v := raw[t.PublicID] * cfg.CategoryDiscount
				if v > categoryRaw[t.CategorySlug] {
					categoryRaw[t.CategorySlug] = v
				}
			}
		}
		type storyCatSignal struct {
			Category  string
			Type      models.InteractionType
			CreatedAt time.Time
		}
		var catSignals []storyCatSignal
		if err := tx.Raw(`
		SELECT s.category, ui.type, ui.created_at
		FROM user_interactions ui
		JOIN content_items ci ON ci.public_id = ui.content_item_id
		JOIN stories s ON s.public_id = ci.story_id
		WHERE ui.user_id = ? AND ui.created_at > ? AND ci.tenant_id = ? AND s.tenant_id = ? AND s.category IS NOT NULL AND s.category <> ''
	`, uid, cutoff, tenantID, tenantID).Scan(&catSignals).Error; err != nil {
			return err
		}
		for _, s := range catSignals {
			categoryRaw[s.Category] += interactionWeight(s.Type, cfg) * cfg.CategoryDiscount * decayMultiplier(s.CreatedAt, cfg)
		}

		affRows := make([]models.UserTopicAffinity, 0, len(raw))
		for tid, v := range raw {
			if muted[tid] || v <= 0 {
				continue
			}
			affRows = append(affRows, models.UserTopicAffinity{TenantID: tenantID, UserID: uid, TopicID: tid, Score: v / (v + affinityK), Declared: declared[tid]})
		}
		catRows := make([]models.UserCategoryAffinity, 0, len(categoryRaw))
		for cat, v := range categoryRaw {
			if v > 0 {
				catRows = append(catRows, models.UserCategoryAffinity{TenantID: tenantID, UserID: uid, CategorySlug: cat, Score: v / (v + affinityK)})
			}
		}
		if err := tx.Where("tenant_id = ? AND user_id = ?", tenantID, uid).Delete(&models.UserTopicAffinity{}).Error; err != nil {
			return err
		}
		if err := tx.Where("tenant_id = ? AND user_id = ?", tenantID, uid).Delete(&models.UserCategoryAffinity{}).Error; err != nil {
			return err
		}
		if len(affRows) > 0 {
			if err := tx.Create(&affRows).Error; err != nil {
				return err
			}
		}
		if len(catRows) > 0 {
			if err := tx.Create(&catRows).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	recordPreferenceRecompute(db, tenantID, time.Since(start))
	return nil
}

func recordPreferenceRecompute(db *gorm.DB, tenantID string, d time.Duration) {
	day := time.Now().UTC().Truncate(24 * time.Hour)
	stat := models.PreferenceStat{TenantID: tenantID, Day: day}
	_ = db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "tenant_id"}, {Name: "day"}},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"recompute_runs":     gorm.Expr("preference_stats.recompute_runs + 1"),
			"recompute_ms_total": gorm.Expr("preference_stats.recompute_ms_total + ?", d.Milliseconds()),
			"updated_at":         time.Now(),
		}),
	}).Create(&stat).Error
}

// The catalog-maintenance heartbeat (formerly StartTopicsHeartbeat) is now owned
// by the Preferences Autopilot scheduler: its incumbent body lives in
// runPreferenceBaseline (preferenceAutopilotRunner.go) and is driven by
// StartPreferenceAutopilotHeartbeat (preferenceAutopilotScheduler.go). Disabled
// tenants get exactly today's behavior; enabled tenants get the ledgered runner.

// GetPreferenceSettings handles GET /admin/preferences/settings.
func GetPreferenceSettings(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	cfg := loadPreferenceSettings(c.MustGet("db").(*gorm.DB), principal.TenantID)
	c.JSON(http.StatusOK, gin.H{"data": cfg})
}

// UpdatePreferenceSettings handles PUT /admin/preferences/settings.
func UpdatePreferenceSettings(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	cfg := loadPreferenceSettings(db, principal.TenantID)
	var patch map[string]interface{}
	if err := c.ShouldBindJSON(&patch); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request", Code: "INVALID_REQUEST"})
		return
	}
	delete(patch, "tenant_id")
	for key, value := range patch {
		if key == "foryou_enabled" || key == "news_enabled" {
			if _, ok := value.(bool); !ok {
				c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Boolean setting required: " + key, Code: "INVALID_SETTING_VALUE"})
				return
			}
			continue
		}
		n, ok := value.(float64)
		if !ok || !validPreferenceSettingValue(key, n) {
			c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid value for setting: " + key, Code: "INVALID_SETTING_VALUE"})
			return
		}
	}
	if err := db.Model(&cfg).Updates(patch).Error; err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Failed to update settings", Code: "UPDATE_FAILED"})
		return
	}
	invalidatePrefSettingsCache(principal.TenantID) // kill switch / weight changes take effect immediately
	cfg = loadPreferenceSettings(db, principal.TenantID)
	c.JSON(http.StatusOK, gin.H{"data": cfg})
}

type topicCategoryRequest struct {
	Slug      string `json:"slug"`
	LabelAR   string `json:"label_ar"`
	LabelEN   string `json:"label_en"`
	SortOrder *int   `json:"sort_order"`
	Active    *bool  `json:"active"`
}

// AdminListTopicCategories handles GET /admin/topics/categories.
func AdminListTopicCategories(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	ensureDefaultTopicCategories(db, principal.TenantID)
	var categories []models.TopicCategory
	if err := db.Where("tenant_id = ?", principal.TenantID).Order("sort_order ASC, slug ASC").Find(&categories).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to list categories", Code: "CATEGORIES_LIST_FAILED"})
		return
	}
	// Topic counts per category so the UI can show how populated each one is.
	type catCount struct {
		CategorySlug string
		N            int64
	}
	var counts []catCount
	db.Model(&models.Topic{}).
		Select("category_slug, COUNT(*) AS n").
		Where("tenant_id = ? AND category_slug <> ''", principal.TenantID).
		Group("category_slug").Scan(&counts)
	countBySlug := make(map[string]int64, len(counts))
	for _, cc := range counts {
		countBySlug[cc.CategorySlug] = cc.N
	}
	c.JSON(http.StatusOK, gin.H{"data": categories, "topic_counts": countBySlug})
}

// AdminCreateTopicCategory handles POST /admin/topics/categories.
func AdminCreateTopicCategory(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var req topicCategoryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request", Code: "INVALID_REQUEST"})
		return
	}
	slug := normalizedTopicSlug(req.Slug)
	if slug == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Category slug is required", Code: "INVALID_SLUG"})
		return
	}
	en := strings.TrimSpace(req.LabelEN)
	if en == "" {
		en = strings.ReplaceAll(slug, "-", " ")
	}
	ar := strings.TrimSpace(req.LabelAR)
	if ar == "" {
		ar = en
	}
	cat := models.TopicCategory{TenantID: principal.TenantID, Slug: slug, LabelAR: ar, LabelEN: en, Active: true}
	if req.SortOrder != nil {
		cat.SortOrder = *req.SortOrder
	}
	if req.Active != nil {
		cat.Active = *req.Active
	}
	if err := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "tenant_id"}, {Name: "slug"}},
		DoNothing: true,
	}).Create(&cat).Error; err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Failed to create category: " + err.Error(), Code: "CREATE_FAILED"})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"data": cat})
}

// AdminUpdateTopicCategory handles PUT /admin/topics/categories/:slug.
func AdminUpdateTopicCategory(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	slug := normalizedTopicSlug(c.Param("slug"))
	if slug == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid category slug", Code: "INVALID_SLUG"})
		return
	}
	var req topicCategoryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request", Code: "INVALID_REQUEST"})
		return
	}
	updates := map[string]interface{}{}
	if strings.TrimSpace(req.LabelAR) != "" {
		updates["label_ar"] = strings.TrimSpace(req.LabelAR)
	}
	if strings.TrimSpace(req.LabelEN) != "" {
		updates["label_en"] = strings.TrimSpace(req.LabelEN)
	}
	if req.SortOrder != nil {
		updates["sort_order"] = *req.SortOrder
	}
	if req.Active != nil {
		updates["active"] = *req.Active
	}
	if len(updates) == 0 {
		c.JSON(http.StatusOK, gin.H{"message": "No changes"})
		return
	}
	if err := db.Model(&models.TopicCategory{}).Where("tenant_id = ? AND slug = ?", principal.TenantID, slug).Updates(updates).Error; err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Failed to update category: " + err.Error(), Code: "UPDATE_FAILED"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "Category updated"})
}

// AdminDeleteTopic handles DELETE /admin/topics/catalog/:id. Irreversible: purges
// the topic's content/story mappings AND its user preference + affinity rows
// (those tables carry no FK cascade to topics), then removes the topic. The UI
// defaults to deactivate; this is the explicit destructive path.
func AdminDeleteTopic(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid topic id", Code: "INVALID_ID"})
		return
	}
	var topic models.Topic
	if err := db.Where("tenant_id = ? AND public_id = ?", principal.TenantID, id).First(&topic).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Topic not found", Code: "NOT_FOUND"})
		return
	}
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("topic_id = ?", topic.PublicID).Delete(&models.ContentItemTopic{}).Error; err != nil {
			return err
		}
		if err := tx.Where("topic_id = ?", topic.PublicID).Delete(&models.StoryTopic{}).Error; err != nil {
			return err
		}
		if err := tx.Where("tenant_id = ? AND topic_id = ?", principal.TenantID, topic.PublicID).Delete(&models.UserTopicPref{}).Error; err != nil {
			return err
		}
		if err := tx.Where("tenant_id = ? AND topic_id = ?", principal.TenantID, topic.PublicID).Delete(&models.UserTopicAffinity{}).Error; err != nil {
			return err
		}
		return tx.Where("tenant_id = ? AND public_id = ?", principal.TenantID, topic.PublicID).Delete(&models.Topic{}).Error
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to delete topic: " + err.Error(), Code: "DELETE_FAILED"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "Topic deleted"})
}

// AdminGetTopicDrilldown handles GET /admin/topics/catalog/:id. Read-only:
// returns the topic plus its top-scoring mapped content items and stories, so an
// admin can see WHAT a topic actually captures before editing/deleting it.
func AdminGetTopicDrilldown(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid topic id", Code: "INVALID_ID"})
		return
	}
	var topic models.Topic
	if err := db.Where("tenant_id = ? AND public_id = ?", principal.TenantID, id).First(&topic).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Topic not found", Code: "NOT_FOUND"})
		return
	}

	const drilldownLimit = 25
	type mappedItem struct {
		ID    uuid.UUID `json:"id"`
		Title string    `json:"title"`
		Type  string    `json:"type"`
		Score float64   `json:"score"`
	}
	type mappedStory struct {
		ID    uuid.UUID `json:"id"`
		Label string    `json:"label"`
		Score float64   `json:"score"`
	}
	var items []mappedItem
	db.Table("content_item_topics cit").
		Select("ci.public_id AS id, COALESCE(ci.title, '') AS title, ci.type, cit.score").
		Joins("JOIN content_items ci ON ci.public_id = cit.content_item_id").
		Where("cit.topic_id = ? AND ci.tenant_id = ?", topic.PublicID, principal.TenantID).
		Order("cit.score DESC").Limit(drilldownLimit).Scan(&items)
	var stories []mappedStory
	db.Table("story_topics st").
		Select("s.public_id AS id, COALESCE(s.label, '') AS label, st.score").
		Joins("JOIN stories s ON s.public_id = st.story_id").
		Where("st.topic_id = ? AND s.tenant_id = ?", topic.PublicID, principal.TenantID).
		Order("st.score DESC").Limit(drilldownLimit).Scan(&stories)

	var itemCount, storyCount int64
	db.Model(&models.ContentItemTopic{}).Where("topic_id = ?", topic.PublicID).Count(&itemCount)
	db.Model(&models.StoryTopic{}).Where("topic_id = ?", topic.PublicID).Count(&storyCount)

	c.JSON(http.StatusOK, gin.H{
		"topic":          topic,
		"mapped_items":   items,
		"mapped_stories": stories,
		"item_count":     itemCount,
		"story_count":    storyCount,
	})
}
