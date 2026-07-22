package controllers

import (
	"content-management-system/src/models"
	"sort"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func applyPreferenceFeedHook(db *gorm.DB, tenantID string, userIDStr string, scored []ScoredItem) ([]ScoredItem, bool) {
	if userIDStr == "" {
		return scored, false
	}
	userID, err := uuid.Parse(userIDStr)
	if err != nil {
		return scored, false
	}
	mutedSources := loadMutedSourceKeys(db, tenantID, userID)
	if len(mutedSources) > 0 {
		filtered := scored[:0]
		for _, candidate := range scored {
			if _, muted := mutedSources[canonicalContentSourceKey(candidate.Item)]; !muted {
				filtered = append(filtered, candidate)
			}
		}
		scored = filtered
	}
	if len(scored) < 2 {
		return scored, len(mutedSources) > 0
	}
	cfg := loadPreferenceSettingsCached(db, tenantID)
	if !cfg.ForYouEnabled {
		return scored, false
	}
	// From here the authenticated, enabled hook ran. It may legitimately find no
	// affinities or matches, but it is an eligible personalization observation.
	eligible := true
	topicAff, categoryAff, mutedTopics := loadUserAffinityMaps(db, tenantID, userID)
	if len(topicAff) == 0 && len(categoryAff) == 0 {
		return scored, eligible
	}
	ids := make([]uuid.UUID, 0, len(scored))
	for _, s := range scored {
		ids = append(ids, s.Item.PublicID)
	}
	type row struct {
		ContentItemID uuid.UUID
		TopicID       uuid.UUID
		Score         float64
		CategorySlug  string
	}
	var rows []row
	db.Table("content_item_topics cit").
		Select("cit.content_item_id, cit.topic_id, cit.score, topics.category_slug").
		Joins("JOIN topics ON topics.public_id = cit.topic_id").
		Where("cit.content_item_id IN ? AND topics.tenant_id = ?", ids, tenantID).
		Scan(&rows)
	byItem := map[uuid.UUID]float64{}
	for _, r := range rows {
		if mutedTopics[r.TopicID] {
			continue
		}
		a := topicAff[r.TopicID]
		if r.CategorySlug != "" && categoryAff[r.CategorySlug] > a {
			a = categoryAff[r.CategorySlug]
		}
		a *= r.Score
		if a > byItem[r.ContentItemID] {
			byItem[r.ContentItemID] = a
		}
	}
	boosted := 0
	for i := range scored {
		aff := byItem[scored[i].Item.PublicID]
		if aff <= 0 {
			continue
		}
		if aff > 1 {
			aff = 1
		}
		boost := cfg.WForYou * aff
		scored[i].FinalScore *= 1 + boost
		scored[i].ScoreBreakdown.Preference = boost
		boosted++
	}
	if boosted > 0 {
		sort.SliceStable(scored, func(i, j int) bool { return scored[i].FinalScore > scored[j].FinalScore })
		scored = applyDiversityPenalty(scored)
	}
	return scored, eligible
}

func loadMutedSourceKeys(db *gorm.DB, tenantID string, userID uuid.UUID) map[string]struct{} {
	var prefs []models.UserSourcePref
	db.Where("tenant_id = ? AND user_id = ? AND state = ?", tenantID, userID, "muted").Find(&prefs)
	keys := make(map[string]struct{}, len(prefs))
	for _, pref := range prefs {
		keys[pref.SourceKey] = struct{}{}
	}
	return keys
}

// applyChronologicalPreferenceOrder keeps the default feed chronological at
// page granularity, while allowing affinities to move a matching item a small
// number of positions inside that page. The cursor remains tied to the
// chronological boundary selected before this function runs.
func applyChronologicalPreferenceOrder(db *gorm.DB, tenantID, userIDStr string, items []models.ContentItem) ([]models.ContentItem, int, bool) {
	if len(items) == 0 {
		return items, 0, false
	}
	scored := make([]ScoredItem, len(items))
	for i, item := range items {
		scored[i] = ScoredItem{Item: item, FinalScore: float64(len(items) - i)}
	}
	var eligible bool
	scored, eligible = applyPreferenceFeedHook(db, tenantID, userIDStr, scored)
	boosted := 0
	items = items[:0]
	for _, s := range scored {
		items = append(items, s.Item)
		if s.ScoreBreakdown.Preference > 0 {
			boosted++
		}
	}
	return items, boosted, eligible
}

func loadUserAffinityMaps(db *gorm.DB, tenantID string, userID uuid.UUID) (map[uuid.UUID]float64, map[string]float64, map[uuid.UUID]bool) {
	var rows []models.UserTopicAffinity
	db.Where("tenant_id = ? AND user_id = ? AND score > 0", tenantID, userID).Find(&rows)
	topicAff := make(map[uuid.UUID]float64, len(rows))
	for _, r := range rows {
		topicAff[r.TopicID] = r.Score
	}
	var cats []models.UserCategoryAffinity
	db.Where("tenant_id = ? AND user_id = ? AND score > 0", tenantID, userID).Find(&cats)
	categoryAff := make(map[string]float64, len(cats))
	for _, r := range cats {
		categoryAff[r.CategorySlug] = r.Score
	}
	var prefs []models.UserTopicPref
	db.Where("tenant_id = ? AND user_id = ? AND state = ?", tenantID, userID, "muted").Find(&prefs)
	mutedTopics := make(map[uuid.UUID]bool, len(prefs))
	for _, p := range prefs {
		mutedTopics[p.TopicID] = true
	}
	return topicAff, categoryAff, mutedTopics
}

func recordPreferenceServes(db *gorm.DB, tenantID string, eligible bool, boosted, total int64) {
	if !eligible || total <= 0 {
		return
	}
	day := time.Now().UTC().Truncate(24 * time.Hour)
	stat := models.PreferenceStat{TenantID: tenantID, Day: day}
	_ = db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "tenant_id"}, {Name: "day"}},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"boosted_serves": gorm.Expr("preference_stats.boosted_serves + ?", boosted),
			"total_serves":   gorm.Expr("preference_stats.total_serves + ?", total),
			"updated_at":     time.Now(),
		}),
	}).Create(&stat).Error
}

func shouldPersonalizeNews(db *gorm.DB, tenantID string, userIDStr string) bool {
	if userIDStr == "" {
		return false
	}
	if _, err := uuid.Parse(userIDStr); err != nil {
		return false
	}
	return loadPreferenceSettingsCached(db, tenantID).NewsEnabled
}

func applyNewsPreferenceBoost(db *gorm.DB, tenantID string, userIDStr string, order []*storyAgg, storyIDs []uuid.UUID) ([]*storyAgg, bool) {
	if len(order) < 2 || !shouldPersonalizeNews(db, tenantID, userIDStr) {
		return order, false
	}
	userID, err := uuid.Parse(userIDStr)
	if err != nil {
		return order, false
	}
	eligible := true
	topicAff, categoryAff, mutedTopics := loadUserAffinityMaps(db, tenantID, userID)
	if len(topicAff) == 0 && len(categoryAff) == 0 {
		return order, eligible
	}
	cfg := loadPreferenceSettingsCached(db, tenantID)
	affByStory := make(map[uuid.UUID]float64, len(order))

	type topicRow struct {
		StoryID      uuid.UUID
		TopicID      uuid.UUID
		Score        float64
		CategorySlug string
	}
	var topicRows []topicRow
	db.Table("story_topics st").
		Select("st.story_id, st.topic_id, st.score, topics.category_slug").
		Joins("JOIN topics ON topics.public_id = st.topic_id").
		Where("st.story_id IN ? AND topics.tenant_id = ?", storyIDs, tenantID).
		Scan(&topicRows)
	for _, r := range topicRows {
		if mutedTopics[r.TopicID] {
			continue
		}
		a := topicAff[r.TopicID]
		if r.CategorySlug != "" && categoryAff[r.CategorySlug] > a {
			a = categoryAff[r.CategorySlug]
		}
		a *= r.Score
		if a > affByStory[r.StoryID] {
			affByStory[r.StoryID] = a
		}
	}

	type catRow struct {
		PublicID uuid.UUID
		Category string
	}
	var cats []catRow
	db.Table("stories").Select("public_id, category").Where("tenant_id = ? AND public_id IN ?", tenantID, storyIDs).Scan(&cats)
	for _, r := range cats {
		if r.Category != "" && categoryAff[r.Category] > affByStory[r.PublicID] {
			affByStory[r.PublicID] = categoryAff[r.Category]
		}
	}

	for _, a := range order {
		aff := affByStory[a.storyID]
		if aff <= 0 {
			continue
		}
		if aff > 1 {
			aff = 1
		}
		a.score *= 1 + cfg.WNews*aff
		a.preferenceBoosted = true
	}
	return order, eligible
}

func reorderRelatedByPreference(db *gorm.DB, tenantID string, userIDStr string, related []StorySummary) []StorySummary {
	if len(related) < 2 || !shouldPersonalizeNews(db, tenantID, userIDStr) {
		return related
	}
	userID, err := uuid.Parse(userIDStr)
	if err != nil {
		return related
	}
	topicAff, categoryAff, mutedTopics := loadUserAffinityMaps(db, tenantID, userID)
	if len(topicAff) == 0 && len(categoryAff) == 0 {
		return related
	}
	storyIDs := make([]uuid.UUID, 0, len(related))
	for _, s := range related {
		storyIDs = append(storyIDs, s.StoryID)
	}
	type row struct {
		StoryID      uuid.UUID
		TopicID      uuid.UUID
		Score        float64
		CategorySlug string
	}
	var rows []row
	db.Table("story_topics st").
		Select("st.story_id, st.topic_id, st.score, topics.category_slug").
		Joins("JOIN topics ON topics.public_id = st.topic_id").
		Where("st.story_id IN ? AND topics.tenant_id = ?", storyIDs, tenantID).
		Scan(&rows)
	aff := map[uuid.UUID]float64{}
	for _, r := range rows {
		if mutedTopics[r.TopicID] {
			continue
		}
		a := topicAff[r.TopicID]
		if r.CategorySlug != "" && categoryAff[r.CategorySlug] > a {
			a = categoryAff[r.CategorySlug]
		}
		a *= r.Score
		if a > aff[r.StoryID] {
			aff[r.StoryID] = a
		}
	}
	for _, s := range related {
		if s.Category != "" && categoryAff[s.Category] > aff[s.StoryID] {
			aff[s.StoryID] = categoryAff[s.Category]
		}
	}
	sort.SliceStable(related, func(i, j int) bool {
		return aff[related[i].StoryID] > aff[related[j].StoryID]
	})
	return related
}
