package controllers

// Month in Review is an immutable archive builder. It has no delete tool: a
// verified archive is a prerequisite for a later slice, never authorization to
// purge the underlying month.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"content-management-system/src/models"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const monthlyReviewFormulaVersion = "v1"

type monthlyReviewPolicyConfig struct {
	FormulaVersion   string  `json:"formula_version"`
	ImportanceWeight float64 `json:"importance_weight"`
	EngagementWeight float64 `json:"engagement_weight"`
	CategoryCap      float64 `json:"category_cap"`
	LeadSourceCap    float64 `json:"lead_source_cap"`
	TargetMin        int     `json:"target_min"`
	TargetMax        int     `json:"target_max"`
}

func defaultMonthlyReviewPolicy() monthlyReviewPolicyConfig {
	return monthlyReviewPolicyConfig{FormulaVersion: monthlyReviewFormulaVersion, ImportanceWeight: .60, EngagementWeight: .40, CategoryCap: .30, LeadSourceCap: .20, TargetMin: 20, TargetMax: 30}
}
func monthlyReviewJSON(v interface{}) datatypes.JSON {
	raw, _ := json.Marshal(v)
	return datatypes.JSON(raw)
}

func monthlyReviewLocation(timezone string) *time.Location {
	l, err := time.LoadLocation(timezone)
	if err != nil {
		return time.FixedZone("Asia/Riyadh", 3*60*60)
	}
	return l
}
func monthlyStart(value time.Time, timezone string) time.Time {
	local := value.In(monthlyReviewLocation(timezone))
	return time.Date(local.Year(), local.Month(), 1, 0, 0, 0, 0, time.UTC)
}

func loadMonthlyReviewPolicy(db *gorm.DB, tenant string) (models.MonthlyReviewPolicyVersion, error) {
	var head models.MonthlyReviewPolicyHead
	if err := db.Where("tenant_id = ?", tenant).First(&head).Error; err == nil {
		var policy models.MonthlyReviewPolicyVersion
		err = db.Where("id = ? AND tenant_id = ?", head.PolicyID, tenant).First(&policy).Error
		return policy, err
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return models.MonthlyReviewPolicyVersion{}, err
	}
	now := time.Now().UTC()
	config := defaultMonthlyReviewPolicy()
	policy := models.MonthlyReviewPolicyVersion{TenantID: tenant, Version: 1, State: "active", Config: monthlyReviewJSON(config), Reason: "Slice 4 deterministic default", CreatedBy: "system", EffectiveAt: now}
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&policy).Error; err != nil {
			return err
		}
		return tx.Create(&models.MonthlyReviewPolicyHead{TenantID: tenant, PolicyID: policy.ID}).Error
	}); err != nil {
		return models.MonthlyReviewPolicyVersion{}, err
	}
	return policy, nil
}

func decodeMonthlyReviewPolicy(policy models.MonthlyReviewPolicyVersion) (monthlyReviewPolicyConfig, error) {
	c := defaultMonthlyReviewPolicy()
	if err := json.Unmarshal(policy.Config, &c); err != nil {
		return c, err
	}
	if c.FormulaVersion != monthlyReviewFormulaVersion || math.Abs(c.ImportanceWeight+c.EngagementWeight-1) > .0001 || c.ImportanceWeight != .60 || c.EngagementWeight != .40 || c.CategoryCap < .20 || c.CategoryCap > .40 || c.LeadSourceCap < .10 || c.LeadSourceCap > .30 || c.TargetMin != 20 || c.TargetMax != 30 {
		return c, errors.New("monthly review policy is outside locked V1 bounds")
	}
	return c, nil
}

type monthlyCandidate struct {
	Story                                                 models.Story
	Lead                                                  models.ContentItem
	Sources                                               int
	CoverageHours                                         float64
	CoverageDays                                          int
	Views, Likes, Shares, Comments, Bookmarks, Meaningful int
	Impressions                                           int64
	ExcludedEvents, DeduplicatedEvents                    int
	Importance, Engagement, Score                         float64
	Category, LeadSource                                  string
}

func monthlyCountNorm(x, p95 float64) float64 {
	if x <= 0 || p95 <= 0 {
		return 0
	}
	return math.Min(1, math.Log1p(x)/math.Log1p(p95))
}
func monthlyMax(values []float64) float64 {
	var max float64
	for _, v := range values {
		if v > max {
			max = v
		}
	}
	return max
}
func monthlyP95(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	if len(values) < 20 {
		return monthlyMax(values)
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	index := int(math.Ceil(.95*float64(len(sorted)))) - 1
	return sorted[index]
}
func monthlyRateNorm(events int, impressions, totalImpressions int64, totalEvents int, p95 float64) float64 {
	if impressions < 1 {
		impressions = 1
	}
	if totalImpressions < 1 || p95 <= 0 {
		return 0
	}
	prior := float64(totalEvents) / float64(totalImpressions)
	rate := (float64(events) + 20*prior) / (float64(impressions) + 20)
	return math.Min(1, rate/p95)
}
func monthlySource(item models.ContentItem) string {
	if s := strings.TrimSpace(derefStr(item.SourceName)); s != "" {
		return strings.ToLower(s)
	}
	return strings.ToLower(string(item.Source))
}

func buildMonthlyCandidates(db *gorm.DB, tenant string, start, end time.Time) ([]monthlyCandidate, error) {
	var stories []models.Story
	if err := db.Where("tenant_id = ? AND last_member_at >= ? AND last_member_at < ?", tenant, start, end).Order("public_id ASC").Find(&stories).Error; err != nil {
		return nil, err
	}
	if len(stories) == 0 {
		return nil, nil
	}
	ids := make([]uuid.UUID, len(stories))
	for i := range stories {
		ids[i] = stories[i].PublicID
	}
	var items []models.ContentItem
	if err := db.Where("tenant_id = ? AND story_id IN ? AND type = ? AND status = ?", tenant, ids, models.ContentTypeNews, models.ContentStatusReady).Find(&items).Error; err != nil {
		return nil, err
	}
	by := map[uuid.UUID][]models.ContentItem{}
	itemStories := map[uuid.UUID]uuid.UUID{}
	for _, item := range items {
		if item.StoryID != nil {
			by[*item.StoryID] = append(by[*item.StoryID], item)
			itemStories[item.PublicID] = *item.StoryID
		}
	}
	itemIDs := make([]uuid.UUID, 0, len(itemStories))
	for id := range itemStories {
		itemIDs = append(itemIDs, id)
	}
	var interactions []models.UserInteraction
	if len(itemIDs) > 0 {
		if err := db.Where("content_item_id IN ? AND created_at >= ? AND created_at < ?", itemIDs, start, end).Find(&interactions).Error; err != nil {
			return nil, err
		}
	}
	// Count one quality event per story, signal, actor/session, and calendar day.
	engagement := map[uuid.UUID]map[models.InteractionType]int{}
	seenEngagement := map[string]bool{}
	excludedByStory, deduplicatedByStory := map[uuid.UUID]int{}, map[uuid.UUID]int{}
	actorWindows := map[string][]time.Time{}
	actorStoryDay := map[string]int{}
	sort.SliceStable(interactions, func(i, j int) bool { return interactions[i].CreatedAt.Before(interactions[j].CreatedAt) })
	for _, event := range interactions {
		storyID, exists := itemStories[event.ContentItemID]
		if !exists {
			continue
		}
		actor := ""
		if event.UserID != nil {
			actor = event.UserID.String()
		} else if event.SessionID != nil {
			actor = strings.TrimSpace(*event.SessionID)
		}
		if actor == "" {
			continue
		}
		window := actorWindows[actor]
		cutoff := event.CreatedAt.Add(-time.Hour)
		for len(window) > 0 && window[0].Before(cutoff) {
			window = window[1:]
		}
		window = append(window, event.CreatedAt)
		actorWindows[actor] = window
		day := event.CreatedAt.UTC().Format("2006-01-02")
		actorStoryKey := actor + ":" + storyID.String() + ":" + day
		actorStoryDay[actorStoryKey]++
		if len(window) > 100 || actorStoryDay[actorStoryKey] > 20 {
			excludedByStory[storyID]++
			continue
		}
		key := storyID.String() + ":" + string(event.Type) + ":" + actor + ":" + day
		if seenEngagement[key] {
			deduplicatedByStory[storyID]++
			continue
		}
		seenEngagement[key] = true
		if engagement[storyID] == nil {
			engagement[storyID] = map[models.InteractionType]int{}
		}
		engagement[storyID][event.Type]++
	}
	result := []monthlyCandidate{}
	for _, story := range stories {
		members := by[story.PublicID]
		if len(members) == 0 {
			continue
		}
		sort.SliceStable(members, func(i, j int) bool { return compactMemberLess(members[i], members[j]) })
		lead := members[0]
		if story.RetainedLeadContentID != nil {
			for _, m := range members {
				if m.PublicID == *story.RetainedLeadContentID {
					lead = m
					break
				}
			}
		}
		sources := map[string]bool{}
		days := map[string]bool{}
		var earliest, latest time.Time
		candidate := monthlyCandidate{Story: story, Lead: lead, Category: strings.ToLower(strings.TrimSpace(derefStr(story.Category))), LeadSource: monthlySource(lead)}
		for _, m := range members {
			sources[monthlySource(m)] = true
			t := itemTime(m)
			if earliest.IsZero() || t.Before(earliest) {
				earliest = t
			}
			if latest.IsZero() || t.After(latest) {
				latest = t
			}
			days[t.UTC().Format("2006-01-02")] = true
			candidate.Views += m.ViewCount
			candidate.Likes += m.LikeCount
			candidate.Shares += m.ShareCount
			candidate.Comments += m.CommentCount
			candidate.Impressions += m.ImpressionCount
		}
		candidate.Sources = len(sources)
		candidate.CoverageDays = len(days)
		candidate.CoverageHours = latest.Sub(earliest).Hours()
		counts := engagement[story.PublicID]
		candidate.Bookmarks = counts[models.InteractionTypeBookmark]
		candidate.Shares = counts[models.InteractionTypeShare]
		candidate.Comments = counts[models.InteractionTypeComment]
		candidate.Likes = counts[models.InteractionTypeLike]
		candidate.Meaningful = counts[models.InteractionTypeMeaningful]
		candidate.ExcludedEvents = excludedByStory[story.PublicID]
		candidate.DeduplicatedEvents = deduplicatedByStory[story.PublicID]
		result = append(result, candidate)
	}
	return result, nil
}

func scoreMonthlyCandidates(c []monthlyCandidate) []monthlyCandidate {
	if len(c) == 0 {
		return c
	}
	sources, hours, days, bookmarks, likes, shares, comments, meaningful := []float64{}, []float64{}, []float64{}, []float64{}, []float64{}, []float64{}, []float64{}, []float64{}
	for _, x := range c {
		sources = append(sources, float64(x.Sources))
		hours = append(hours, x.CoverageHours)
		days = append(days, float64(x.CoverageDays))
		bookmarks = append(bookmarks, float64(x.Bookmarks))
		likes = append(likes, float64(x.Likes))
		shares = append(shares, float64(x.Shares))
		comments = append(comments, float64(x.Comments))
		meaningful = append(meaningful, float64(x.Meaningful))
	}
	ps, ph, pd := monthlyP95(sources), monthlyP95(hours), monthlyP95(days)
	var totalImpressions int64
	totals := map[string]int{"bookmarks": 0, "likes": 0, "shares": 0, "comments": 0, "meaningful": 0}
	for _, x := range c {
		totalImpressions += x.Impressions
		totals["bookmarks"] += x.Bookmarks
		totals["likes"] += x.Likes
		totals["shares"] += x.Shares
		totals["comments"] += x.Comments
		totals["meaningful"] += x.Meaningful
	}
	rateP95 := func(values []float64) float64 { return monthlyP95(values) }
	rates := func(kind string) []float64 {
		out := make([]float64, len(c))
		for i, x := range c {
			n := 0
			switch kind {
			case "bookmarks":
				n = x.Bookmarks
			case "likes":
				n = x.Likes
			case "shares":
				n = x.Shares
			case "comments":
				n = x.Comments
			case "meaningful":
				n = x.Meaningful
			}
			imp := x.Impressions
			if imp < 1 {
				imp = 1
			}
			prior := 0.0
			if totalImpressions > 0 {
				prior = float64(totals[kind]) / float64(totalImpressions)
			}
			out[i] = (float64(n) + 20*prior) / (float64(imp) + 20)
		}
		return out
	}
	bRates, lRates, sRates, cRates, mRates := rates("bookmarks"), rates("likes"), rates("shares"), rates("comments"), rates("meaningful")
	pb, pl, psh, pc, pm := rateP95(bRates), rateP95(lRates), rateP95(sRates), rateP95(cRates), rateP95(mRates)
	for i := range c {
		x := &c[i]
		x.Importance = .30*monthlyCountNorm(float64(x.Sources), ps) + .15*monthlyCountNorm(x.CoverageHours, ph) + .15*monthlyCountNorm(float64(x.CoverageDays), pd)
		x.Engagement = .12*monthlyRateNorm(x.Bookmarks, x.Impressions, totalImpressions, totals["bookmarks"], pb) + .10*monthlyRateNorm(x.Shares, x.Impressions, totalImpressions, totals["shares"], psh) + .08*monthlyRateNorm(x.Comments, x.Impressions, totalImpressions, totals["comments"], pc) + .06*monthlyRateNorm(x.Likes, x.Impressions, totalImpressions, totals["likes"], pl) + .04*monthlyRateNorm(x.Meaningful, x.Impressions, totalImpressions, totals["meaningful"], pm)
		x.Score = x.Importance + x.Engagement
	}
	return c
}

func selectMonthlyCandidates(c []monthlyCandidate, policy monthlyReviewPolicyConfig, overrides map[uuid.UUID]string) (selected []monthlyCandidate, qualified int) {
	for _, x := range c {
		if overrides[x.Story.PublicID] == "exclude" {
			continue
		}
		if overrides[x.Story.PublicID] == "include" || (x.Score >= .10 && (x.Sources >= 2 || x.Bookmarks+x.Shares >= 3)) {
			selected = append(selected, x)
		}
	}
	qualified = len(selected)
	sort.SliceStable(selected, func(i, j int) bool {
		if selected[i].Score != selected[j].Score {
			return selected[i].Score > selected[j].Score
		}
		if selected[i].Sources != selected[j].Sources {
			return selected[i].Sources > selected[j].Sources
		}
		return selected[i].Story.PublicID.String() < selected[j].Story.PublicID.String()
	})
	target := len(selected)
	if target > policy.TargetMax {
		target = policy.TargetMax
	}
	categoryCap := int(math.Ceil(policy.CategoryCap * float64(target)))
	sourceCap := int(math.Ceil(policy.LeadSourceCap * float64(target)))
	cats, sources := map[string]int{}, map[string]int{}
	out := []monthlyCandidate{}
	for _, x := range selected {
		cat := x.Category
		if cat == "" {
			cat = "general"
		}
		src := x.LeadSource
		if src == "" {
			src = "unknown"
		}
		if cats[cat] >= categoryCap || sources[src] >= sourceCap {
			continue
		}
		cats[cat]++
		sources[src]++
		out = append(out, x)
		if len(out) == target {
			break
		}
	}
	return out, qualified
}

func monthlyReviewOverrides(db *gorm.DB, tenant string, start time.Time) (map[uuid.UUID]string, error) {
	var rows []models.MonthlyReviewStoryOverride
	if err := db.Where("tenant_id = ? AND month_start = ?", tenant, start).Find(&rows).Error; err != nil {
		return nil, err
	}
	result := map[uuid.UUID]string{}
	for _, row := range rows {
		result[row.StoryID] = row.Decision
	}
	return result, nil
}

func composeGroundedMonthlyReview(selected []monthlyCandidate, start time.Time, timezone string) (string, string, datatypes.JSON, string, string, datatypes.JSON, string) {
	monthLabel := start.In(monthlyReviewLocation(timezone)).Format("January 2006")
	groups := map[string][]string{}
	for _, candidate := range selected {
		category := candidate.Category
		if category == "" {
			category = "general"
		}
		groups[category] = append(groups[category], candidate.Story.Label)
	}
	categories := make([]string, 0, len(groups))
	for category := range groups {
		categories = append(categories, category)
	}
	sort.Strings(categories)
	sections := make([]map[string]interface{}, 0, len(categories))
	sectionsAR := make([]map[string]interface{}, 0, len(categories))
	for _, category := range categories {
		sections = append(sections, map[string]interface{}{"title": category, "grounding": "selected archive story labels", "story_labels": groups[category]})
		sectionsAR = append(sectionsAR, map[string]interface{}{"title": category, "grounding": "عناوين القصص المؤرشفة المختارة", "story_labels": groups[category]})
	}
	headline := fmt.Sprintf("%s in Review", monthLabel)
	intro := fmt.Sprintf("%d verified stories, selected from preserved News evidence across %d sections.", len(selected), len(sections))
	headlineAR := "مراجعة شهرية موثقة"
	introAR := fmt.Sprintf("%d قصة موثقة اختيرت من الأدلة المحفوظة عبر %d أقسام.", len(selected), len(sectionsAR))
	raw, _ := json.Marshal(map[string]interface{}{"headline": headline, "introduction": intro, "sections": sections, "headline_ar": headlineAR, "introduction_ar": introAR, "sections_ar": sectionsAR})
	hash := sha256.Sum256(raw)
	return headline, intro, datatypes.JSON(rawJSON(sections)), headlineAR, introAR, datatypes.JSON(rawJSON(sectionsAR)), hex.EncodeToString(hash[:])
}

func rawJSON(value interface{}) []byte { raw, _ := json.Marshal(value); return raw }

func BuildMonthlyReview(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy, err := loadMonthlyReviewPolicy(db, principal.TenantID)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	config, err := decodeMonthlyReviewPolicy(policy)
	if err != nil {
		c.JSON(409, gin.H{"error": err.Error()})
		return
	}
	month, err := time.Parse("2006-01", c.Param("month"))
	if err != nil {
		c.JSON(400, gin.H{"error": "month must be YYYY-MM"})
		return
	}
	timezone := retentionNewsTimezone(db, principal.TenantID)
	start := monthlyStart(month, timezone)
	current := monthlyStart(time.Now(), timezone)
	if !start.Before(current) {
		c.JSON(409, gin.H{"error": "only a completed month can be archived"})
		return
	}
	end := start.AddDate(0, 1, 0)
	candidates, err := buildMonthlyCandidates(db, principal.TenantID, start, end)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	overrides, err := monthlyReviewOverrides(db, principal.TenantID, start)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	selected, qualified := selectMonthlyCandidates(scoreMonthlyCandidates(candidates), config, overrides)
	if len(selected) == 0 {
		c.JSON(409, gin.H{"error": "no qualified readable stories for this month"})
		return
	}
	now := time.Now().UTC()
	var archive models.NewsMonthArchive
	err = db.Transaction(func(tx *gorm.DB) error {
		var latest models.NewsMonthArchive
		revision := 1
		var previous *uint
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND month_start=?", principal.TenantID, start).Order("revision DESC").First(&latest).Error; err == nil {
			revision = latest.Revision + 1
			previous = &latest.ID
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		manifest := map[string]interface{}{"formula_version": monthlyReviewFormulaVersion, "policy_version": policy.Version, "month_start": start.Format("2006-01-02"), "qualified_count": qualified, "selected_ids": []string{}}
		for _, x := range selected {
			manifest["selected_ids"] = append(manifest["selected_ids"].([]string), x.Story.PublicID.String())
		}
		raw, _ := json.Marshal(manifest)
		sum := sha256.Sum256(raw)
		selectionHash := hex.EncodeToString(sum[:])
		headline, intro, sections, headlineAR, introAR, sectionsAR, compositionHash := composeGroundedMonthlyReview(selected, start, timezone)
		archive = models.NewsMonthArchive{TenantID: principal.TenantID, MonthStart: start, Timezone: timezone, Revision: revision, SupersedesID: previous, PolicyVersionID: policy.ID, State: "verified", LimitedCoverage: len(selected) < config.TargetMin, Headline: headline, Introduction: intro, Sections: sections, HeadlineAR: headlineAR, IntroductionAR: introAR, SectionsAR: sectionsAR, SelectionManifest: datatypes.JSON(raw), SelectionHash: selectionHash, CompositionHash: compositionHash, QualifiedCount: qualified, SelectedCount: len(selected), Verification: monthlyReviewJSON(map[string]interface{}{"grounded": true, "readback": "selected lead snapshots persisted", "formula_version": monthlyReviewFormulaVersion}), BuiltAt: now, VerifiedAt: &now}
		if err := tx.Create(&archive).Error; err != nil {
			return err
		}
		rows := make([]models.NewsMonthArchiveStory, 0, len(selected))
		for i, x := range selected {
			snapshot := monthlyReviewJSON(map[string]interface{}{"title": x.Lead.Title, "excerpt": x.Lead.Excerpt, "body_text": x.Lead.BodyText, "original_url": x.Lead.OriginalURL, "source_name": x.Lead.SourceName, "published_at": x.Lead.PublishedAt, "category": x.Story.Category, "summary": x.Story.Summary, "bullets": x.Story.Bullets, "member_count": x.Story.OriginalMemberCount, "source_count": x.Sources})
			rows = append(rows, models.NewsMonthArchiveStory{ArchiveID: archive.ID, Position: i + 1, Section: "The month in review", OriginalStoryID: x.Story.PublicID, LeadContentID: x.Lead.PublicID, Label: x.Story.Label, Snapshot: snapshot, ImportanceScore: x.Importance, EngagementScore: x.Engagement, FinalScore: x.Score, SelectionEvidence: monthlyReviewJSON(map[string]interface{}{"sources": x.Sources, "coverage_hours": x.CoverageHours, "coverage_days": x.CoverageDays, "bookmarks": x.Bookmarks, "likes": x.Likes, "shares": x.Shares, "comments": x.Comments, "meaningful_opens": x.Meaningful})})
		}
		if err := tx.Create(&rows).Error; err != nil {
			return err
		}
		var persisted int64
		if err := tx.Model(&models.NewsMonthArchiveStory{}).Where("archive_id = ?", archive.ID).Count(&persisted).Error; err != nil {
			return err
		}
		if persisted != int64(len(rows)) {
			return errors.New("archive story readback is incomplete")
		}
		rollups := make([]models.NewsEngagementMonthlyRollup, 0, len(candidates)*5)
		for _, candidate := range candidates {
			for _, signal := range []struct {
				name  string
				count int
			}{{"bookmark", candidate.Bookmarks}, {"share", candidate.Shares}, {"comment", candidate.Comments}, {"like", candidate.Likes}, {"meaningful_open", candidate.Meaningful}} {
				rollups = append(rollups, models.NewsEngagementMonthlyRollup{TenantID: principal.TenantID, MonthStart: start, StoryID: candidate.Story.PublicID, InteractionType: signal.name, TotalCount: signal.count, UniqueActorCount: signal.count, ImpressionCount: candidate.Impressions, ExcludedEventCount: candidate.ExcludedEvents, Evidence: monthlyReviewJSON(map[string]interface{}{"formula_version": monthlyReviewFormulaVersion, "deduplicated_events": candidate.DeduplicatedEvents})})
			}
		}
		if len(rollups) > 0 {
			if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "month_start"}, {Name: "story_id"}, {Name: "interaction_type"}}, DoUpdates: clause.AssignmentColumns([]string{"total_count", "unique_actor_count", "impression_count", "excluded_event_count", "evidence"})}).Create(&rollups).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		c.JSON(500, gin.H{"error": "build monthly review: " + err.Error()})
		return
	}
	retentionAudit(db, principal, "retention.monthly_review.build", archive.PublicID.String(), "success", map[string]interface{}{"month": start.Format("2006-01"), "revision": archive.Revision})
	c.JSON(201, gin.H{"data": archive})
}

// VerifyMonthlyReview is the only publication step. It re-reads the immutable
// revision before atomically advancing the public head, so build and verify
// never expose a partial archive.
func VerifyMonthlyReview(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	month, err := time.Parse("2006-01", c.Param("month"))
	if err != nil {
		c.JSON(400, gin.H{"error": "month must be YYYY-MM"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	timezone := retentionNewsTimezone(db, principal.TenantID)
	start := monthlyStart(month, timezone)
	now := time.Now().UTC()
	var archive models.NewsMonthArchive
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id = ? AND month_start = ? AND state = ?", principal.TenantID, start, "verified").Order("revision DESC").First(&archive).Error; err != nil {
			return err
		}
		var stories []models.NewsMonthArchiveStory
		if err := tx.Where("archive_id = ?", archive.ID).Order("position ASC").Find(&stories).Error; err != nil {
			return err
		}
		if len(stories) != archive.SelectedCount || len(stories) == 0 {
			return errors.New("archive readback count does not match selection")
		}
		for _, story := range stories {
			if story.LeadContentID == uuid.Nil || len(story.Snapshot) == 0 {
				return errors.New("archive has an unreadable selected lead")
			}
		}
		if err := tx.Model(&models.NewsMonthArchive{}).Where("id = ?", archive.ID).Updates(map[string]interface{}{"state": "finalized", "finalized_at": now, "finalized_by": principal.Email, "updated_at": now}).Error; err != nil {
			return err
		}
		return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "month_start"}}, DoUpdates: clause.AssignmentColumns([]string{"archive_id", "updated_at"})}).Create(&models.NewsMonthArchiveHead{TenantID: principal.TenantID, MonthStart: start, ArchiveID: archive.ID}).Error
	})
	if err != nil {
		c.JSON(409, gin.H{"error": "verify monthly review: " + err.Error()})
		return
	}
	archive.State = "finalized"
	archive.FinalizedAt = &now
	archive.FinalizedBy = principal.Email
	retentionAudit(db, principal, "retention.monthly_review.verify", archive.PublicID.String(), "success", map[string]interface{}{"month": start.Format("2006-01"), "revision": archive.Revision})
	c.JSON(200, gin.H{"data": archive})
}

func CreateMonthlyReviewOverride(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	month, err := time.Parse("2006-01", c.Param("month"))
	if err != nil {
		c.JSON(400, gin.H{"error": "month must be YYYY-MM"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	start := monthlyStart(month, retentionNewsTimezone(db, principal.TenantID))
	var request struct {
		StoryID  string `json:"story_id"`
		Decision string `json:"decision"`
		Reason   string `json:"reason"`
	}
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(400, gin.H{"error": "invalid override"})
		return
	}
	storyID, err := uuid.Parse(request.StoryID)
	if err != nil || (request.Decision != "include" && request.Decision != "exclude") || strings.TrimSpace(request.Reason) == "" {
		c.JSON(400, gin.H{"error": "story_id, include|exclude decision, and reason are required"})
		return
	}
	var story models.Story
	if err := db.Where("tenant_id=? AND public_id=? AND last_member_at >= ? AND last_member_at < ?", principal.TenantID, storyID, start, start.AddDate(0, 1, 0)).First(&story).Error; err != nil {
		c.JSON(404, gin.H{"error": "story is not eligible for that month"})
		return
	}
	override := models.MonthlyReviewStoryOverride{TenantID: principal.TenantID, MonthStart: start, StoryID: storyID, Decision: request.Decision, Reason: strings.TrimSpace(request.Reason), CreatedBy: principal.Email}
	if err := db.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "month_start"}, {Name: "story_id"}}, DoUpdates: clause.AssignmentColumns([]string{"decision", "reason", "created_by", "created_at"})}).Create(&override).Error; err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	retentionAudit(db, principal, "retention.monthly_review.override", storyID.String(), "success", map[string]interface{}{"month": start.Format("2006-01"), "decision": request.Decision})
	c.JSON(201, gin.H{"data": override})
}

func DeleteMonthlyReviewOverride(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(400, gin.H{"error": "invalid override id"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	result := db.Where("tenant_id=? AND public_id=?", principal.TenantID, id).Delete(&models.MonthlyReviewStoryOverride{})
	if result.Error != nil {
		c.JSON(500, gin.H{"error": result.Error.Error()})
		return
	}
	if result.RowsAffected == 0 {
		c.JSON(404, gin.H{"error": "override not found"})
		return
	}
	retentionAudit(db, principal, "retention.monthly_review.override.delete", id.String(), "success", nil)
	c.Status(204)
}

func GetMonthlyReviewPolicy(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	p, err := loadMonthlyReviewPolicy(c.MustGet("db").(*gorm.DB), principal.TenantID)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"data": p})
}
func ListMonthlyReviewPolicies(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	var policies []models.MonthlyReviewPolicyVersion
	db := c.MustGet("db").(*gorm.DB)
	if err := db.Where("tenant_id=?", principal.TenantID).Order("version DESC").Find(&policies).Error; err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"data": gin.H{"items": policies}})
}
func UpdateMonthlyReviewPolicy(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	current, err := loadMonthlyReviewPolicy(db, principal.TenantID)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	config, err := decodeMonthlyReviewPolicy(current)
	if err != nil {
		c.JSON(409, gin.H{"error": err.Error()})
		return
	}
	var req struct {
		Config monthlyReviewPolicyConfig `json:"config"`
		Reason string                    `json:"reason"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "invalid monthly review policy"})
		return
	}
	if req.Config.FormulaVersion == "" {
		req.Config = config
	}
	raw := monthlyReviewJSON(req.Config)
	next := models.MonthlyReviewPolicyVersion{TenantID: principal.TenantID, Version: current.Version + 1, State: "active", Config: raw, Reason: req.Reason, CreatedBy: principal.Email, PreviousID: &current.ID, EffectiveAt: time.Now().UTC()}
	if _, err := decodeMonthlyReviewPolicy(next); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&next).Error; err != nil {
			return err
		}
		return tx.Model(&models.MonthlyReviewPolicyHead{}).Where("tenant_id=?", principal.TenantID).Update("policy_id", next.ID).Error
	})
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	retentionAudit(db, principal, "retention.monthly_review.policy.create", next.PublicID.String(), "success", nil)
	c.JSON(201, gin.H{"data": next})
}

func ListMonthlyReviews(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	var rows []models.NewsMonthArchive
	db := c.MustGet("db").(*gorm.DB)
	if err := db.Where("tenant_id=?", principal.TenantID).Order("month_start DESC, revision DESC").Limit(60).Find(&rows).Error; err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"data": gin.H{"items": rows}})
}
func GetMonthlyReviewArchiveAdmin(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	month, err := time.Parse("2006-01", c.Param("month"))
	if err != nil {
		c.JSON(400, gin.H{"error": "month must be YYYY-MM"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	start := monthlyStart(month, retentionNewsTimezone(db, principal.TenantID))
	query := db.Where("tenant_id=? AND month_start=?", principal.TenantID, start).Order("revision DESC")
	if raw := c.Query("revision"); raw != "" {
		var revision int
		if _, err := fmt.Sscanf(raw, "%d", &revision); err != nil || revision < 1 {
			c.JSON(400, gin.H{"error": "invalid revision"})
			return
		}
		query = query.Where("revision=?", revision)
	}
	var archive models.NewsMonthArchive
	if err := query.First(&archive).Error; err != nil {
		c.JSON(404, gin.H{"error": "archive revision not found"})
		return
	}
	var stories []models.NewsMonthArchiveStory
	if err := db.Where("archive_id=?", archive.ID).Order("position ASC").Find(&stories).Error; err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"data": gin.H{"archive": archive, "stories": stories}})
}
func GetPublicMonthlyReview(c *gin.Context) {
	month, err := time.Parse("2006-01", c.Param("month"))
	if err != nil {
		c.JSON(400, gin.H{"error": "month must be YYYY-MM"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	start := time.Date(month.Year(), month.Month(), 1, 0, 0, 0, 0, time.UTC)
	var head models.NewsMonthArchiveHead
	if err := db.Where("tenant_id=? AND month_start=?", "default", start).First(&head).Error; err != nil {
		c.JSON(404, gin.H{"error": "month in review not found"})
		return
	}
	var archive models.NewsMonthArchive
	if err := db.Where("id=? AND state=?", head.ArchiveID, "finalized").First(&archive).Error; err != nil {
		c.JSON(404, gin.H{"error": "month in review not published"})
		return
	}
	var stories []models.NewsMonthArchiveStory
	db.Where("archive_id=?", archive.ID).Order("position ASC").Find(&stories)
	c.JSON(200, gin.H{"data": gin.H{"archive": archive, "stories": stories}})
}
