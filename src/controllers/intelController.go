package controllers

import (
	"content-management-system/src/models"
	"encoding/json"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/lib/pq"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var urlRegex = regexp.MustCompile(`https?://[^\s"'<>)\]]+`)

func intelTenant(c *gin.Context) string {
	t := strings.TrimSpace(c.Query("tenant_id"))
	if t == "" {
		return "default"
	}
	return t
}

func hostOf(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		return ""
	}
	return strings.ToLower(strings.TrimPrefix(u.Hostname(), "www."))
}

func loadSourceHosts(db *gorm.DB, tenantID string) map[string]struct{} {
	var feeds []string
	db.Model(&models.ContentSource{}).
		Where("tenant_id = ? AND feed_url IS NOT NULL AND feed_url != ''", tenantID).
		Pluck("feed_url", &feeds)
	set := make(map[string]struct{}, len(feeds))
	for _, f := range feeds {
		if h := hostOf(f); h != "" {
			set[h] = struct{}{}
		}
	}
	return set
}

// InternalGetCorpusCitations handles GET /internal/intel/corpus-citations —
// external domains referenced in your own recent content (body links), excluding
// domains you already track. The corpus signal for the source graph.
func InternalGetCorpusCitations(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	tenantID := intelTenant(c)
	existing := loadSourceHosts(db, tenantID)

	type itemRow struct {
		BodyText    *string
		PublishedAt *time.Time
	}
	var items []itemRow
	cutoff := time.Now().AddDate(0, 0, -60)
	db.Model(&models.ContentItem{}).
		Where("tenant_id = ? AND body_text IS NOT NULL AND body_text != ''", tenantID).
		Where("COALESCE(published_at, created_at) >= ?", cutoff).
		Select("body_text, published_at").
		Order("COALESCE(published_at, created_at) DESC").
		Limit(3000).Scan(&items)

	recentCutoff := time.Now().AddDate(0, 0, -14)
	type agg struct{ count, recent int }
	counts := map[string]*agg{}
	for _, it := range items {
		if it.BodyText == nil {
			continue
		}
		isRecent := it.PublishedAt != nil && it.PublishedAt.After(recentCutoff)
		seen := map[string]bool{} // count each domain once per item
		for _, m := range urlRegex.FindAllString(*it.BodyText, -1) {
			h := hostOf(m)
			if h == "" || seen[h] {
				continue
			}
			seen[h] = true
			if _, tracked := existing[h]; tracked {
				continue
			}
			a := counts[h]
			if a == nil {
				a = &agg{}
				counts[h] = a
			}
			a.count++
			if isRecent {
				a.recent++
			}
		}
	}

	out := make([]gin.H, 0, len(counts))
	for h, a := range counts {
		out = append(out, gin.H{"domain": h, "count": a.count, "recent_count": a.recent})
	}
	c.JSON(http.StatusOK, gin.H{"data": out})
}

// InternalGetApprovedSourcePages handles GET /internal/intel/approved-source-pages
// — the trusted set (news content_sources) the graph job crawls for outbound links.
func InternalGetApprovedSourcePages(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	tenantID := intelTenant(c)
	var sources []models.ContentSource
	// RSS/web sources only — Telegram (gramjs) + Twitter (syndication) are crawled
	// via their own graph contributors, not HTTP link extraction.
	db.Where("tenant_id = ? AND category = ? AND type NOT IN (?) AND feed_url IS NOT NULL",
		tenantID, models.SourceCategoryNews, []models.SourceType{models.SourceTypeTelegram, models.SourceTypeTwitter}).Find(&sources)

	out := make([]gin.H, 0, len(sources))
	seen := map[string]bool{}
	for _, s := range sources {
		if s.FeedURL == nil {
			continue
		}
		h := hostOf(*s.FeedURL)
		if h == "" || seen[h] {
			continue
		}
		seen[h] = true
		out = append(out, gin.H{"host": h, "site_url": "https://" + h, "feed_url": *s.FeedURL})
	}
	c.JSON(http.StatusOK, gin.H{"data": out})
}

// InternalGetApprovedTelegramChannels handles GET /internal/intel/approved-telegram-channels
// — the approved Telegram channels that seed the forward-graph.
func InternalGetApprovedTelegramChannels(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	tenantID := intelTenant(c)
	var sources []models.ContentSource
	db.Where("tenant_id = ? AND type = ? AND feed_url IS NOT NULL", tenantID, models.SourceTypeTelegram).Find(&sources)

	out := make([]gin.H, 0, len(sources))
	seen := map[string]bool{}
	for _, s := range sources {
		if s.FeedURL == nil {
			continue
		}
		u := telegramUsername(*s.FeedURL)
		if u == "" || seen[u] {
			continue
		}
		seen[u] = true
		out = append(out, gin.H{"username": u})
	}
	c.JSON(http.StatusOK, gin.H{"data": out})
}

// telegramUsername extracts the bare channel username from a t.me URL or @handle.
func telegramUsername(raw string) string {
	s := strings.TrimSpace(raw)
	s = strings.TrimPrefix(s, "https://")
	s = strings.TrimPrefix(s, "http://")
	s = strings.TrimPrefix(s, "t.me/")
	s = strings.TrimPrefix(s, "telegram.me/")
	s = strings.TrimPrefix(s, "s/")
	s = strings.TrimPrefix(s, "@")
	if i := strings.IndexAny(s, "/?#"); i >= 0 {
		s = s[:i]
	}
	return strings.ToLower(strings.TrimSpace(s))
}

// InternalGetApprovedTwitterHandles handles GET /internal/intel/approved-twitter-handles
// — the approved X accounts that seed the interaction-graph.
func InternalGetApprovedTwitterHandles(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	tenantID := intelTenant(c)
	var sources []models.ContentSource
	db.Where("tenant_id = ? AND type = ? AND feed_url IS NOT NULL", tenantID, models.SourceTypeTwitter).Find(&sources)

	out := make([]gin.H, 0, len(sources))
	seen := map[string]bool{}
	for _, s := range sources {
		if s.FeedURL == nil {
			continue
		}
		u := twitterHandle(*s.FeedURL)
		if u == "" || seen[u] {
			continue
		}
		seen[u] = true
		out = append(out, gin.H{"username": u})
	}
	c.JSON(http.StatusOK, gin.H{"data": out})
}

// twitterHandle extracts the bare handle from an x.com/twitter.com URL or @handle.
func twitterHandle(raw string) string {
	s := strings.TrimSpace(raw)
	s = strings.TrimPrefix(s, "https://")
	s = strings.TrimPrefix(s, "http://")
	s = strings.TrimPrefix(s, "www.")
	s = strings.TrimPrefix(s, "x.com/")
	s = strings.TrimPrefix(s, "twitter.com/")
	s = strings.TrimPrefix(s, "mobile.twitter.com/")
	s = strings.TrimPrefix(s, "@")
	if i := strings.IndexAny(s, "/?#"); i >= 0 {
		s = s[:i]
	}
	return strings.ToLower(strings.TrimSpace(s))
}

// ---------- Ledger upsert + auto-promotion ----------

type intelCandidateIn struct {
	Domain          string                   `json:"domain"`
	Kind            string                   `json:"kind"`
	CanonicalKey    string                   `json:"canonical_key"`
	ResolvedFeedURL *string                  `json:"resolved_feed_url"`
	FeedValid       bool                     `json:"feed_valid"`
	CitationCount   int                      `json:"citation_count"`
	CocitationCount int                      `json:"cocitation_count"`
	AuthorityScore  float64                  `json:"authority_score"`
	Trend           string                   `json:"trend"`
	DiscoveredVia   []string                 `json:"discovered_via"`
	SampleTitles    []map[string]interface{} `json:"sample_titles"`
	FeedHealth      map[string]interface{}   `json:"feed_health"`
}

type intelEdgeIn struct {
	FromHost string `json:"from_host"`
	ToHost   string `json:"to_host"`
	Weight   int    `json:"weight"`
}

type intelCandidatesReq struct {
	TenantID   string             `json:"tenant_id"`
	Candidates []intelCandidateIn `json:"candidates"`
	Edges      []intelEdgeIn      `json:"edges"`
}

// InternalUpsertCandidates handles POST /internal/intel/candidates — the graph
// job writes the ledger + edges here, then CMS auto-promotes high-scoring
// on-topic candidates into the review queue.
func InternalUpsertCandidates(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var req intelCandidatesReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}
	tenantID := strings.TrimSpace(req.TenantID)
	if tenantID == "" {
		tenantID = "default"
	}

	// Upsert ledger rows (signals refreshed; status preserved by NOT updating it).
	now := time.Now().UTC()
	rows := make([]models.SourceCandidate, 0, len(req.Candidates))
	for _, cand := range req.Candidates {
		domain := strings.ToLower(strings.TrimSpace(cand.Domain))
		if domain == "" {
			continue
		}
		kind := strings.ToLower(strings.TrimSpace(cand.Kind))
		if kind != models.CandidateKindTelegram && kind != models.CandidateKindTwitter {
			kind = models.CandidateKindRSS
		}
		row := models.SourceCandidate{
			TenantID:        tenantID,
			Domain:          domain,
			Kind:            kind,
			CanonicalKey:    cand.CanonicalKey,
			ResolvedFeedURL: cand.ResolvedFeedURL,
			FeedValid:       cand.FeedValid,
			CitationCount:   cand.CitationCount,
			CocitationCount: cand.CocitationCount,
			AuthorityScore:  cand.AuthorityScore,
			Trend:           defaultStr(cand.Trend, "flat"),
			DiscoveredVia:   pq.StringArray(cand.DiscoveredVia),
			LastSeenAt:      now,
		}
		if cand.FeedValid {
			row.LastResolvedAt = &now
		}
		if raw, err := json.Marshal(cand.SampleTitles); err == nil {
			row.SampleTitles = datatypes.JSON(raw)
		}
		if raw, err := json.Marshal(cand.FeedHealth); err == nil {
			row.FeedHealth = datatypes.JSON(raw)
		}
		rows = append(rows, row)
	}
	if len(rows) > 0 {
		_ = db.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "tenant_id"}, {Name: "domain"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"kind", "canonical_key", "resolved_feed_url", "feed_valid", "last_resolved_at",
				"citation_count", "cocitation_count", "authority_score", "trend",
				"discovered_via", "sample_titles", "feed_health", "last_seen_at",
			}),
		}).Create(&rows).Error
	}

	// Upsert edges.
	edges := make([]models.SourceEdge, 0, len(req.Edges))
	for _, e := range req.Edges {
		from := strings.ToLower(strings.TrimSpace(e.FromHost))
		to := strings.ToLower(strings.TrimSpace(e.ToHost))
		if from == "" || to == "" || from == to {
			continue
		}
		w := e.Weight
		if w < 1 {
			w = 1
		}
		edges = append(edges, models.SourceEdge{TenantID: tenantID, FromHost: from, ToHost: to, Weight: w})
	}
	if len(edges) > 0 {
		_ = db.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "tenant_id"}, {Name: "from_host"}, {Name: "to_host"}},
			DoUpdates: clause.AssignmentColumns([]string{"weight", "updated_at"}),
		}).Create(&edges).Error
	}

	promoted := promoteCandidatesForAllProfiles(db, tenantID)
	c.JSON(http.StatusOK, gin.H{"candidates": len(rows), "edges": len(edges), "promoted": promoted})
}

// InternalListCandidates handles GET /internal/intel/candidates — the ledger,
// e.g. for the console "top authorities" insight.
func InternalListCandidates(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	tenantID := intelTenant(c)
	var cands []models.SourceCandidate
	q := db.Where("tenant_id = ?", tenantID)
	if strings.EqualFold(c.Query("resolved"), "true") {
		q = q.Where("feed_valid = ?", true)
	}
	q.Order("authority_score desc, citation_count desc").Limit(50).Find(&cands)
	c.JSON(http.StatusOK, gin.H{"data": cands})
}

func defaultStr(s, fallback string) string {
	if strings.TrimSpace(s) == "" {
		return fallback
	}
	return s
}

// ---------- Composite scoring + promotion ----------

func normLog(v float64, cap float64) float64 {
	if v <= 0 {
		return 0
	}
	return math.Min(1, math.Log1p(v)/math.Log1p(cap))
}

func compositeScore(cfg models.DiscoveryConfig, citation, cocitation, authority, relevance, health, novelty float64) float64 {
	citN := normLog(citation, 20)
	cocN := normLog(cocitation, 10)
	relN := math.Min(1, relevance/0.25) // Arabic cosine peaks ~0.25
	return cfg.WeightCitation*citN + cfg.WeightCocitation*cocN + cfg.WeightAuthority*authority +
		cfg.WeightRelevance*relN + cfg.WeightHealth*health + cfg.WeightNovelty*novelty
}

func subscribersFromHealth(raw datatypes.JSON) int {
	if len(raw) == 0 {
		return 0
	}
	var h struct {
		Subscribers int `json:"subscribers"`
	}
	if err := json.Unmarshal(raw, &h); err != nil {
		return 0
	}
	return h.Subscribers
}

func healthScore(raw datatypes.JSON) float64 {
	if len(raw) == 0 {
		return 0.3
	}
	var h struct {
		ItemsCount int    `json:"items_count"`
		LastItemAt string `json:"last_item_at"`
	}
	if err := json.Unmarshal(raw, &h); err != nil {
		return 0.3
	}
	vol := math.Min(1, float64(h.ItemsCount)/20.0)
	rec := 0.5
	if t, err := time.Parse(time.RFC3339, h.LastItemAt); err == nil {
		ageDays := time.Since(t).Hours() / 24
		switch {
		case ageDays <= 2:
			rec = 1
		case ageDays <= 7:
			rec = 0.8
		case ageDays <= 30:
			rec = 0.5
		default:
			rec = 0.2
		}
	}
	return 0.5*vol + 0.5*rec
}

func promoteCandidatesForAllProfiles(db *gorm.DB, tenantID string) int {
	cfg := loadDiscoveryConfig(db, tenantID)
	if !cfg.IntelligenceEnabled {
		return 0
	}
	var profiles []models.DiscoveryProfile
	db.Where("tenant_id = ? AND enabled = ?", tenantID, true).Find(&profiles)

	var candidates []models.SourceCandidate
	db.Where("tenant_id = ? AND feed_valid = ? AND status NOT IN ?", tenantID, true, []string{models.CandidateStatusApproved, models.CandidateStatusRejected}).
		Order("authority_score desc").Limit(80).Find(&candidates)

	total := 0
	for i := range profiles {
		total += promoteForProfile(db, tenantID, &profiles[i], cfg, candidates)
	}
	return total
}

func promoteForProfile(db *gorm.DB, tenantID string, profile *models.DiscoveryProfile, cfg models.DiscoveryConfig, candidates []models.SourceCandidate) int {
	profileVec, ok := ensureProfileEmbedding(db, profile)
	if !ok {
		return 0
	}
	promoted := 0
	for ci := range candidates {
		cand := &candidates[ci]
		if cand.ResolvedFeedURL == nil {
			continue
		}
		titles := sampleTitleListFromCandidate(cand.SampleTitles)
		relevance := 0.0
		novelty := 1.0
		if len(titles) > 0 {
			if vecs, err := embedBatchViaEnrichment(titles); err == nil && len(vecs) > 0 {
				var sum float64
				n := 0
				for _, v := range vecs {
					if len(v) == len(profileVec) {
						sum += clamp01(dotProduct(profileVec, v))
						n++
					}
				}
				if n > 0 {
					relevance = sum / float64(n)
				}
				novelty = noveltyFactor(db, tenantID, meanVector(vecs), cfg.DupThreshold, cfg.DupPenalty)
			}
		}
		// Relevance gate: don't let a high-authority source flood interests it
		// isn't actually about (reuses the same floor the review UI hides below).
		if relevance < cfg.MinRelevance {
			continue
		}
		health := healthScore(cand.FeedHealth)
		composite := compositeScore(cfg, float64(cand.CitationCount), float64(cand.CocitationCount), cand.AuthorityScore, relevance, health, novelty)
		if composite < cfg.PromotionThreshold {
			continue
		}

		// Kind-aware: RSS → RSS feed suggestion; Telegram → TELEGRAM channel; Twitter
		// → TWITTER account. The matching fetcher ingests the resolved_feed_url on approve.
		sugType := models.SourceTypeRSS
		via := "graph"
		switch cand.Kind {
		case models.CandidateKindTelegram:
			sugType = models.SourceTypeTelegram
			via = "telegram-graph"
		case models.CandidateKindTwitter:
			sugType = models.SourceTypeTwitter
			via = "x-graph"
		}
		if len(cand.DiscoveredVia) > 0 {
			via = cand.DiscoveredVia[0]
		}

		ev := gin.H{
			"citation_count":   cand.CitationCount,
			"cocitation_count": cand.CocitationCount,
			"authority":        round2(cand.AuthorityScore),
			"relevance":        round2(relevance),
			"composite":        round2(composite),
			"trend":            cand.Trend,
			"via":              []string(cand.DiscoveredVia),
		}
		if subs := subscribersFromHealth(cand.FeedHealth); subs > 0 {
			ev["subscribers"] = subs
		}
		evidence, _ := json.Marshal(ev)

		rel := relevance
		sug := models.SourceSuggestion{
			TenantID:       tenantID,
			ProfileID:      &profile.ID,
			Name:           cand.Domain,
			Type:           sugType,
			FeedURL:        *cand.ResolvedFeedURL,
			CanonicalKey:   defaultStr(cand.CanonicalKey, *cand.ResolvedFeedURL),
			Confidence:     composite,
			RelevanceScore: &rel,
			SampleItems:    cand.SampleTitles,
			Health:         cand.FeedHealth,
			Evidence:       datatypes.JSON(evidence),
			DiscoveredVia:  via,
			Category:       models.SourceCategoryNews,
			Status:         models.SuggestionStatusPending,
		}
		err := db.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "tenant_id"}, {Name: "profile_id"}, {Name: "canonical_key"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"name", "type", "feed_url", "confidence", "relevance_score",
				"sample_items", "health", "evidence", "discovered_via", "category", "updated_at",
			}),
		}).Create(&sug).Error
		if err == nil {
			promoted++
			db.Model(&models.SourceCandidate{}).Where("id = ?", cand.ID).Update("status", models.CandidateStatusPromoted)
		}
	}
	return promoted
}

func sampleTitleListFromCandidate(raw datatypes.JSON) []string {
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

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}
