package controllers

import (
	"content-management-system/src/models"
	"encoding/json"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
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

// InternalGetApprovedYouTubeChannels handles GET /internal/intel/approved-youtube-channels
// — the approved YouTube channels that seed the media interaction graph (watch-next
// + featured). Returns a channel reference (handle / channelId / custom URL) that
// Enrichment's InnerTube extractor resolves.
func InternalGetApprovedYouTubeChannels(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	tenantID := intelTenant(c)
	var sources []models.ContentSource
	db.Where("tenant_id = ? AND type = ? AND feed_url IS NOT NULL", tenantID, models.SourceTypeYouTube).Find(&sources)

	out := make([]gin.H, 0, len(sources))
	seen := map[string]bool{}
	for _, s := range sources {
		if s.FeedURL == nil {
			continue
		}
		ref := youtubeChannelRef(*s.FeedURL)
		if ref == "" || seen[ref] {
			continue
		}
		seen[ref] = true
		out = append(out, gin.H{"channel": ref})
	}
	c.JSON(http.StatusOK, gin.H{"data": out})
}

// youtubeChannelRef extracts a stable channel reference from a YouTube URL,
// @handle, or raw channelId. Enrichment resolves any of these to a channel.
func youtubeChannelRef(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return ""
	}
	if u, err := url.Parse(s); err == nil && u.Host != "" {
		p := strings.Trim(u.Path, "/")
		switch {
		case strings.HasPrefix(p, "channel/"):
			return strings.TrimPrefix(p, "channel/")
		case strings.HasPrefix(p, "c/"):
			return strings.TrimPrefix(p, "c/")
		case strings.HasPrefix(p, "user/"):
			return strings.TrimPrefix(p, "user/")
		case strings.HasPrefix(p, "@"):
			return p // @handle
		case p != "":
			return p
		}
	}
	// Bare @handle or channelId.
	if i := strings.IndexAny(s, "/?#"); i >= 0 {
		s = s[:i]
	}
	return strings.TrimSpace(s)
}

// InternalGetApprovedPodcastFeeds handles GET /internal/intel/approved-podcast-feeds
// — the approved podcast RSS feeds that seed the media link-graph (show-notes
// outbound links + topical similarity).
func InternalGetApprovedPodcastFeeds(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	tenantID := intelTenant(c)
	var sources []models.ContentSource
	db.Where("tenant_id = ? AND type = ? AND feed_url IS NOT NULL", tenantID, models.SourceTypePodcast).Find(&sources)

	out := make([]gin.H, 0, len(sources))
	seen := map[string]bool{}
	for _, s := range sources {
		if s.FeedURL == nil {
			continue
		}
		feed := strings.TrimSpace(*s.FeedURL)
		if feed == "" || seen[feed] {
			continue
		}
		seen[feed] = true
		out = append(out, gin.H{"feed_url": feed})
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
		switch kind {
		case models.CandidateKindTelegram, models.CandidateKindTwitter,
			models.CandidateKindYouTube, models.CandidateKindPodcast:
			// recognized kind — keep as-is
		default:
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

// imageFromHealth pulls the profile avatar the discovery contributor stashed in
// feed_health (e.g. X account photo) so promotion can surface it on the
// suggestion without a dedicated candidate column.
func imageFromHealth(raw datatypes.JSON) string {
	if len(raw) == 0 {
		return ""
	}
	var h struct {
		Image string `json:"image"`
	}
	if err := json.Unmarshal(raw, &h); err != nil {
		return ""
	}
	return strings.TrimSpace(h.Image)
}

// ---------- source classification (deterministic, multi-signal) ----------
//
// Tags a discovered source OFFICIAL / NEWS / PERSON / OTHER so reviewers can scan
// and filter by type. No LLM — combines bio-URL TLD, an outlet allowlist (fixes
// keyword-less outlets like AlArabiya), and multilingual (AR/EN) keyword scoring
// with a tie-break of OFFICIAL > NEWS > PERSON. RSS feeds default to NEWS.

const (
	SourceClassOfficial = "official"
	SourceClassNews     = "news"
	SourceClassPerson   = "person"
	SourceClassOther    = "other"
)

// Major Arabic outlets whose bios lack obvious news keywords. Handle (lowercased).
var newsOutletAllowlist = map[string]bool{
	"alarabiya": true, "alarabiya_brk": true, "alarabiya_egy": true, "alhadath": true,
	"skynewsarabia": true, "skynewsarabia_b": true, "ajarabic": true, "ajabreaking": true,
	"ajmubasher": true, "bbcarabic": true, "rtarabic": true, "rtarabic_bn": true,
	"france24_ar": true, "almayadeennews": true, "akhbaar24": true, "okaznews": true,
	"aawsat": true, "cnbcarabia": true, "alhurranews": true, "aa_arabic": true,
	"youm7": true, "almasryalyoum": true, "alahram": true, "alqabas": true,
	"arabi21news": true, "rassdnewsn": true, "elwatannews": true, "trtarabi": true,
}

// Keyword-less official accounts — rulers/royals + state entities/regulators whose
// bios carry no gov keyword (so the LLM fallback / keyword pass would miss them).
var officialAllowlist = map[string]bool{
	"kingsalman": true, "mohamedbinzayed": true, "hhshkmohd": true, "hamdanmohammed": true,
	"abzayed": true, "saudivision2030": true, "tadawul": true, "pif_en": true, "pifsaudi": true,
	"sama_gov": true, "saudicma": true, "spagov": true, "absher": true,
}

var officialStrongKW = []string{
	"حكوم", "وزار", "أمانة", "امانة", "إمارة", "امارة", "ديوان", "رئاسة", "سفار",
	"government", "ministry", "minister", "municipal", "embassy", "governorate", "royal court",
}
var officialWeakKW = []string{"هيئة", "authority", "official account"}
var newsKW = []string{
	"أخبار", "اخبار", "خبر", "عاجل", "قناة", "صحيف", "جريدة", "وكالة", "أنباء", "انباء",
	"إعلام", "اعلام", "نيوز", "شبكة", "تلفز", "إذاع", "اذاع", "صحاف",
	"news", "media", "press", "newspaper", "agency", "broadcast", "journal",
}
var personKW = []string{
	"صحفي", "صحافي", "إعلامي", "اعلامي", "كاتب", "محلل", "مذيع", "رئيس تحرير", "مؤلف", "ناشط",
	"journalist", "writer", "columnist", "anchor", "analyst", "author", "presenter", "reporter", "editor",
}
var newsHandleTokens = []string{"news", "tv", "press", "akhbar", "sabq", "media"}

func countMatches(text string, kws []string) int {
	n := 0
	for _, k := range kws {
		if strings.Contains(text, k) {
			n++
		}
	}
	return n
}

func feedHealthStr(raw datatypes.JSON, key string) string {
	if len(raw) == 0 {
		return ""
	}
	var m map[string]any
	if json.Unmarshal(raw, &m) != nil {
		return ""
	}
	if s, ok := m[key].(string); ok {
		return s
	}
	return ""
}

// classifySource returns the source class for a discovered candidate.
func classifySource(cand *models.SourceCandidate) string {
	handle := strings.ToLower(strings.TrimSpace(cand.Domain))
	bio := feedHealthStr(cand.FeedHealth, "bio")
	url := strings.ToLower(feedHealthStr(cand.FeedHealth, "url"))
	titles := strings.Join(sampleTitleListFromCandidate(cand.SampleTitles), " ")
	text := strings.ToLower(handle + " " + bio + " " + titles)
	isRSS := cand.Kind != models.CandidateKindTwitter && cand.Kind != models.CandidateKindTelegram

	// 1. Authoritative: a government/military bio website (or RSS host).
	govProbe := url
	if isRSS {
		govProbe = handle
	}
	if strings.Contains(govProbe, ".gov") || strings.Contains(govProbe, "gov.") || strings.Contains(govProbe, ".mil") {
		return SourceClassOfficial
	}
	// 2. Allowlists for keyword-less accounts (official rulers/entities, major outlets).
	if officialAllowlist[handle] {
		return SourceClassOfficial
	}
	if newsOutletAllowlist[handle] {
		return SourceClassNews
	}

	// 3. Weighted keyword scoring.
	official := countMatches(text, officialStrongKW)*5 + countMatches(text, officialWeakKW)*2
	news := countMatches(text, newsKW) * 2
	person := countMatches(text, personKW) * 3
	for _, tok := range newsHandleTokens {
		if strings.Contains(handle, tok) {
			news += 2
			break
		}
	}

	switch {
	case official > 0 && official >= news && official >= person:
		return SourceClassOfficial
	case news > 0 && news >= person:
		return SourceClassNews
	case person > 0:
		return SourceClassPerson
	case isRSS:
		// RSS feeds in this pipeline are news sources by construction.
		return SourceClassNews
	default:
		return SourceClassOther
	}
}

// ClassifySuggestion runs the deterministic classifier over an existing
// suggestion's stored signals (handle/name, sample_items, health). Used to
// backfill source_class onto suggestions promoted before classification existed.
func ClassifySuggestion(s *models.SourceSuggestion) string {
	kind := ""
	switch s.Type {
	case models.SourceTypeTwitter:
		kind = models.CandidateKindTwitter
	case models.SourceTypeTelegram:
		kind = models.CandidateKindTelegram
	}
	return classifySource(&models.SourceCandidate{
		Domain:       strings.ToLower(strings.TrimSpace(s.Name)),
		Kind:         kind,
		SampleTitles: s.SampleItems,
		FeedHealth:   s.Health,
	})
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

	// Fetch the candidate pool PER category (news vs media) so a large, mature
	// news ledger can't starve media graph candidates out of the top-N by
	// authority (the two graphs have independent authority scales). Only pull a
	// category that actually has an enabled profile to score against.
	wantCat := map[string]bool{}
	for i := range profiles {
		c := strings.TrimSpace(profiles[i].Category)
		if c == "" {
			c = models.SourceCategoryNews
		}
		wantCat[c] = true
	}
	kindsByCat := map[string][]string{
		models.SourceCategoryNews:  {models.CandidateKindRSS, models.CandidateKindTelegram, models.CandidateKindTwitter},
		models.SourceCategoryMedia: {models.CandidateKindYouTube, models.CandidateKindPodcast},
	}
	var candidates []models.SourceCandidate
	for cat := range wantCat {
		kinds := kindsByCat[cat]
		if len(kinds) == 0 {
			continue
		}
		var part []models.SourceCandidate
		db.Where("tenant_id = ? AND feed_valid = ? AND status NOT IN ? AND kind IN ?",
			tenantID, true, []string{models.CandidateStatusApproved, models.CandidateStatusRejected}, kinds).
			Order("authority_score desc").Limit(80).Find(&part)
		candidates = append(candidates, part...)
	}

	// Classify each candidate ONCE (class is per-account, not per-profile).
	classMap, methodMap := classifyCandidates(candidates)

	total := 0
	for i := range profiles {
		total += promoteForProfile(db, tenantID, &profiles[i], cfg, candidates, classMap, methodMap)
	}
	return total
}

// classifyCandidates tags every candidate OFFICIAL/NEWS/PERSON/OTHER. The cheap
// deterministic pass runs for all; ambiguous ("other") X/Telegram accounts that
// carry a bio escalate, in one batch, to Enrichment's cached LLM classifier
// (on by default; set LLM_SOURCE_CLASSIFY_ENABLED=false to disable). Returns
// candID -> class and candID -> method ("rule" | "llm").
func classifyCandidates(candidates []models.SourceCandidate) (map[uint]string, map[uint]string) {
	classMap := make(map[uint]string, len(candidates))
	methodMap := make(map[uint]string, len(candidates))
	for i := range candidates {
		classMap[candidates[i].ID] = classifySource(&candidates[i])
		methodMap[candidates[i].ID] = "rule"
	}

	if strings.EqualFold(strings.TrimSpace(os.Getenv("LLM_SOURCE_CLASSIFY_ENABLED")), "false") {
		return classMap, methodMap
	}

	const maxLLM = 40
	var batch []accountToClassify
	var ids []uint
	for i := range candidates {
		if len(batch) >= maxLLM {
			break
		}
		c := &candidates[i]
		if classMap[c.ID] != SourceClassOther {
			continue
		}
		if c.Kind != models.CandidateKindTwitter && c.Kind != models.CandidateKindTelegram {
			continue // RSS has no bio; stays news-default.
		}
		bio := feedHealthStr(c.FeedHealth, "bio")
		name := ""
		if titles := sampleTitleListFromCandidate(c.SampleTitles); len(titles) > 0 {
			name = titles[0]
		}
		if strings.TrimSpace(bio) == "" && strings.TrimSpace(name) == "" {
			continue // nothing to reason over.
		}
		batch = append(batch, accountToClassify{Handle: c.Domain, Name: name, Bio: bio})
		ids = append(ids, c.ID)
	}
	if len(batch) == 0 {
		return classMap, methodMap
	}

	results, err := classifyAccountsViaEnrichment(batch)
	if err != nil {
		log.Printf("source classify: LLM fallback failed (keeping deterministic): %v", err)
		return classMap, methodMap
	}
	for j, acc := range batch {
		if cls, ok := results[strings.ToLower(strings.TrimSpace(acc.Handle))]; ok && cls != "" && cls != SourceClassOther {
			classMap[ids[j]] = cls
			methodMap[ids[j]] = "llm"
		}
	}
	return classMap, methodMap
}

func promoteForProfile(db *gorm.DB, tenantID string, profile *models.DiscoveryProfile, cfg models.DiscoveryConfig, candidates []models.SourceCandidate, classMap map[uint]string, methodMap map[uint]string) int {
	profileVec, ok := ensureProfileEmbedding(db, profile)
	if !ok {
		return 0
	}
	profileCategory := strings.TrimSpace(profile.Category)
	if profileCategory == "" {
		profileCategory = models.SourceCategoryNews
	}
	promoted := 0
	for ci := range candidates {
		cand := &candidates[ci]
		// Category-isolation: a profile only ever promotes candidates from its own
		// hub — news profiles match RSS/Telegram/X, media profiles match
		// YouTube/podcast. Keeps the two authority graphs from bleeding together.
		if models.CategoryForCandidateKind(cand.Kind) != profileCategory {
			continue
		}
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
		case models.CandidateKindYouTube:
			sugType = models.SourceTypeYouTube
			via = "youtube-graph"
		case models.CandidateKindPodcast:
			sugType = models.SourceTypePodcast
			via = "podcast-graph"
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
		if cls, ok := classMap[cand.ID]; ok {
			ev["source_class"] = cls
			ev["source_class_method"] = methodMap[cand.ID]
		} else {
			ev["source_class"] = classifySource(cand)
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
			Category:       profileCategory,
			Status:         models.SuggestionStatusPending,
		}
		if img := imageFromHealth(cand.FeedHealth); img != "" {
			sug.ImageURL = &img
		}
		err := db.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "tenant_id"}, {Name: "profile_id"}, {Name: "canonical_key"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"name", "type", "feed_url", "confidence", "relevance_score",
				"sample_items", "health", "evidence", "discovered_via", "category", "image_url", "updated_at",
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
