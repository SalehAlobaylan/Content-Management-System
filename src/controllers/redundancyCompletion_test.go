package controllers

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"content-management-system/src/feedcontract"
	"content-management-system/src/models"
	"content-management-system/src/tests/testdb"
	"content-management-system/src/utils"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/pgvector/pgvector-go"
	"gorm.io/gorm"
)

func TestRedundancyEvidenceRules(t *testing.T) {
	p := models.DefaultRedundancyPolicy("tenant-a")
	for _, test := range []struct {
		name     string
		lanes    map[string]redundancyLane
		expected string
	}{
		{"identical recording", map[string]redundancyLane{"duration": {1, true, ""}, "title": {.6, true, ""}, "transcript": {.91, true, ""}}, models.RedundancyVerdictHighConfidence},
		{"shared show art and title with different body", map[string]redundancyLane{"duration": {1, true, ""}, "title": {.95, true, ""}, "transcript": {.1, true, ""}, "thumbnail": {1, true, ""}, "embedding": {.99, true, ""}}, models.RedundancyVerdictClear},
		{"title and artwork alone", map[string]redundancyLane{"duration": {1, true, ""}, "title": {1, true, ""}, "thumbnail": {1, true, ""}}, models.RedundancyVerdictClear},
		{"wrong duration", map[string]redundancyLane{"duration": {0, true, ""}, "transcript": {1, true, ""}}, models.RedundancyVerdictClear},
	} {
		t.Run(test.name, func(t *testing.T) {
			score, verdict := redundancyVerdict(test.lanes, p)
			if verdict != test.expected || score < 0 || score > 1 {
				t.Fatalf("%s %.3f", verdict, score)
			}
		})
	}
	if got := titleSimilarity("host", "host host host"); got > 1 {
		t.Fatal("repeated tokens inflate confidence", got)
	}
	body := strings.Repeat("different important body words scientific conversation evidence eighth ninth tenth ", 30)
	transcript := models.Transcript{FullText: "ignored full text", Segments: redundancyJSON([]map[string]interface{}{{"start": 0, "end": 90, "text": "SHARED INTRO"}, {"start": 90, "end": 400, "text": body}, {"start": 400, "end": 490, "text": "SHARED OUTRO"}})}
	text := redundancyTranscriptBody(transcript)
	if strings.Contains(text, "intro") || strings.Contains(text, "outro") || !strings.Contains(text, "scientific") {
		t.Fatal("timestamp window not honored", text)
	}
	a, b := redundancyMinHash(text), redundancyMinHash(text)
	if redundancySignatureSimilarity(a, b) != 1 {
		t.Fatal("signature not deterministic")
	}
}
func redundancyFixture(t *testing.T) *gorm.DB {
	t.Helper()
	if os.Getenv("CMS_TEST_ADMIN_URL") == "" && os.Getenv("CMS_TEST_DATABASE_URL") == "" {
		t.Skip("guarded disposable PostgreSQL required")
	}
	db := testdb.Open(t)
	for _, sql := range []string{"CREATE EXTENSION IF NOT EXISTS vector", "CREATE EXTENSION IF NOT EXISTS pgcrypto"} {
		if err := db.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.AutoMigrate(&models.ContentItem{}, &models.Transcript{}, &models.UserInteraction{}, &models.MediaIntelligenceScore{}, &models.MediaCirculationRecommendation{}, &models.ContentSource{}, &models.MediaCirculationOverride{}, &models.StoragePolicy{}, &models.ContentFlag{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&models.StoragePolicy{}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&models.StoragePolicy{}).Where("tenant_id IS NULL").Update("protect_top_n_by_views", 0).Error; err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"20260712010000_redundancy_hygiene.sql", "20261019010000_redundancy_hygiene_completion.sql"} {
		sql, err := os.ReadFile(filepath.Join("../../migrations", name))
		if err != nil {
			t.Fatal(err)
		}
		if err = db.Exec(string(sql)).Error; err != nil {
			t.Fatal(err)
		}
	}
	descriptorMu.Lock()
	old := descriptorCache
	descriptorCache = map[string]expectedSpace{EmbeddingSpaceText: {SpaceID: "test-text", ObservedAt: time.Now()}, EmbeddingSpaceImage: {SpaceID: "test-image", ObservedAt: time.Now()}}
	descriptorMu.Unlock()
	t.Cleanup(func() { descriptorMu.Lock(); descriptorCache = old; descriptorMu.Unlock() })
	return db
}
func seedRedundancyItem(t *testing.T, db *gorm.DB, tenant string, duration int, old bool) models.ContentItem {
	t.Helper()
	title, url := "Episode about science", "https://fixture.test/"+uuid.NewString()
	at := time.Now().UTC().Add(-time.Minute)
	if old {
		at = at.Add(-24 * time.Hour)
	}
	item := models.ContentItem{TenantID: tenant, Type: models.ContentTypeVideo, Status: models.ContentStatusReady, Title: &title, DurationSec: &duration, PlaybackURL: &url, MediaURL: &url, OriginalURL: &url, IsFeedUnit: true, FeedVisibility: "visible", FileSizeBytes: 600 << 20, CreatedAt: at, UpdatedAt: at}
	if err := db.Create(&item).Error; err != nil {
		t.Fatal(err)
	}
	transcript := models.Transcript{ContentItemID: item.PublicID, FullText: strings.Repeat("science conversation research evidence telescope galaxy scientist observation results analysis ", 100), CreatedAt: at}
	if err := db.Create(&transcript).Error; err != nil {
		t.Fatal(err)
	}
	item.TranscriptID = &transcript.PublicID
	if err := db.Model(&item).UpdateColumns(map[string]interface{}{"transcript_id": transcript.PublicID, "updated_at": at}).Error; err != nil {
		t.Fatal(err)
	}
	return item
}
func redundancyHTTP(db *gorm.DB, handler gin.HandlerFunc, id uuid.UUID, body string, query string) *httptest.ResponseRecorder {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/test"+query, strings.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Set("db", db)
	c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: "default", Email: "reviewer@fixture.test", Role: "admin"})
	c.Params = gin.Params{{Key: "id", Value: id.String()}}
	handler(c)
	return rec
}
func TestRedundancyDBStatusWithoutProposals(t *testing.T) {
	db := redundancyFixture(t)
	response := redundancyHTTP(db, GetRedundancyStatus, uuid.Nil, "", "")
	if response.Code != http.StatusOK {
		t.Fatal(response.Code, response.Body.String())
	}
	var status struct {
		OpenProposals int        `json:"open_proposals"`
		Oldest        *time.Time `json:"oldest_proposal_at"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil || status.OpenProposals != 0 || status.Oldest != nil {
		t.Fatal(status, err)
	}
	a := seedRedundancyItem(t, db, "default", 600, true)
	b := seedRedundancyItem(t, db, "default", 602, false)
	if a.PublicID.String() > b.PublicID.String() {
		a, b = b, a
	}
	created := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	pair := models.RedundancyPair{TenantID: "default", ItemAID: a.PublicID, ItemBID: b.PublicID, Verdict: models.RedundancyVerdictProbable, CreatedAt: created}
	if err := db.Create(&pair).Error; err != nil {
		t.Fatal(err)
	}
	response = redundancyHTTP(db, GetRedundancyStatus, uuid.Nil, "", "")
	if response.Code != http.StatusOK {
		t.Fatal(response.Code, response.Body.String())
	}
	if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil || status.OpenProposals != 1 || status.Oldest == nil || !status.Oldest.Equal(created) {
		t.Fatal(status, err)
	}
}
func TestRedundancyDBLegacySchemaForwardUpgrade(t *testing.T) {
	db := redundancyFixture(t)
	// The deployed July schema lacks these columns even though its original
	// migration is already recorded. Only the new forward migration may repair it.
	for _, statement := range []string{
		"ALTER TABLE redundancy_policies DROP COLUMN emit_circulation_recs, DROP COLUMN confirm_rules, DROP COLUMN lane_settings, DROP COLUMN scan_state",
		"ALTER TABLE redundancy_runs DROP COLUMN error_class",
		"ALTER TABLE redundancy_fingerprints DROP COLUMN signature",
	} {
		if err := db.Exec(statement).Error; err != nil {
			t.Fatal(err)
		}
	}
	migration, err := os.ReadFile("../../migrations/20261019010000_redundancy_hygiene_completion.sql")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(string(migration)).Error; err != nil {
		t.Fatal(err)
	}
	if response := redundancyHTTP(db, GetRedundancyStatus, uuid.Nil, "", ""); response.Code != http.StatusOK {
		t.Fatal(response.Code, response.Body.String())
	}
	if response := redundancyHTTP(db, UpdateRedundancyPolicy, uuid.Nil, `{"emit_circulation_recs":true}`, ""); response.Code != http.StatusOK {
		t.Fatal(response.Code, response.Body.String())
	}
	if run, err := runRedundancyTenant(db, "default", "manual", "test", "full"); err != nil || run.Status != "completed" {
		t.Fatal(run, err)
	}
}
func TestRedundancyDBPairReviewPagination(t *testing.T) {
	db := redundancyFixture(t)
	assertPage := func(query string, total, size int, expectedID uuid.UUID) {
		t.Helper()
		response := redundancyHTTP(db, ListRedundancyPairs, uuid.Nil, "", query)
		if response.Code != http.StatusOK {
			t.Fatal(response.Code, response.Body.String())
		}
		var page struct {
			Data  []struct{ ID uuid.UUID } `json:"data"`
			Total int                      `json:"total"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
			t.Fatal(err)
		}
		if page.Total != total || len(page.Data) != size || size > 0 && page.Data[0].ID != expectedID {
			t.Fatal(response.Body.String())
		}
	}
	assertPage("", 0, 0, uuid.Nil)
	a := seedRedundancyItem(t, db, "default", 600, true)
	b := seedRedundancyItem(t, db, "default", 602, false)
	if a.PublicID.String() > b.PublicID.String() {
		a, b = b, a
	}
	pair := models.RedundancyPair{TenantID: "default", ItemAID: a.PublicID, ItemBID: b.PublicID, Confidence: .9, Verdict: models.RedundancyVerdictProbable}
	if err := db.Create(&pair).Error; err != nil {
		t.Fatal(err)
	}
	assertPage("?limit=1", 1, 1, pair.PublicID)
	assertPage("?limit=1&offset=1", 1, 0, uuid.Nil)
	assertPage("?verdict=all&item="+a.PublicID.String(), 1, 1, pair.PublicID)
	assertPage("?verdict="+models.RedundancyVerdictRejected, 0, 0, uuid.Nil)
}
func TestRedundancyDBRunGuardTransactionLifetime(t *testing.T) {
	db := redundancyFixture(t)
	first, err := acquireRedundancyRunGuard(db, "default")
	if err != nil {
		t.Fatal(err)
	}
	defer first.release()
	if _, err := acquireRedundancyRunGuard(db, "default"); !errors.Is(err, errRedundancyBusy) {
		t.Fatal("a second replica entered the same tenant's run", err)
	}
	other, err := acquireRedundancyRunGuard(db, "another-tenant")
	if err != nil {
		t.Fatal("unrelated tenant was blocked", err)
	}
	other.release()
	// Returning an ordinary query connection to the pool must not release
	// the guard transaction or let another replica begin this tenant's run.
	if err := db.Exec("SELECT 1").Error; err != nil {
		t.Fatal(err)
	}
	if err := first.check(); err != nil {
		t.Fatal(err)
	}
	if _, err := acquireRedundancyRunGuard(db, "default"); !errors.Is(err, errRedundancyBusy) {
		t.Fatal("guard outlived its physical backend pin", err)
	}
	first.release()
	if err := first.check(); err == nil {
		t.Fatal("released guard still authorizes checkpoint advancement")
	}
	next, err := acquireRedundancyRunGuard(db, "default")
	if err != nil {
		t.Fatal("the completed run stranded a lock", err)
	}
	next.release()
}
func TestRedundancyDBLibraryLifecycleAndConsequences(t *testing.T) {
	db := redundancyFixture(t)
	anchor := seedRedundancyItem(t, db, "default", 600, true)
	copy := seedRedundancyItem(t, db, "default", 602, false)
	past := time.Now().UTC().Add(-time.Hour)
	if err := db.Model(&models.RedundancyPolicy{}).Where("tenant_id='default'").Updates(map[string]interface{}{"enabled": true, "last_swept_at": past, "max_frontier_items": 1, "max_pairs_scored": 1}).Error; err != nil {
		t.Fatal(err)
	}
	checksum := func() string {
		var out string
		if e := db.Raw("SELECT md5(COALESCE(jsonb_agg(to_jsonb(c) ORDER BY public_id)::text,'')) FROM content_items c").Scan(&out).Error; e != nil {
			t.Fatal(e)
		}
		return out
	}
	beforeScan := checksum()
	first, err := runRedundancyTenant(db, "default", "manual", "test", "frontier")
	if err != nil || first.Status != "completed" {
		t.Fatal(first, err)
	}
	if checksum() != beforeScan {
		t.Fatal("proposal scan mutated content outside redundancy ownership")
	}
	var pair models.RedundancyPair
	if err := db.First(&pair).Error; err != nil {
		t.Fatal(err)
	}
	if pair.Verdict != models.RedundancyVerdictHighConfidence || pair.PublicID == uuid.Nil {
		t.Fatal(pair)
	}
	// Machine output must not alter feed visibility or form a family.
	var n int64
	db.Model(&models.RedundancyFamily{}).Count(&n)
	if n != 0 {
		t.Fatal("machine activated a family")
	}
	before := redundancyHTTP(db, GetRedundancyStatus, uuid.Nil, "", "")
	if before.Code != 200 || !strings.Contains(before.Body.String(), `"collapse_enabled":true`) {
		t.Fatal(before.Code, before.Body.String())
	}
	second, err := runRedundancyTenant(db, "default", "manual", "test", "full")
	if err != nil || second.PublicID == first.PublicID || second.PublicID == uuid.Nil {
		t.Fatal(second, err)
	}
	if err := confirmRedundancyPair(db, "default", "test", pair.PublicID, *pair.LatestEvaluationID); err != nil {
		t.Fatal(err)
	}
	var family models.RedundancyFamily
	if err := db.First(&family).Error; err != nil {
		t.Fatal(err)
	}
	var visible []models.ContentItem
	if err := feedcontract.PodsEligibleMediaQuery(db, "default", true).Find(&visible).Error; err != nil {
		t.Fatal(err)
	}
	if len(visible) != 1 {
		t.Fatalf("collapse returned %d", len(visible))
	}
	snapshot := []PodsItem{{ID: anchor.PublicID}, {ID: copy.PublicID}}
	frozen, _ := visibleFrozenPodsPage(db, "default", snapshot, 0, 20)
	if len(frozen) != 1 {
		t.Fatalf("frozen session bypassed collapse: %d", len(frozen))
	}
	if err := db.Model(&models.RedundancyPolicy{}).Where("tenant_id='default'").Update("emit_circulation_recs", true).Error; err != nil {
		t.Fatal(err)
	}
	if err := reconcileRedundancyRecommendations(db, "default", "test", nil); err != nil {
		t.Fatal(err)
	}
	if err := persistRecommendationsForUnit(db, "default", models.MediaCirculationUnitItemFamily, nil); err != nil {
		t.Fatal(err)
	}
	db.Model(&models.MediaCirculationRecommendation{}).Where("status='pending'").Count(&n)
	if n != 2 {
		t.Fatal("Circulation discarded hygiene recommendations", n)
	}
	var rec models.MediaCirculationRecommendation
	db.First(&rec)
	if err := validateRedundancyRecommendation(db, "default", rec); err != nil {
		t.Fatal(err)
	}
	if response := redundancyHTTP(db, DissolveRedundancyFamily, family.PublicID, `{"reason":"different editions confirmed on review"}`, ""); response.Code != 200 {
		t.Fatal(response.Body.String())
	}
	if err := validateRedundancyRecommendation(db, "default", rec); err == nil {
		t.Fatal("stale consequence survived dissolution")
	}
	if err := confirmRedundancyPair(db, "default", "test", pair.PublicID, *pair.LatestEvaluationID); err == nil {
		t.Fatal("tombstone was reconfirmed")
	}
	db.First(&pair, pair.ID)
	if !pair.Tombstoned || pair.Verdict != models.RedundancyVerdictRejected {
		t.Fatal(pair)
	}
	var after []models.ContentItem
	if err := feedcontract.PodsEligibleMediaQuery(db, "default", true).Find(&after).Error; err != nil || len(after) != 2 {
		t.Fatal("dissolution did not restore visibility", len(after), err)
	}
	var actions []models.RedundancyAction
	if err := db.Find(&actions).Error; err != nil || len(actions) < 5 {
		t.Fatal("incomplete action history", len(actions), err)
	}
	seen := map[uuid.UUID]bool{}
	for _, a := range actions {
		if a.PublicID == uuid.Nil || seen[a.PublicID] {
			t.Fatal("nonunique action ID")
		}
		seen[a.PublicID] = true
	}
}
func TestRedundancyDBBackfillCapsTenantAndEvidence(t *testing.T) {
	db := redundancyFixture(t)
	a := seedRedundancyItem(t, db, "default", 700, true)
	b := seedRedundancyItem(t, db, "default", 701, true)
	c := seedRedundancyItem(t, db, "default", 702, true)
	if err := db.Model(&models.RedundancyPolicy{}).Where("tenant_id='default'").Updates(map[string]interface{}{"enabled": true, "max_frontier_items": 1, "max_pairs_scored": 1}).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		run, err := runRedundancyTenant(db, "default", "manual", "test", "full")
		if err != nil || run.Status != "completed" {
			t.Fatal(run, err)
		}
		var counts map[string]int
		_ = json.Unmarshal(run.Counts, &counts)
		if counts["pairs_scored"] > 1 {
			t.Fatal("cap exceeded")
		}
		p, _ := redundancyPolicyFor(db, "default")
		if p.LastSweptAt != nil {
			break
		}
		if i == 9 {
			t.Fatal("backfill made no progress")
		}
	}
	var pairs []models.RedundancyPair
	if err := db.Find(&pairs).Error; err != nil || len(pairs) != 3 {
		t.Fatal("backfill missed pairs", len(pairs), err)
	}
	pair := pairs[0]
	if response := redundancyHTTP(db, RejectRedundancyPair, pair.PublicID, `{"reason":"different episodes"}`, ""); response.Code != 200 {
		t.Fatal(response.Body.String())
	}
	if _, err := runRedundancyTenant(db, "default", "manual", "test", "full"); err != nil {
		t.Fatal(err)
	}
	db.First(&pair, pair.ID)
	if pair.Verdict != models.RedundancyVerdictRejected {
		t.Fatal("machine overwrote rejection")
	}
	// Editing transcript evidence invalidates approval even if the title is unchanged.
	target := pairs[1]
	transcript := models.Transcript{ContentItemID: target.ItemAID, FullText: "changed provider transcript content", CreatedAt: time.Now().UTC()}
	if err := db.Create(&transcript).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&models.ContentItem{}).Where("public_id=?", target.ItemAID).Update("transcript_id", transcript.PublicID).Error; err != nil {
		t.Fatal(err)
	}
	if err := confirmRedundancyPair(db, "default", "test", target.PublicID, *target.LatestEvaluationID); err == nil {
		t.Fatal("stale evidence accepted")
	}
	// The precheck reads only the requested tenant and never grants lifecycle completion.
	payload := fmt.Sprintf(`{"tenant_id":"tenant-b","candidates":[{"title":"Episode about science","source_url":%q}]}`, *a.OriginalURL)
	response := redundancyHTTP(db, InternalRedundancyPrecheck, uuid.Nil, payload, "")
	if response.Code != 200 || strings.Contains(response.Body.String(), "exact_identity") {
		t.Fatal("cross-tenant identity leaked", response.Body.String())
	}
	// Space mismatch is explicit absent evidence, not a numeric similarity.
	v := pgvector.NewVector(make([]float32, 1024))
	a.Embedding = &v
	b.Embedding = &v
	space, other := "test-text", "other-space"
	a.EmbeddingSpaceID = &space
	b.EmbeddingSpaceID = &other
	lanes, _, err := redundancyLanes(db, "default", a, b, map[uuid.UUID]models.RedundancyFingerprint{})
	if err != nil || lanes["embedding"].Available || lanes["embedding"].Reason != "space_mismatch" {
		t.Fatal(lanes, err)
	}
	_ = c
}
func TestRedundancyDBMergeHasOneCanonicalAndLockSurvives(t *testing.T) {
	db := redundancyFixture(t)
	items := []models.ContentItem{}
	for i := 0; i < 4; i++ {
		items = append(items, seedRedundancyItem(t, db, "default", 900+i, true))
	}
	if _, err := runRedundancyTenant(db, "default", "manual", "test", "full"); err != nil {
		t.Fatal(err)
	}
	find := func(a, b uuid.UUID) models.RedundancyPair {
		if a.String() > b.String() {
			a, b = b, a
		}
		var pair models.RedundancyPair
		if err := db.Where("item_a_id=? AND item_b_id=?", a, b).First(&pair).Error; err != nil {
			t.Fatal(err)
		}
		return pair
	}
	confirm := func(a, b uuid.UUID) {
		pair := find(a, b)
		if err := confirmRedundancyPair(db, "default", "test", pair.PublicID, *pair.LatestEvaluationID); err != nil {
			t.Fatal(err)
		}
	}
	confirm(items[0].PublicID, items[1].PublicID)
	confirm(items[2].PublicID, items[3].PublicID)
	var family models.RedundancyFamily
	db.First(&family)
	response := redundancyHTTP(db, SetRedundancyCanonical, family.PublicID, fmt.Sprintf(`{"content_item_id":%q}`, family.CanonicalContentItemID), "")
	if response.Code != 200 {
		t.Fatal(response.Body.String())
	}
	confirm(items[1].PublicID, items[2].PublicID)
	var n int64
	db.Model(&models.RedundancyFamily{}).Where("status='active'").Count(&n)
	if n != 1 {
		t.Fatal("union did not reconcile", n)
	}
	db.Model(&models.RedundancyFamilyMember{}).Where("ended_at IS NULL AND role='canonical'").Count(&n)
	if n != 1 {
		t.Fatal("more than one canonical", n)
	}
	var after models.RedundancyFamily
	db.Where("status='active'").First(&after)
	if after.CanonicalContentItemID != family.CanonicalContentItemID || after.CanonicalLockedBy == "" {
		t.Fatal("canonical lock lost")
	}
	if response := redundancyHTTP(db, DissolveRedundancyFamily, after.PublicID, `{"reason":"incorrect recording family"}`, ""); response.Code != 200 {
		t.Fatal(response.Body.String())
	}
	db.Model(&models.RedundancyPair{}).Where("tombstoned=true").Count(&n)
	if n != 6 {
		t.Fatal("unconfirmed within-family proposals survived dissolution", n)
	}

}

func TestRedundancyDBPauseAndFailedWritesPreserveCheckpoint(t *testing.T) {
	t.Run("pause during scan survives completion", func(t *testing.T) {
		db := redundancyFixture(t)
		for i := 0; i < 3; i++ {
			seedRedundancyItem(t, db, "default", 1000+i, true)
		}
		if err := db.Model(&models.RedundancyPolicy{}).Where("tenant_id='default'").Updates(map[string]interface{}{"enabled": true}).Error; err != nil {
			t.Fatal(err)
		}
		paused := false
		if err := db.Callback().Create().Before("gorm:create").Register("test:pause_redundancy", func(tx *gorm.DB) {
			if _, ok := tx.Statement.Dest.(*models.RedundancyPairEvaluation); ok && !paused {
				paused = true
				until := time.Now().Add(time.Hour)
				if e := db.Model(&models.RedundancyPolicy{}).Where("tenant_id='default'").Update("paused_until", until).Error; e != nil {
					tx.AddError(e)
				}
			}
		}); err != nil {
			t.Fatal(err)
		}
		run, err := runRedundancyTenant(db, "default", "manual", "test", "full")
		if err != nil || run.Status != "paused" {
			t.Fatal(run, err)
		}
		p, err := redundancyPolicyFor(db, "default")
		if err != nil || p.PausedUntil == nil || !p.PausedUntil.After(time.Now()) || p.LastSweptAt != nil {
			t.Fatal("pause/checkpoint was overwritten", p, err)
		}
	})
	t.Run("evaluation write failure never reports completed", func(t *testing.T) {
		db := redundancyFixture(t)
		seedRedundancyItem(t, db, "default", 1100, true)
		seedRedundancyItem(t, db, "default", 1101, true)
		if err := db.Callback().Create().Before("gorm:create").Register("test:fail_redundancy", func(tx *gorm.DB) {
			if _, ok := tx.Statement.Dest.(*models.RedundancyPairEvaluation); ok {
				tx.AddError(fmt.Errorf("fixture rejected evaluation"))
			}
		}); err != nil {
			t.Fatal(err)
		}
		run, err := runRedundancyTenant(db, "default", "manual", "test", "full")
		if err != nil || run.Status != "failed" {
			t.Fatal(run, err)
		}
		p, err := redundancyPolicyFor(db, "default")
		if err != nil || p.LastSweptAt != nil {
			t.Fatal("failed work advanced watermark", p, err)
		}
		if p.ScanState == nil || string(p.ScanState) == "{}" {
			t.Fatal("failed work cannot be resumed")
		}
	})
}

func TestRedundancyDBCanonicalMigrationChain(t *testing.T) {
	if os.Getenv("CMS_TEST_ADMIN_URL") == "" && os.Getenv("CMS_TEST_DATABASE_URL") == "" {
		t.Skip("guarded disposable PostgreSQL required")
	}
	db := feedIntegrityTestDB(t)
	p, err := redundancyPolicyFor(db, "default")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&p).Update("enabled", true).Error; err != nil {
		t.Fatal(err)
	}
	if !db.Migrator().HasColumn(&models.RedundancyFingerprint{}, "signature") {
		t.Fatal("canonical migration omitted signatures")
	}
	if run, err := runRedundancyTenant(db, "default", "manual", "test", "full"); err != nil || run.Status != "completed" {
		t.Fatal(run, err)
	}
}
