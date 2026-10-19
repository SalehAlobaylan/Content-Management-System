package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"content-management-system/src/models"
	"github.com/google/uuid"
	"github.com/pgvector/pgvector-go"
	"gorm.io/gorm"
)

func reviewPair(t *testing.T, db *gorm.DB, a, b models.ContentItem) models.RedundancyPair {
	t.Helper()
	if err := db.First(&a, a.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.First(&b, b.ID).Error; err != nil {
		t.Fatal(err)
	}
	p, err := redundancyPolicyFor(db, "default")
	if err != nil {
		t.Fatal(err)
	}
	run := models.RedundancyRun{TenantID: "default", Status: "running", Trigger: "manual", StartedAt: time.Now().UTC()}
	if err = db.Create(&run).Error; err != nil {
		t.Fatal(err)
	}
	verdict, err := evaluateRedundancyPair(db, p, run.ID, a, b, map[uuid.UUID]models.RedundancyFingerprint{})
	if err != nil || verdict != models.RedundancyVerdictHighConfidence {
		t.Fatal(verdict, err)
	}
	if a.PublicID.String() > b.PublicID.String() {
		a, b = b, a
	}
	var pair models.RedundancyPair
	if err = db.Where("item_a_id=? AND item_b_id=?", a.PublicID, b.PublicID).First(&pair).Error; err != nil {
		t.Fatal(err)
	}
	return pair
}
func reviewConfirm(t *testing.T, db *gorm.DB, pair models.RedundancyPair) {
	t.Helper()
	if err := confirmRedundancyPair(db, "default", "reviewer", pair.PublicID, *pair.LatestEvaluationID); err != nil {
		t.Fatal(err)
	}
}
func TestRedundancyReviewRejectedCrossEdgeBlocksUnion(t *testing.T) {
	db := redundancyFixture(t)
	a, b, c := seedRedundancyItem(t, db, "default", 900, true), seedRedundancyItem(t, db, "default", 900, true), seedRedundancyItem(t, db, "default", 900, true)
	ab, ac, bc := reviewPair(t, db, a, b), reviewPair(t, db, a, c), reviewPair(t, db, b, c)
	reviewConfirm(t, db, ab)
	if r := redundancyHTTP(db, RejectRedundancyPair, ac.PublicID, `{"reason":"different recordings"}`, ""); r.Code != 200 {
		t.Fatal(r.Body.String())
	}
	if err := confirmRedundancyPair(db, "default", "reviewer", bc.PublicID, *bc.LatestEvaluationID); err == nil || !strings.Contains(err.Error(), "permanent human rejection") {
		t.Fatal(err)
	}
	var n int64
	db.Model(&models.RedundancyFamilyMember{}).Where("ended_at IS NULL").Count(&n)
	if n != 2 {
		t.Fatal("conflicting union wrote memberships", n)
	}
}
func TestRedundancyReviewUnavailableCanonicalAtMerge(t *testing.T) {
	for _, state := range []string{"ARCHIVED", models.StorageStateMissing, "locked"} {
		t.Run(state, func(t *testing.T) {
			db := redundancyFixture(t)
			a, b, c := seedRedundancyItem(t, db, "default", 900, true), seedRedundancyItem(t, db, "default", 900, true), seedRedundancyItem(t, db, "default", 900, true)
			db.Model(&a).UpdateColumn("view_count", 1000000)
			db.First(&a, a.ID)
			ab, bc := reviewPair(t, db, a, b), reviewPair(t, db, b, c)
			reviewConfirm(t, db, ab)
			var f models.RedundancyFamily
			db.First(&f)
			if f.CanonicalContentItemID != a.PublicID {
				t.Fatal("fixture canonical", f)
			}
			if state == "locked" {
				db.Model(&f).Update("canonical_locked_by", "editor")
			}
			if state == models.StorageStateMissing {
				db.Model(&a).Update("storage_state", state)
			} else {
				db.Model(&a).Update("status", "ARCHIVED")
			}
			err := confirmRedundancyPair(db, "default", "reviewer", bc.PublicID, *bc.LatestEvaluationID)
			if state == "locked" {
				if err == nil || !strings.Contains(err.Error(), "locked canonical is unavailable") {
					t.Fatal(err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			db.First(&f, f.ID)
			if f.CanonicalContentItemID == a.PublicID {
				t.Fatal("unavailable member selected")
			}
		})
	}
}
func TestRedundancyReviewActiveTranscriptOnly(t *testing.T) {
	db := redundancyFixture(t)
	a, b := seedRedundancyItem(t, db, "default", 900, true), seedRedundancyItem(t, db, "default", 900, true)
	pair := reviewPair(t, db, a, b)
	before, err := loadRedundancyFingerprint(db, "default", a.PublicID)
	if err != nil {
		t.Fatal(err)
	}
	inactive := models.Transcript{ContentItemID: a.PublicID, FullText: strings.Repeat("canceled unwanted text ", 50)}
	if err = db.Create(&inactive).Error; err != nil {
		t.Fatal(err)
	}
	after, err := loadRedundancyFingerprint(db, "default", a.PublicID)
	if err != nil || after.TranscriptChecksum != before.TranscriptChecksum {
		t.Fatal("inactive writeback changed evidence", err)
	}
	r := redundancyHTTP(db, GetRedundancyPair, pair.PublicID, "", "")
	if r.Code != 200 || strings.Contains(r.Body.String(), "canceled unwanted") {
		t.Fatal(r.Body.String())
	}
	if _, err = activeRedundancyTranscript(db, "other-tenant", a.PublicID); err != gorm.ErrRecordNotFound {
		t.Fatal("tenant leak", err)
	}
	db.Model(&a).Update("transcript_id", b.TranscriptID)
	if _, err = activeRedundancyTranscript(db, "default", a.PublicID); err != gorm.ErrRecordNotFound {
		t.Fatal("foreign transcript accepted", err)
	}
}
func TestRedundancyReviewDescriptorRecoveryRetries(t *testing.T) {
	db := redundancyFixture(t)
	for i := 0; i < 2; i++ {
		item := seedRedundancyItem(t, db, "default", 900, true)
		v := make([]float32, 1024)
		v[0] = 1
		if err := db.Model(&item).Updates(map[string]interface{}{"transcript_id": nil, "embedding": pgvector.NewVector(v), "embedding_space_id": strings.Repeat("a", 64)}).Error; err != nil {
			t.Fatal(err)
		}
	}
	descriptorMu.Lock()
	descriptorCache[EmbeddingSpaceText] = expectedSpace{ObservedAt: time.Now().UTC(), Err: "temporarily unavailable"}
	descriptorMu.Unlock()
	run, err := runRedundancyTenant(db, "default", "manual", "test", "full")
	if err != nil || run.Status != "failed" {
		t.Fatal(run, err)
	}
	p, _ := redundancyPolicyFor(db, "default")
	if p.LastSweptAt != nil {
		t.Fatal("outage advanced watermark")
	}
	descriptorMu.Lock()
	descriptorCache[EmbeddingSpaceText] = expectedSpace{SpaceID: strings.Repeat("a", 64), ObservedAt: time.Now().UTC()}
	descriptorMu.Unlock()
	run, err = runRedundancyTenant(db, "default", "manual", "test", "frontier")
	if err != nil || run.Status != "completed" {
		t.Fatal(run, err)
	}
	var pair models.RedundancyPair
	db.First(&pair)
	if pair.Verdict != models.RedundancyVerdictHighConfidence {
		var es []models.RedundancyPairEvaluation
		db.Find(&es)
		t.Fatal("recovery missed pair", pair, string(run.Counts), es)
	}
}
func TestRedundancyReviewCandidatePagesExhaustBlock(t *testing.T) {
	db := redundancyFixture(t)
	items := make([]models.ContentItem, 100)
	for i := range items {
		items[i] = seedRedundancyItem(t, db, "default", 900, true)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].PublicID.String() < items[j].PublicID.String() })
	for i := 0; i < 98; i++ {
		if err := db.Model(&models.Transcript{}).Where("public_id=?", items[i].TranscriptID).Update("full_text", strings.Repeat(fmt.Sprintf("different%d ", i), 100)).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, it := range items[98:] {
		db.Model(&it).Update("updated_at", time.Now().UTC().Add(-time.Second))
	}
	past := time.Now().UTC().Add(-time.Hour)
	db.Model(&models.RedundancyPolicy{}).Where("tenant_id='default'").Updates(map[string]interface{}{"last_swept_at": past, "max_frontier_items": 1, "max_pairs_scored": 17})
	for i := 0; i < 15; i++ {
		run, err := runRedundancyTenant(db, "default", "manual", "test", "frontier")
		if err != nil || run.Status != "completed" {
			t.Fatal(run, err)
		}
		var counts map[string]int
		json.Unmarshal(run.Counts, &counts)
		if counts["pairs_scored"] > 17 {
			t.Fatal("cap exceeded")
		}
		p, _ := redundancyPolicyFor(db, "default")

		if p.LastSweptAt != nil && p.LastSweptAt.After(past) {
			break
		}
		if i == 14 {
			t.Fatal("block did not finish")
		}
	}
	var pair models.RedundancyPair
	if err := db.Where("item_a_id=? AND item_b_id=?", items[98].PublicID, items[99].PublicID).First(&pair).Error; err != nil || pair.Verdict != models.RedundancyVerdictHighConfidence {
		t.Fatal("pair outside priority pools missed", pair, err)
	}
}
func TestRedundancyReviewPolicyConflict(t *testing.T) {
	db := redundancyFixture(t)
	p, _ := redundancyPolicyFor(db, "default")
	stale := p.UpdatedAt.Format(time.RFC3339Nano)
	if r := redundancyHTTP(db, UpdateRedundancyPolicy, uuid.Nil, `{"enabled":false}`, ""); r.Code != 200 {
		t.Fatal(r.Body.String())
	}
	r := redundancyHTTP(db, UpdateRedundancyPolicy, uuid.Nil, fmt.Sprintf(`{"enabled":true,"expected_updated_at":%q}`, stale), "")
	if r.Code != 409 {
		t.Fatal(r.Code, r.Body.String())
	}
	p, _ = redundancyPolicyFor(db, "default")
	if p.Enabled {
		t.Fatal("stale save enabled consequences")
	}
}
func TestRedundancyReviewCirculationProtection(t *testing.T) {
	db := redundancyFixture(t)
	a, b := seedRedundancyItem(t, db, "default", 900, true), seedRedundancyItem(t, db, "default", 900, true)
	reviewConfirm(t, db, reviewPair(t, db, a, b))
	var member models.RedundancyFamilyMember
	db.Where("role='redundant'").First(&member)
	db.Model(&models.RedundancyPolicy{}).Where("tenant_id='default'").Updates(map[string]interface{}{"enabled": true, "emit_circulation_recs": true})
	hold := models.MediaCirculationOverride{TenantID: "default", SubjectKind: "item", SubjectID: member.ContentItemID, OverrideType: models.MediaCirculationOverrideEditorialHold}
	if err := db.Create(&hold).Error; err != nil {
		t.Fatal(err)
	}
	if err := reconcileRedundancyRecommendations(db, "default", "test", nil); err != nil {
		t.Fatal(err)
	}
	var n int64
	db.Model(&models.MediaCirculationRecommendation{}).Count(&n)
	if n != 0 {
		t.Fatal("held copy nominated", n)
	}
	db.Delete(&hold)
	if err := reconcileRedundancyRecommendations(db, "default", "test", nil); err != nil {
		t.Fatal(err)
	}
	var rec models.MediaCirculationRecommendation
	db.Where("verdict='rank_down'").First(&rec)
	hold.ID = 0
	hold.PublicID = uuid.Nil
	if err := db.Create(&hold).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := applyRecommendation(db, "default", "test", "", rec); err == nil {
		t.Fatal("new hold ignored at application")
	}
	db.Delete(&hold)
	db.Model(&models.StoragePolicy{}).Where("tenant_id IS NULL").Update("protect_top_n_by_views", 50)
	if err := validateRedundancyRecommendation(db, "default", rec); err == nil {
		t.Fatal("hot protection ignored")
	}
}

func TestRedundancyReviewCirculationBeyondFirstBatch(t *testing.T) {
	db := redundancyFixture(t)
	canonical := seedRedundancyItem(t, db, "default", 900, true)
	f := models.RedundancyFamily{TenantID: "default", Status: "active", CanonicalContentItemID: canonical.PublicID, FirstConfirmedAt: time.Now().UTC(), LastConfirmedAt: time.Now().UTC()}
	if err := db.Create(&f).Error; err != nil {
		t.Fatal(err)
	}
	items := make([]models.ContentItem, 5001)
	members := make([]models.RedundancyFamilyMember, 5001)
	recs := make([]models.MediaCirculationRecommendation, 5000)
	title, url, duration := "batch copy", "https://fixture.test/recording.mp3", 900
	for i := range items {
		items[i] = models.ContentItem{PublicID: uuid.New(), TenantID: "default", Type: models.ContentTypeVideo, Status: models.ContentStatusReady, Title: &title, DurationSec: &duration, PlaybackURL: &url, FileSizeBytes: 1}
		members[i] = models.RedundancyFamilyMember{TenantID: "default", FamilyID: f.ID, ContentItemID: items[i].PublicID, Role: "redundant", Since: time.Now().UTC()}
		if i < 5000 {
			recs[i] = models.MediaCirculationRecommendation{TenantID: "default", SubjectKind: "content_item", SubjectID: items[i].PublicID, UnitType: models.MediaCirculationUnitItemFamily, Verdict: "rank_down", Action: "rank_down", Status: "pending", Metrics: redundancyJSON(map[string]interface{}{"origin": "redundancy", "family_id": f.ID, "canonical_id": canonical.PublicID, "identity_key": fmt.Sprintf("%d:%s:%s:rank_down", f.ID, canonical.PublicID, items[i].PublicID)})}
		}
	}
	for _, rows := range []interface{}{&items, &members, &recs} {
		if err := db.CreateInBatches(rows, 100).Error; err != nil {
			t.Fatal(err)
		}
	}
	db.Model(&models.RedundancyPolicy{}).Where("tenant_id='default'").Updates(map[string]interface{}{"enabled": true, "emit_circulation_recs": true})
	if err := reconcileRedundancyRecommendations(db, "default", "test", nil); err != nil {
		t.Fatal(err)
	}
	var n int64
	db.Model(&models.MediaCirculationRecommendation{}).Where("subject_id=?", items[5000].PublicID).Count(&n)
	if n != 1 {
		t.Fatal("later member starved behind satisfied batch", n)
	}
}

func TestRedundancyReviewIntegrityHonorsOffSwitch(t *testing.T) {
	db := redundancyFixture(t)
	a, b := seedRedundancyItem(t, db, "default", 900, true), seedRedundancyItem(t, db, "default", 900, true)
	reviewConfirm(t, db, reviewPair(t, db, a, b))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "pods") {
			json.NewEncoder(w).Encode(integrityPodsPage{Items: []PodsItem{{ID: a.PublicID, Type: "VIDEO", Title: "a", DurationSec: 900, MediaURL: *a.PlaybackURL}, {ID: b.PublicID, Type: "VIDEO", Title: "b", DurationSec: 900, MediaURL: *b.PlaybackURL}}})
		} else {
			json.NewEncoder(w).Encode(integrityNewsPage{})
		}
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	t.Setenv("PORT", u.Port())
	for _, test := range []struct {
		enabled, collapse bool
		outcome           string
	}{{true, false, "skipped:collapse_disabled"}, {false, true, "skipped:collapse_disabled"}, {true, true, "violation"}} {
		db.Model(&models.RedundancyPolicy{}).Where("tenant_id='default'").Updates(map[string]interface{}{"enabled": test.enabled, "collapse_enabled": test.collapse})
		policy := models.DefaultFeedIntegrityPolicy("default")
		policy.EdgePagesPerFeed = 1
		collector := newIntegrityCollector(models.FeedIntegrityRun{TenantID: "default", Tier: "deep", StartedAt: time.Now().UTC()}, nil, nil)
		collectFeedIntegrityEdge(context.Background(), db, policy, "deep", collector)
		got := collector.observations["edge_pods_family_dup|pods|default"]
		if got.Outcome != test.outcome {
			t.Fatalf("enabled=%t collapse=%t: %+v", test.enabled, test.collapse, got)
		}
	}
}
