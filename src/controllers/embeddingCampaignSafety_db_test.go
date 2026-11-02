package controllers

import (
	"strings"
	"sync"
	"testing"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/spaceid"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

func embeddingSafetyFixture(t *testing.T) (*gorm.DB, models.EmbeddingCampaign) {
	t.Helper()
	db := embeddingLifecycleTestDB(t)
	policy := models.EmbeddingLifecyclePolicy{TenantID: embeddingLifecycleTenant}
	if err := db.Create(&policy).Error; err != nil {
		t.Fatal(err)
	}
	campaign := models.EmbeddingCampaign{
		TenantID: "default", Space: EmbeddingSpaceText, State: models.EmbeddingCampaignRunning,
		TargetSpaceID: strings.Repeat("a", 64), ItemsPerBatch: 20, DailyItemCap: 100, RetryCeiling: 3,
	}
	if err := db.Create(&campaign).Error; err != nil {
		t.Fatal(err)
	}
	return db, campaign
}

func embeddingSafetyAction(c models.EmbeddingCampaign, target string) models.EmbeddingCampaignAction {
	return models.EmbeddingCampaignAction{
		CampaignID: c.ID, TenantID: "tenant", SurfaceKey: "content_text", Tool: models.EmbeddingToolTextEmbedding,
		TargetID: target, Status: models.EmbeddingActionAttempted,
		ExpectedProducerID: spaceid.ProducerID(c.TargetSpaceID, spaceid.RecipeContentText),
	}
}

func embeddingSafetyItem(t *testing.T, db *gorm.DB, id uuid.UUID, space, producer string) {
	t.Helper()
	if err := db.Exec(`CREATE TABLE IF NOT EXISTS content_items (
        public_id uuid PRIMARY KEY, tenant_id varchar(64) NOT NULL,
        embedding vector(1024), embedding_space_id char(64), embedding_producer_id char(64)
    )`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO content_items(public_id,tenant_id,embedding,embedding_space_id,embedding_producer_id)
        VALUES (?, 'tenant', array_fill(0.1::real, ARRAY[1024])::vector, ?, ?)`, id, space, producer).Error; err != nil {
		t.Fatal(err)
	}
}

func TestEmbeddingSafetyConcurrentClaimHasOneOwner(t *testing.T) {
	db, campaign := embeddingSafetyFixture(t)
	const workers = 12
	start := make(chan struct{})
	results := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			action := embeddingSafetyAction(campaign, "same-target")
			results <- claimCampaignAction(db, &campaign, &action)
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		}
	}
	var count int64
	if err := db.Model(&models.EmbeddingCampaignAction{}).Where("campaign_id=?", campaign.ID).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if successes != 1 || count != 1 {
		t.Fatalf("concurrent ownership: successful claims=%d persisted=%d, want one each", successes, count)
	}
}

func TestEmbeddingSafetyPausePreventsNewClaims(t *testing.T) {
	for _, pause := range []string{"campaign", "policy"} {
		t.Run(pause, func(t *testing.T) {
			db, campaign := embeddingSafetyFixture(t)
			if pause == "campaign" {
				if err := db.Model(&campaign).Update("state", models.EmbeddingCampaignPaused).Error; err != nil {
					t.Fatal(err)
				}
				campaign.State = models.EmbeddingCampaignRunning // stale runner snapshot
			} else {
				if err := db.Model(&models.EmbeddingLifecyclePolicy{}).Where("tenant_id=?", embeddingLifecycleTenant).
					Update("campaigns_paused_until", time.Now().Add(time.Hour)).Error; err != nil {
					t.Fatal(err)
				}
			}
			action := embeddingSafetyAction(campaign, "target")
			if err := claimCampaignAction(db, &campaign, &action); err == nil {
				t.Fatal("paused campaign/policy admitted a new action")
			}
			var count int64
			if err := db.Model(&models.EmbeddingCampaignAction{}).Count(&count).Error; err != nil || count != 0 {
				t.Fatalf("paused claim persisted work: count=%d err=%v", count, err)
			}
		})
	}
}

func TestEmbeddingSafetyDailyCapIncludesFailuresAndAttempts(t *testing.T) {
	db, campaign := embeddingSafetyFixture(t)
	campaign.DailyItemCap = 3
	if err := db.Model(&campaign).Update("daily_item_cap", 3).Error; err != nil {
		t.Fatal(err)
	}
	for _, status := range []string{models.EmbeddingActionFailed, models.EmbeddingActionAttempted, models.EmbeddingActionCompleted, models.EmbeddingActionSkipped} {
		action := embeddingSafetyAction(campaign, status)
		action.Status = status
		if err := db.Create(&action).Error; err != nil {
			t.Fatal(err)
		}
	}
	old := embeddingSafetyAction(campaign, "old-failure")
	old.Status, old.CreatedAt = models.EmbeddingActionFailed, time.Now().Add(-25*time.Hour)
	if err := db.Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	remaining, err := campaignBudgetRemaining(db, &campaign)
	if err != nil || remaining != 0 {
		t.Fatalf("recent failed, attempted and completed calls must exhaust cap: remaining=%d err=%v", remaining, err)
	}
	action := embeddingSafetyAction(campaign, "new-target")
	if err := claimCampaignAction(db, &campaign, &action); err == nil {
		t.Fatal("daily cap admitted an additional call")
	}
	if err := db.Where("campaign_id=? AND target_id=?", campaign.ID, models.EmbeddingActionFailed).Delete(&models.EmbeddingCampaignAction{}).Error; err != nil {
		t.Fatal(err)
	}
	remaining, err = campaignBudgetRemaining(db, &campaign)
	if err != nil || remaining != 1 {
		t.Fatalf("skipped/old calls must not consume current budget: remaining=%d err=%v", remaining, err)
	}
}

func TestEmbeddingSafetyExpiredAttemptRecoversVerifiedReadback(t *testing.T) {
	db, campaign := embeddingSafetyFixture(t)
	id := uuid.New()
	action := embeddingSafetyAction(campaign, id.String())
	action.RequestID, action.CreatedAt = uuid.NewString(), time.Now().Add(-embeddingAttemptLease-time.Minute)
	embeddingSafetyItem(t, db, id, campaign.TargetSpaceID, action.ExpectedProducerID)
	if err := db.Create(&action).Error; err != nil {
		t.Fatal(err)
	}
	exception := models.EmbeddingCampaignException{
		CampaignID: campaign.ID, TenantID: "tenant", SurfaceKey: action.SurfaceKey, TargetID: action.TargetID,
		Status: models.EmbeddingExceptionOpen, Attempts: 1,
	}
	if err := db.Create(&exception).Error; err != nil {
		t.Fatal(err)
	}
	if err := recoverCampaignActions(db, &campaign); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&action, action.ID).Error; err != nil {
		t.Fatal(err)
	}
	if action.Status != models.EmbeddingActionCompleted || action.ObservedProducerID != action.ExpectedProducerID {
		t.Fatalf("verified recovery did not complete the attempt: %+v", action)
	}
	if err := db.First(&exception, exception.ID).Error; err != nil || exception.Status != models.EmbeddingExceptionResolved {
		t.Fatalf("verified recovery did not resolve prior exception: status=%s err=%v", exception.Status, err)
	}
}

func TestEmbeddingSafetyExpiredStaleAttemptCanRetryWithFreshClaim(t *testing.T) {
	db, campaign := embeddingSafetyFixture(t)
	id := uuid.New()
	action := embeddingSafetyAction(campaign, id.String())
	action.RequestID, action.CreatedAt = uuid.NewString(), time.Now().Add(-embeddingAttemptLease-time.Minute)
	embeddingSafetyItem(t, db, id, strings.Repeat("b", 64), strings.Repeat("c", 64))
	if err := db.Create(&action).Error; err != nil {
		t.Fatal(err)
	}
	if err := recoverCampaignActions(db, &campaign); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&action, action.ID).Error; err != nil || action.Status != models.EmbeddingActionFailed {
		t.Fatalf("stale expired attempt must fail: status=%s err=%v", action.Status, err)
	}
	var exception models.EmbeddingCampaignException
	if err := db.Where("campaign_id=? AND target_id=?", campaign.ID, action.TargetID).First(&exception).Error; err != nil || exception.Status != models.EmbeddingExceptionOpen || exception.Attempts != 1 {
		t.Fatalf("stale expired attempt must leave one actionable exception: %+v err=%v", exception, err)
	}
	surface, _ := EmbeddingSurfaceByKey("content_text")
	retry, ok := claimOwnerAction(db, &campaign, surface, action.TargetID, "retry-batch", action.Tool, action.ExpectedProducerID, "tenant")
	if !ok || retry.RetryNumber != 1 || retry.RequestID == "" || retry.RequestID == action.RequestID {
		t.Fatalf("fresh retry did not acquire independent ownership: %+v ok=%v", retry, ok)
	}
	item := models.ContentItem{PublicID: id, TenantID: "tenant"}
	for _, attempt := range []struct {
		action models.EmbeddingCampaignAction
		valid  bool
	}{{action, false}, {*retry, true}} {
		correlation := embeddingCampaignCorrelation{CampaignID: campaign.ID, ActionID: attempt.action.ID, RequestID: attempt.action.RequestID}
		err := db.Transaction(func(tx *gorm.DB) error {
			return authorizeEmbeddingCampaignWrite(tx, item, "content_text", &correlation, campaign.TargetSpaceID, action.ExpectedProducerID)
		})
		if (err == nil) != attempt.valid {
			t.Fatalf("claim authorization validity=%t, want %t (err=%v)", err == nil, attempt.valid, err)
		}
	}
}

func TestEmbeddingSafetyWriteCorrelationBindsTargetAndFreshAttempt(t *testing.T) {
	db, campaign := embeddingSafetyFixture(t)
	id := uuid.New()
	action := embeddingSafetyAction(campaign, id.String())
	if err := claimCampaignAction(db, &campaign, &action); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		change  func(*embeddingCampaignCorrelation, *models.ContentItem, *string, *string, *string)
		wantErr bool
	}{
		{"current", func(*embeddingCampaignCorrelation, *models.ContentItem, *string, *string, *string) {}, false},
		{"wrong_request", func(c *embeddingCampaignCorrelation, _ *models.ContentItem, _, _, _ *string) {
			c.RequestID = uuid.NewString()
		}, true},
		{"wrong_campaign", func(c *embeddingCampaignCorrelation, _ *models.ContentItem, _, _, _ *string) { c.CampaignID++ }, true},
		{"wrong_action", func(c *embeddingCampaignCorrelation, _ *models.ContentItem, _, _, _ *string) { c.ActionID++ }, true},
		{"wrong_item", func(_ *embeddingCampaignCorrelation, i *models.ContentItem, _, _, _ *string) { i.PublicID = uuid.New() }, true},
		{"wrong_tenant", func(_ *embeddingCampaignCorrelation, i *models.ContentItem, _, _, _ *string) { i.TenantID = "other" }, true},
		{"wrong_surface", func(_ *embeddingCampaignCorrelation, _ *models.ContentItem, s, _, _ *string) { *s = "content_image" }, true},
		{"wrong_space", func(_ *embeddingCampaignCorrelation, _ *models.ContentItem, _, s, _ *string) {
			*s = strings.Repeat("b", 64)
		}, true},
		{"wrong_producer", func(_ *embeddingCampaignCorrelation, _ *models.ContentItem, _, _, p *string) {
			*p = strings.Repeat("c", 64)
		}, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			correlation := embeddingCampaignCorrelation{CampaignID: campaign.ID, ActionID: action.ID, RequestID: action.RequestID}
			item := models.ContentItem{PublicID: id, TenantID: "tenant"}
			surface, space, producer := "content_text", campaign.TargetSpaceID, action.ExpectedProducerID
			test.change(&correlation, &item, &surface, &space, &producer)
			err := db.Transaction(func(tx *gorm.DB) error {
				return authorizeEmbeddingCampaignWrite(tx, item, surface, &correlation, space, producer)
			})
			if (err != nil) != test.wantErr {
				t.Fatalf("authorization error=%v wantErr=%t", err, test.wantErr)
			}
		})
	}
	if err := db.Model(&action).Update("created_at", time.Now().Add(-embeddingAttemptLease-time.Second)).Error; err != nil {
		t.Fatal(err)
	}
	correlation := embeddingCampaignCorrelation{CampaignID: campaign.ID, ActionID: action.ID, RequestID: action.RequestID}
	if err := db.Transaction(func(tx *gorm.DB) error {
		return authorizeEmbeddingCampaignWrite(tx, models.ContentItem{PublicID: id, TenantID: "tenant"}, "content_text", &correlation, campaign.TargetSpaceID, action.ExpectedProducerID)
	}); err == nil {
		t.Fatal("late response from expired attempt retained write authority")
	}
}

func TestEmbeddingSafetyWaiverExcludesInconsistentSpaceDebt(t *testing.T) {
	db, c := embeddingSafetyFixture(t)
	id := uuid.New()
	producer := spaceid.ProducerID(c.TargetSpaceID, spaceid.RecipeContentText)
	embeddingSafetyItem(t, db, id, "", producer)
	if err := db.Exec(`ALTER TABLE content_items ADD COLUMN status text DEFAULT 'READY', ADD COLUMN created_at timestamptz DEFAULT now()`).Error; err != nil {
		t.Fatal(err)
	}
	surface, _ := EmbeddingSurfaceByKey("content_text")
	if remainingItemTargets(db, &c, expectedSpace{}) != 1 || len(campaignBatchTargets(db, &c, surface, producer, 10)) != 1 {
		t.Fatal("expected producer concealed wrong-space debt")
	}
	expires := time.Now().Add(time.Hour)
	exception := models.EmbeddingCampaignException{CampaignID: c.ID, TenantID: "tenant", SurfaceKey: surface.Key, TargetID: id.String(), Status: models.EmbeddingExceptionWaived, WaiverExpires: &expires}
	if err := db.Create(&exception).Error; err != nil {
		t.Fatal(err)
	}
	if remainingItemTargets(db, &c, expectedSpace{}) != 0 || len(campaignBatchTargets(db, &c, surface, producer, 10)) != 0 {
		t.Fatal("waived item still blocks or consumes a batch")
	}
	if err := db.Model(&exception).Update("waiver_expires", time.Now().Add(-time.Minute)).Error; err != nil {
		t.Fatal(err)
	}
	if remainingItemTargets(db, &c, expectedSpace{}) != 1 {
		t.Fatal("expired waiver removed real debt")
	}
}

func TestEmbeddingSafetyAuditLockAndOperatorStateAreAuthoritative(t *testing.T) {
	db, c := embeddingSafetyFixture(t)
	release, ok := tryEmbeddingAdvisoryLock(db)
	if !ok {
		t.Fatal("first runner could not lock")
	}
	releaseSecond, second := tryEmbeddingAdvisoryLock(db)
	releaseSecond()
	if second {
		release()
		t.Fatal("concurrent runner obtained advisory lock")
	}
	release()
	release, ok = tryEmbeddingAdvisoryLock(db)
	if !ok {
		t.Fatal("released lock could not be reclaimed")
	}
	release()
	for _, state := range []string{models.EmbeddingCampaignPaused, models.EmbeddingCampaignAborted} {
		if err := db.Model(&c).Update("state", state).Error; err != nil {
			t.Fatal(err)
		}
		stale := c
		stale.State = models.EmbeddingCampaignRunning
		if campaignSetState(db, &stale, models.EmbeddingCampaignVerifying, "") {
			t.Fatal("stale runner overwrote operator state")
		}
	}
}
