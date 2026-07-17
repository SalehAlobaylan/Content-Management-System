package controllers

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/tests/testdb"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

func studioTrustChapter(t *testing.T, db *gorm.DB, tenantID, code string) models.Chapter {
	t.Helper()
	chapter := models.Chapter{TenantID: tenantID, TranscriptID: uuid.New(), Title: "Trust case", Source: models.ChapterSourceDerived, NeedsReviewCode: &code}
	if err := db.Create(&chapter).Error; err != nil {
		t.Fatal(err)
	}
	return chapter
}

func studioUUIDPtr(id uuid.UUID) *uuid.UUID {
	return &id
}

func studioTrustAudit(t *testing.T, db *gorm.DB, tenantID, userID, email, action string, chapterID uuid.UUID) {
	t.Helper()
	payload, err := json.Marshal(map[string]string{"chapter_id": chapterID.String()})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&models.AuditLog{TenantID: tenantID, UserID: userID, UserEmail: email, Action: action, TargetService: "cms", TargetResource: chapterID.String(), Status: "success", Payload: datatypes.JSON(payload)}).Error; err != nil {
		t.Fatal(err)
	}
}

// mediaStudioTestDB covers row-lock and policy precedence on disposable Postgres.
func mediaStudioTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db := testdb.Open(t)
	if err := db.AutoMigrate(&models.ContentSource{}, &models.MediaAtomizationPolicy{}, &models.ContentItem{}, &models.Chapter{}, &models.MediaCirculationOverride{}, &models.MediaStudioAutopilotPolicy{}, &models.MediaStudioRun{}, &models.MediaStudioAction{}, &models.TranscriptionConfig{}, &models.TranscriptionJob{}, &models.TranscriptQuality{}, &models.AuditLog{}); err != nil {
		t.Fatalf("migrate Media Studio fixture: %v", err)
	}
	clear := func() {
		for _, table := range []string{"audit_logs", "media_studio_actions", "media_studio_runs", "media_studio_autopilot_policies", "transcription_jobs", "transcript_quality", "transcription_configs", "media_circulation_overrides", "chapters", "content_items", "content_sources", "media_atomization_policies"} {
			_ = db.Exec("DELETE FROM " + table).Error
		}
	}
	clear()
	t.Cleanup(clear)
	return db
}

func studioTranscriptRepairFixture(t *testing.T, db *gorm.DB, tenantID string) (models.ContentItem, models.TranscriptQuality) {
	t.Helper()
	mediaURL := "https://media.example.test/episode.mp3"
	captionState := models.CaptionStateSTTDone
	transcriptID := uuid.New()
	item := models.ContentItem{TenantID: tenantID, Type: models.ContentTypePodcast, Source: models.SourceTypePodcast, MediaURL: &mediaURL, TranscriptID: &transcriptID, CaptionState: &captionState}
	if err := db.Create(&item).Error; err != nil {
		t.Fatal(err)
	}
	config := models.DefaultTranscriptionConfig(tenantID)
	config.AutoRepairEnabled = true
	if err := db.Create(&config).Error; err != nil {
		t.Fatal(err)
	}
	quality := models.TranscriptQuality{TenantID: tenantID, ContentItemID: item.PublicID, TranscriptID: transcriptID, Status: models.TranscriptQualityAutoRepair}
	if err := db.Create(&quality).Error; err != nil {
		t.Fatal(err)
	}
	return item, quality
}

func studioPolicyFixture(t *testing.T, db *gorm.DB, tenantID string, sourcePolicy bool) (models.ContentItem, models.ContentItem, models.ContentSource) {
	t.Helper()
	feedURL := "https://example.test/" + tenantID
	config := datatypes.JSON([]byte(fmt.Sprintf(`{"auto_publish_high_confidence":%t}`, sourcePolicy)))
	source := models.ContentSource{TenantID: tenantID, Name: "Studio source", Type: models.SourceTypePodcast, Category: models.SourceCategoryMedia, FeedURL: &feedURL, APIConfig: config}
	if err := db.Create(&source).Error; err != nil {
		t.Fatal(err)
	}
	policy := models.DefaultMediaAtomizationPolicy(tenantID)
	policy.AutoPublishHighConfidence = true
	if err := db.Create(&policy).Error; err != nil {
		t.Fatal(err)
	}
	parent := models.ContentItem{TenantID: tenantID, Type: models.ContentTypePodcast, Source: models.SourceTypePodcast, SourceFeedURL: &feedURL, IsFeedUnit: false}
	if err := db.Create(&parent).Error; err != nil {
		t.Fatal(err)
	}
	duration := forYouMinDurationSec
	child := models.ContentItem{TenantID: tenantID, Type: models.ContentTypePodcast, Source: models.SourceTypePodcast, ParentContentItemID: &parent.PublicID, DurationSec: &duration, FeedVisibility: feedVisibilityReview, Status: models.ContentStatusPending}
	if err := db.Create(&child).Error; err != nil {
		t.Fatal(err)
	}
	return parent, child, source
}

func TestStudioDB_ParentPolicyUsesCanonicalSourceOverride(t *testing.T) {
	db := mediaStudioTestDB(t)
	_, child, _ := studioPolicyFixture(t, db, "studio-a", false)
	// A same-URL source in another tenant must never influence this parent.
	otherURL := "https://example.test/studio-a"
	if err := db.Create(&models.ContentSource{TenantID: "studio-b", Name: "Other tenant", Type: models.SourceTypePodcast, FeedURL: &otherURL, APIConfig: datatypes.JSON([]byte(`{"auto_publish_high_confidence":true}`))}).Error; err != nil {
		t.Fatal(err)
	}
	allowed, found := studioParentAutoPublishPolicy(db, "studio-a", &child)
	if !found || allowed {
		t.Fatalf("source override must win for the parent: found=%v allowed=%v", found, allowed)
	}
	missing := models.ContentItem{ID: 1, ParentContentItemID: func() *uuid.UUID { id := uuid.New(); return &id }()}
	if _, found := studioParentAutoPublishPolicy(db, "studio-a", &missing); found {
		t.Fatal("missing parent must be held rather than falling back to a tenant default")
	}
}

func TestStudioDB_ApplyRechecksParentPolicyAtMutationTime(t *testing.T) {
	db := mediaStudioTestDB(t)
	_, child, source := studioPolicyFixture(t, db, "studio-race", true)
	transcriptID := uuid.New()
	chapter := models.Chapter{TenantID: "studio-race", TranscriptID: transcriptID, Title: "Merged", Source: models.ChapterSourceDerived, Status: chapterStatusReview, ChildContentItemID: &child.PublicID}
	if err := db.Create(&chapter).Error; err != nil {
		t.Fatal(err)
	}
	// Classification observed source=true; the source flips before the review
	// mutation and the transaction must refuse publication.
	source.APIConfig = datatypes.JSON([]byte(`{"auto_publish_high_confidence":false}`))
	if err := db.Save(&source).Error; err != nil {
		t.Fatal(err)
	}
	_, reviewErr := applyAtomizedChapterReviewWithOptions(db, "studio-race", chapter.PublicID, true,
		chapterReviewActor{Email: models.StudioAuditPrincipal}, chapterReviewApplyOptions{RequireNeedsReview: true, RequireParentAutoPublish: true})
	if reviewErr == nil || reviewErr.code != chapterReviewErrUpstreamDisabled {
		t.Fatalf("policy flip must block publish, got %#v", reviewErr)
	}
	var stored models.ContentItem
	if err := db.Where("public_id = ?", child.PublicID).First(&stored).Error; err != nil {
		t.Fatal(err)
	}
	if stored.FeedVisibility == feedVisibilityVisible || stored.Status == models.ContentStatusReady {
		t.Fatalf("blocked publication mutated child: %+v", stored)
	}
}

func TestStudioDB_ApplyRechecksMechanicalFactsAtMutationTime(t *testing.T) {
	db := mediaStudioTestDB(t)
	parent, child, _ := studioPolicyFixture(t, db, "studio-facts", true)
	code := models.StudioReviewCodeMergedShort
	chapter := models.Chapter{TenantID: "studio-facts", TranscriptID: uuid.New(), Title: "Merged", Source: models.ChapterSourceDerived, Status: chapterStatusReview, ChildContentItemID: &child.PublicID, NeedsReviewCode: &code, NeedsReviewCodes: []string{code}, MergedShortProvenance: true}
	if err := db.Create(&chapter).Error; err != nil {
		t.Fatal(err)
	}
	// A sponsor/second code added after classification converts the case into an
	// editorial multi-code case and cannot publish.
	if err := db.Model(&models.Chapter{}).Where("public_id = ?", chapter.PublicID).Updates(map[string]interface{}{"contains_sponsor_intro": true, "needs_review_codes": pq.StringArray{code, models.StudioReviewCodeSponsorIntro}}).Error; err != nil {
		t.Fatal(err)
	}
	_, reviewErr := applyAtomizedChapterReviewWithOptions(db, "studio-facts", chapter.PublicID, true,
		chapterReviewActor{Email: models.StudioAuditPrincipal}, chapterReviewApplyOptions{RequireNeedsReview: true, RequireParentAutoPublish: true, ExpectedChildID: &child.PublicID, ExpectedReviewCodes: []string{code}, RequireMergeProvenance: true, RequireNoSponsor: true, RequireNoBlockingOverride: true})
	if reviewErr == nil || reviewErr.code != chapterReviewErrMultiCode {
		t.Fatalf("changed review facts must block publish, got %#v", reviewErr)
	}
	if err := db.Create(&models.MediaCirculationOverride{TenantID: "studio-facts", SubjectKind: "content_item", SubjectID: parent.PublicID, OverrideType: models.MediaCirculationOverrideEditorialHold}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&models.Chapter{}).Where("public_id = ?", chapter.PublicID).Updates(map[string]interface{}{"contains_sponsor_intro": false, "needs_review_codes": pq.StringArray{code}}).Error; err != nil {
		t.Fatal(err)
	}
	_, reviewErr = applyAtomizedChapterReviewWithOptions(db, "studio-facts", chapter.PublicID, true,
		chapterReviewActor{Email: models.StudioAuditPrincipal}, chapterReviewApplyOptions{RequireNeedsReview: true, RequireParentAutoPublish: true, ExpectedChildID: &child.PublicID, ExpectedReviewCodes: []string{code}, RequireMergeProvenance: true, RequireNoSponsor: true, RequireNoBlockingOverride: true})
	if reviewErr == nil || reviewErr.code != chapterReviewErrOverride {
		t.Fatalf("new override must block publish, got %#v", reviewErr)
	}
}

func TestStudioDB_AutoRejectRechecksCurrentDuration(t *testing.T) {
	db := mediaStudioTestDB(t)
	_, child, _ := studioPolicyFixture(t, db, "studio-reject-race", true)
	code := models.StudioReviewCodeShortUnmergeable
	chapter := models.Chapter{TenantID: "studio-reject-race", TranscriptID: uuid.New(), Title: "Short", Source: models.ChapterSourceDerived, Status: chapterStatusReview, ChildContentItemID: &child.PublicID, NeedsReviewCode: &code, NeedsReviewCodes: []string{code}}
	if err := db.Create(&chapter).Error; err != nil {
		t.Fatal(err)
	}
	_, reviewErr := applyAtomizedChapterReviewWithOptions(db, "studio-reject-race", chapter.PublicID, false,
		chapterReviewActor{Email: models.StudioAuditPrincipal}, chapterReviewApplyOptions{RequireNeedsReview: true, ExpectedChildID: &child.PublicID, ExpectedReviewCodes: []string{code}, RequireNoBlockingOverride: true})
	if reviewErr == nil || reviewErr.code != chapterReviewErrInvalidDuration {
		t.Fatalf("corrected legal duration must block auto-reject, got %#v", reviewErr)
	}
}

func TestStudioDB_TrustCountsOneLatestHumanDecisionPerCase(t *testing.T) {
	db := mediaStudioTestDB(t)
	const tenantID = "studio-trust-unique"
	code := models.StudioReviewCodeMergedShort
	chapter := studioTrustChapter(t, db, tenantID, code)
	for i := 0; i < 20; i++ {
		studioTrustAudit(t, db, tenantID, "human-1", "editor@example.test", "media_studio.atomized_chapter_approved", chapter.PublicID)
	}
	trust := computeStudioReasonCodeTrust(db, tenantID, code, models.DefaultMediaStudioAutopilotPolicy(tenantID))
	if trust.Decisions != 1 || trust.Approvals != 1 || trust.Earned {
		t.Fatalf("repeated endpoint actions must be one case, got %+v", trust)
	}
	// System rows must not become evidence merely because they do not use the
	// Media Studio autopilot email.
	studioTrustAudit(t, db, tenantID, "", "automation", "media_studio.atomized_chapter_approved", chapter.PublicID)
	trust = computeStudioReasonCodeTrust(db, tenantID, code, models.DefaultMediaStudioAutopilotPolicy(tenantID))
	if trust.Decisions != 1 {
		t.Fatalf("system principal counted as a human decision: %+v", trust)
	}
}

func TestStudioDB_AnyHumanReversalLocksTrust(t *testing.T) {
	db := mediaStudioTestDB(t)
	const tenantID = "studio-trust-reversal"
	code := models.StudioReviewCodeMergedShort
	policy := models.DefaultMediaStudioAutopilotPolicy(tenantID)
	for i := 0; i < policy.TrustMinDecisions; i++ {
		chapter := studioTrustChapter(t, db, tenantID, code)
		studioTrustAudit(t, db, tenantID, fmt.Sprintf("human-%d", i), "editor@example.test", "media_studio.atomized_chapter_approved", chapter.PublicID)
		if i == 0 {
			studioTrustAudit(t, db, tenantID, "", models.StudioAuditPrincipal, "media_studio.atomized_chapter_approved", chapter.PublicID)
			studioTrustAudit(t, db, tenantID, "human-0", "editor@example.test", "media_studio.atomized_chapter_rejected", chapter.PublicID)
		}
	}
	trust := computeStudioReasonCodeTrust(db, tenantID, code, policy)
	if trust.Reversals != 1 || trust.Earned {
		t.Fatalf("one explicit human reversal must lock trust even below the percentage threshold: %+v", trust)
	}
}

func TestStudioDB_StaleTranscriptSnapshotCannotCreateJob(t *testing.T) {
	db := mediaStudioTestDB(t)
	item, quality := studioTranscriptRepairFixture(t, db, "studio-stale-snapshot")
	snapshot := studioTranscriptRepairSnapshotFromQuality(quality)
	replacement := uuid.New()
	if err := db.Model(&models.ContentItem{}).Where("public_id = ?", item.PublicID).Update("transcript_id", replacement).Error; err != nil {
		t.Fatal(err)
	}
	_, _, _, _, _, err := claimStudioTranscriptRepairSnapshot(db, item.TenantID, snapshot)
	if !errors.Is(err, errStudioTranscriptSnapshotStale) {
		t.Fatalf("replaced transcript must make claim stale, got %v", err)
	}
	var jobs int64
	if err := db.Model(&models.TranscriptionJob{}).Where("content_item_id = ?", item.PublicID).Count(&jobs).Error; err != nil {
		t.Fatal(err)
	}
	if jobs != 0 {
		t.Fatalf("stale snapshot created %d jobs", jobs)
	}
}

func TestStudioDB_ConcurrentTranscriptClaimsAdmitOneJob(t *testing.T) {
	db := mediaStudioTestDB(t)
	item, quality := studioTranscriptRepairFixture(t, db, "studio-concurrent-claim")
	snapshot := studioTranscriptRepairSnapshotFromQuality(quality)
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _, _, _, _, err := claimStudioTranscriptRepairSnapshot(db, item.TenantID, snapshot)
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	var accepted, blocked int
	for err := range errs {
		if err == nil {
			accepted++
		} else if errors.Is(err, errStudioTranscriptJobInFlight) {
			blocked++
		} else {
			t.Fatalf("unexpected concurrent claim error: %v", err)
		}
	}
	if accepted != 1 || blocked != 1 {
		t.Fatalf("claims accepted=%d blocked=%d, want 1/1", accepted, blocked)
	}
	var jobs int64
	if err := db.Model(&models.TranscriptionJob{}).Where("content_item_id = ? AND status = ?", item.PublicID, models.TranscriptionJobStatusQueued).Count(&jobs).Error; err != nil {
		t.Fatal(err)
	}
	if jobs != 1 {
		t.Fatalf("concurrent claims created %d queued jobs", jobs)
	}
}

func TestStudioDB_LeaseSerializesSameTenantAndReleasesOnClose(t *testing.T) {
	db := mediaStudioTestDB(t)
	releaseFirst, first := tryAcquireStudioAutopilotLock(db, "studio-lease-a")
	if !first {
		t.Fatal("first same-tenant lease must acquire")
	}
	_, second := tryAcquireStudioAutopilotLock(db, "studio-lease-a")
	if second {
		t.Fatal("second connection acquired same-tenant Studio lease")
	}
	releaseOther, other := tryAcquireStudioAutopilotLock(db, "studio-lease-b")
	if !other {
		t.Fatal("different tenant Studio lease must acquire independently")
	}
	releaseOther()
	releaseFirst()
	releaseReacquired, reacquired := tryAcquireStudioAutopilotLock(db, "studio-lease-a")
	if !reacquired {
		t.Fatal("released session lease must be acquirable by a later runner")
	}
	releaseReacquired()
}

func TestStudioDB_HealthAggregatesBeyondExecutionScan(t *testing.T) {
	db := mediaStudioTestDB(t)
	const tenantID = "studio-health-aggregate"
	now := time.Now().UTC()
	merged := models.StudioReviewCodeMergedShort
	chapters := make([]models.Chapter, 0, studioAutopilotMaxCaseScan+51)
	for i := 0; i < studioAutopilotMaxCaseScan+51; i++ {
		code := merged
		createdAt := now
		if i < 11 {
			code = models.StudioReviewCodeShortUnmergeable
			createdAt = now.Add(-8 * 24 * time.Hour)
		}
		chapters = append(chapters, models.Chapter{
			TenantID: tenantID, TranscriptID: uuid.New(), ChildContentItemID: studioUUIDPtr(uuid.New()),
			Title: "health case", Source: models.ChapterSourceDerived, Status: chapterStatusReview,
			NeedsReviewCode: &code, NeedsReviewCodes: []string{code}, CreatedAt: createdAt,
		})
	}
	if err := db.CreateInBatches(&chapters, 100).Error; err != nil {
		t.Fatal(err)
	}

	health, err := collectStudioHealth(db, tenantID, 7)
	if err != nil {
		t.Fatal(err)
	}
	if health.ReviewQueueDepth != studioAutopilotMaxCaseScan+51 {
		t.Fatalf("queue depth=%d, want complete aggregate", health.ReviewQueueDepth)
	}
	if health.AgedCount != 11 || health.ByCode[models.StudioReviewCodeShortUnmergeable] != 11 || health.ByCode[merged] != studioAutopilotMaxCaseScan+40 {
		t.Fatalf("incorrect full health aggregate: %+v", health)
	}
	if health.OldestCaseAgeHours < 7*24 {
		t.Fatalf("oldest age did not include aged rows: %f", health.OldestCaseAgeHours)
	}
}
