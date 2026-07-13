package controllers

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func studioTrustChapter(t *testing.T, db *gorm.DB, tenantID, code string) models.Chapter {
	t.Helper()
	chapter := models.Chapter{TenantID: tenantID, TranscriptID: uuid.New(), Title: "Trust case", Source: models.ChapterSourceDerived, NeedsReviewCode: &code}
	if err := db.Create(&chapter).Error; err != nil {
		t.Fatal(err)
	}
	return chapter
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

// mediaStudioTestDB is deliberately opt-in because it migrates and clears a
// disposable database. It covers the row-lock and policy-precedence behavior
// that SQLite/sqlmock cannot faithfully exercise.
func mediaStudioTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv("MEDIA_STUDIO_TEST_DATABASE_URL"))
	if dsn == "" {
		t.Skip("set MEDIA_STUDIO_TEST_DATABASE_URL to run Media Studio PostgreSQL tests")
	}
	if err := validateMediaStudioTestDSN(dsn); err != nil {
		t.Fatal(err)
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	for _, extension := range []string{"vector", "pgcrypto"} {
		if err := db.Exec("CREATE EXTENSION IF NOT EXISTS " + extension).Error; err != nil {
			t.Fatalf("enable %s extension: %v", extension, err)
		}
	}
	if err := db.AutoMigrate(&models.ContentSource{}, &models.MediaAtomizationPolicy{}, &models.ContentItem{}, &models.Chapter{}, &models.MediaCirculationOverride{}, &models.AuditLog{}); err != nil {
		t.Fatalf("migrate Media Studio fixture: %v", err)
	}
	clear := func() {
		for _, table := range []string{"audit_logs", "media_circulation_overrides", "chapters", "content_items", "content_sources", "media_atomization_policies"} {
			_ = db.Exec("DELETE FROM " + table).Error
		}
	}
	clear()
	t.Cleanup(clear)
	return db
}

func validateMediaStudioTestDSN(dsn string) error {
	if !strings.Contains(strings.ToLower(dsn), "test") {
		return fmt.Errorf("MEDIA_STUDIO_TEST_DATABASE_URL must name a disposable test database")
	}
	return nil
}

func TestMediaStudioDSNSafetyGuard(t *testing.T) {
	if err := validateMediaStudioTestDSN("postgres://localhost/wahb"); err == nil {
		t.Fatal("non-test DSN must be rejected before opening a database")
	}
	if err := validateMediaStudioTestDSN("postgres://localhost/wahb_test"); err != nil {
		t.Fatalf("test DSN rejected: %v", err)
	}
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
	if err := db.Model(&models.Chapter{}).Where("public_id = ?", chapter.PublicID).Updates(map[string]interface{}{"contains_sponsor_intro": true, "needs_review_codes": []string{code, models.StudioReviewCodeSponsorIntro}}).Error; err != nil {
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
	if err := db.Model(&models.Chapter{}).Where("public_id = ?", chapter.PublicID).Updates(map[string]interface{}{"contains_sponsor_intro": false, "needs_review_codes": []string{code}}).Error; err != nil {
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
