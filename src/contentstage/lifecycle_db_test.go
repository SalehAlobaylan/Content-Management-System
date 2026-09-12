package contentstage

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/podsflow"
	"content-management-system/src/tests/testdb"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func contentStageDisposableDatabaseConfigured() bool {
	return os.Getenv("CMS_TEST_ADMIN_URL") != "" || os.Getenv("CMS_TEST_DATABASE_URL") != ""
}

func openContentStageFixtureDB(t *testing.T) *gorm.DB {
	t.Helper()
	if !contentStageDisposableDatabaseConfigured() {
		t.Skip("set guarded CMS_TEST_ADMIN_URL or CMS_TEST_DATABASE_URL to run content-stage claim DB tests")
	}
	db := testdb.Open(t)
	if err := db.AutoMigrate(
		&models.ContentItem{},
		&models.ContentStageRequest{}, &models.ContentStageAttempt{},
		&models.ContentStageReceipt{}, &models.ContentStageEvent{},
		&models.ContentStageCutover{}, &models.ContentStageControl{},
		&models.AtomizationGeneration{}, &models.AtomizationChapterUnit{},
		&models.TranscriptionGeneration{}, &models.TranscriptionSegmentUnit{},
		&models.AtomizationWorkRequest{}, &models.MediaArtifactManifest{},
		&podsflow.Slot{}, &podsflow.Disposition{},
	); err != nil {
		t.Fatalf("migrate content-stage fixture schema: %v", err)
	}
	ResetSchemaAvailabilityCache()
	t.Cleanup(ResetSchemaAvailabilityCache)
	resetContentStageFixture(t, db)
	t.Cleanup(func() { resetContentStageFixture(t, db) })
	return db
}

func resetContentStageFixture(t *testing.T, db *gorm.DB) {
	t.Helper()
	for _, table := range []string{
		"pods_episode_dispositions", "pods_episode_execution_slot", "media_artifact_manifests",
		"atomization_chapter_units", "atomization_generations", "transcription_segment_units", "transcription_generations", "atomization_work_requests",
		"content_stage_events", "content_stage_receipts", "content_stage_attempts",
		"content_stage_requests", "content_stage_controls", "content_stage_cutovers", "content_items",
	} {
		if err := db.Exec("DELETE FROM " + table).Error; err != nil {
			t.Fatalf("clear %s: %v", table, err)
		}
	}
	if err := db.Create(&podsflow.Slot{Singleton: true}).Error; err != nil {
		t.Fatal(err)
	}
}

func seedContentStageRequest(t *testing.T, db *gorm.DB, tenant string, kind models.ContentType, stage string, createdAt time.Time) (models.ContentItem, models.ContentStageRequest) {
	t.Helper()
	descriptor, ok := descriptors[stage]
	if !ok {
		t.Fatalf("unknown stage fixture: %s", stage)
	}
	title := tenant + " " + stage
	item := models.ContentItem{
		PublicID: uuid.New(), TenantID: tenant, Type: kind, Source: models.SourceTypeRSS,
		Status: models.ContentStatusPending, ProcessingGeneration: 1, Title: &title,
		IsFeedUnit: true, FeedVisibility: "visible",
	}
	if err := db.Create(&item).Error; err != nil {
		t.Fatalf("create content item: %v", err)
	}
	cutover := models.ContentStageCutover{
		TenantID: tenant, Lane: descriptor.Lane, Mode: models.ContentStageCutoverDurableRequired,
		ProtocolVersion: ProtocolVersion,
	}
	if err := db.Where("tenant_id=? AND lane=?", tenant, descriptor.Lane).FirstOrCreate(&cutover).Error; err != nil {
		t.Fatalf("create stage cutover: %v", err)
	}
	deadline := time.Now().UTC().Add(time.Hour)
	request := models.ContentStageRequest{
		PublicID: uuid.New(), TenantID: tenant, ContentItemID: item.PublicID,
		ProcessingGeneration: 1, Lane: descriptor.Lane, Stage: stage, Owner: descriptor.Owner,
		BlockingScope: descriptor.BlockingScope, State: models.ContentStageQueued,
		InputFingerprint: stageFingerprint(item, descriptor), PolicyVersion: ProtocolVersion,
		ModelRecipe: descriptor.ModelRecipe, IdempotencyKey: uuid.NewString(),
		DependencyManifest: jsonValue([]string{}), WorkloadEstimate: jsonValue(map[string]any{"units": 1}),
		DeadlineAt: &deadline, CreatedAt: createdAt.UTC(), UpdatedAt: createdAt.UTC(),
	}
	if err := db.Create(&request).Error; err != nil {
		t.Fatalf("create stage request: %v", err)
	}
	return item, request
}

func assertPersistedClaim(t *testing.T, db *gorm.DB, envelope ClaimEnvelope, expected models.ContentStageRequest) {
	t.Helper()
	if envelope.RequestID != expected.PublicID || envelope.TenantID != expected.TenantID || envelope.Stage != expected.Stage {
		t.Fatalf("unexpected envelope identity: %#v", envelope)
	}
	if envelope.AttemptID == uuid.Nil || envelope.ClaimToken == uuid.Nil || envelope.FenceToken == uuid.Nil || envelope.LeaseEpoch != 1 {
		t.Fatalf("claim envelope is not fenced: %#v", envelope)
	}
	if envelope.DeterministicJobID == "" || !envelope.LeaseExpiresAt.After(time.Now().UTC()) {
		t.Fatalf("claim lease is incomplete: %#v", envelope)
	}
	var stored models.ContentStageRequest
	if err := db.Where("public_id=?", expected.PublicID).First(&stored).Error; err != nil {
		t.Fatal(err)
	}
	if stored.State != models.ContentStageClaimed || stored.ClaimToken == nil || *stored.ClaimToken != envelope.ClaimToken {
		t.Fatalf("request claim was not persisted: %#v", stored)
	}
	var attempt models.ContentStageAttempt
	if err := db.Where("public_id=? AND request_id=?", envelope.AttemptID, envelope.RequestID).First(&attempt).Error; err != nil {
		t.Fatal(err)
	}
	if attempt.FenceToken != envelope.FenceToken || attempt.DeterministicJobID != envelope.DeterministicJobID {
		t.Fatalf("attempt fence was not persisted: %#v", attempt)
	}
	var event models.ContentStageEvent
	if err := db.Where("request_id=? AND event_type=?", envelope.RequestID, "claimed").First(&event).Error; err != nil {
		t.Fatal(err)
	}
	if event.AttemptID == nil || *event.AttemptID != envelope.AttemptID {
		t.Fatalf("claimed event is not correlated: %#v", event)
	}
}

func TestClaimNextAnyTenantDB_AllOwnerEntryPointsPersistFencedClaims(t *testing.T) {
	db := openContentStageFixtureDB(t)
	cases := []struct {
		name  string
		kind  models.ContentType
		stage string
		claim func(*gorm.DB) (ClaimEnvelope, bool, error)
	}{
		{
			name: "news", kind: models.ContentTypeNews, stage: models.ContentStageNewsTextEmbedding,
			claim: func(db *gorm.DB) (ClaimEnvelope, bool, error) {
				return ClaimNextAnyTenant(db, models.ContentStageLaneNews, "aggregation-news-test")
			},
		},
		{
			name: "pods", kind: models.ContentTypePodcast, stage: models.ContentStagePodsMediaArtifacts,
			claim: func(db *gorm.DB) (ClaimEnvelope, bool, error) {
				return ClaimNextAnyTenantForStages(db, models.ContentStageLanePods, "aggregation-pods-test", []string{models.ContentStagePodsMediaArtifacts})
			},
		},
		{
			name: "media", kind: models.ContentTypePodcast, stage: models.ContentStagePodsTranscript,
			claim: func(db *gorm.DB) (ClaimEnvelope, bool, error) {
				return ClaimMediaNextAnyTenant(db, "media-test")
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetContentStageFixture(t, db)
			_, request := seedContentStageRequest(t, db, "tenant-"+tc.name, tc.kind, tc.stage, time.Now().UTC().Add(-time.Minute))
			envelope, found, err := tc.claim(db)
			if err != nil {
				t.Fatal(err)
			}
			if !found {
				t.Fatal("eligible request was not claimed")
			}
			assertPersistedClaim(t, db, envelope, request)
		})
	}
}

func TestClaimNextDB_ExplicitTenantRequiredPriorityAndStageFilter(t *testing.T) {
	db := openContentStageFixtureDB(t)
	now := time.Now().UTC()

	t.Run("explicit tenant FIFO", func(t *testing.T) {
		resetContentStageFixture(t, db)
		_, oldest := seedContentStageRequest(t, db, "tenant-a", models.ContentTypeNews, models.ContentStageNewsTextEmbedding, now.Add(-2*time.Minute))
		seedContentStageRequest(t, db, "tenant-a", models.ContentTypeNews, models.ContentStageNewsTextEmbedding, now.Add(-time.Minute))
		envelope, found, err := ClaimNext(db, "tenant-a", models.ContentStageLaneNews, "news-test")
		if err != nil || !found {
			t.Fatalf("claim failed: found=%v err=%v", found, err)
		}
		if envelope.RequestID != oldest.PublicID {
			t.Fatalf("FIFO violated: got %s want %s", envelope.RequestID, oldest.PublicID)
		}
	})

	t.Run("required before optional", func(t *testing.T) {
		resetContentStageFixture(t, db)
		seedContentStageRequest(t, db, "tenant-a", models.ContentTypeNews, models.ContentStageNewsLLMMetadata, now.Add(-2*time.Minute))
		_, required := seedContentStageRequest(t, db, "tenant-a", models.ContentTypeNews, models.ContentStageNewsTextEmbedding, now.Add(-time.Minute))
		envelope, found, err := ClaimNextAnyTenant(db, models.ContentStageLaneNews, "news-test")
		if err != nil || !found {
			t.Fatalf("claim failed: found=%v err=%v", found, err)
		}
		if envelope.RequestID != required.PublicID {
			t.Fatalf("optional work bypassed required request: got %s want %s", envelope.RequestID, required.PublicID)
		}
	})

	t.Run("stage subset", func(t *testing.T) {
		resetContentStageFixture(t, db)
		seedContentStageRequest(t, db, "tenant-a", models.ContentTypePodcast, models.ContentStagePodsMediaArtifacts, now.Add(-2*time.Minute))
		_, embedding := seedContentStageRequest(t, db, "tenant-a", models.ContentTypePodcast, models.ContentStagePodsTextEmbedding, now.Add(-time.Minute))
		envelope, found, err := ClaimNextAnyTenantForStages(db, models.ContentStageLanePods, "pods-test", []string{models.ContentStagePodsTextEmbedding})
		if err != nil || !found {
			t.Fatalf("claim failed: found=%v err=%v", found, err)
		}
		if envelope.RequestID != embedding.PublicID {
			t.Fatalf("stage filter violated: got %s want %s", envelope.RequestID, embedding.PublicID)
		}
	})
}

func TestClaimNextAnyTenantDB_FairnessAndLockSkipping(t *testing.T) {
	db := openContentStageFixtureDB(t)
	now := time.Now().UTC()

	t.Run("unserved tenant precedes recently served tenant", func(t *testing.T) {
		resetContentStageFixture(t, db)
		_, tenantA := seedContentStageRequest(t, db, "tenant-a", models.ContentTypeNews, models.ContentStageNewsTextEmbedding, now.Add(-2*time.Minute))
		_, tenantB := seedContentStageRequest(t, db, "tenant-b", models.ContentTypeNews, models.ContentStageNewsTextEmbedding, now.Add(-time.Minute))
		if err := db.Create(&models.ContentStageAttempt{
			PublicID: uuid.New(), TenantID: tenantA.TenantID, RequestID: uuid.New(), AttemptNumber: 1,
			Lane: tenantA.Lane, Stage: tenantA.Stage, Owner: tenantA.Owner,
			State: models.ContentStageVerified, ClaimToken: uuid.New(), FenceToken: uuid.New(),
			LeaseEpoch: 1, DeterministicJobID: uuid.NewString(), LeaseExpiresAt: now, HeartbeatAt: now,
			CreatedAt: now.Add(-time.Minute), UpdatedAt: now.Add(-time.Minute),
		}).Error; err != nil {
			t.Fatal(err)
		}
		envelope, found, err := ClaimNextAnyTenant(db, models.ContentStageLaneNews, "news-test")
		if err != nil || !found {
			t.Fatalf("claim failed: found=%v err=%v", found, err)
		}
		if envelope.RequestID != tenantB.PublicID {
			t.Fatalf("tenant fairness violated: got %s want %s", envelope.RequestID, tenantB.PublicID)
		}
	})

	t.Run("locked oldest row is skipped", func(t *testing.T) {
		resetContentStageFixture(t, db)
		_, oldest := seedContentStageRequest(t, db, "tenant-a", models.ContentTypeNews, models.ContentStageNewsTextEmbedding, now.Add(-2*time.Minute))
		_, next := seedContentStageRequest(t, db, "tenant-a", models.ContentTypeNews, models.ContentStageNewsTextEmbedding, now.Add(-time.Minute))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		claimDB := db.WithContext(ctx)
		locker := claimDB.Begin()
		if locker.Error != nil {
			t.Fatal(locker.Error)
		}
		defer locker.Rollback()
		var locked models.ContentStageRequest
		if err := locker.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=?", oldest.PublicID).First(&locked).Error; err != nil {
			t.Fatal(err)
		}
		envelope, found, err := ClaimNextAnyTenant(claimDB, models.ContentStageLaneNews, "news-test")
		if err != nil || !found {
			t.Fatalf("claim failed: found=%v err=%v", found, err)
		}
		if envelope.RequestID != next.PublicID {
			t.Fatalf("locked row was not skipped: got %s want %s", envelope.RequestID, next.PublicID)
		}
	})
}

func TestClaimNextAnyTenantDB_ConcurrentClaimersDoNotDuplicate(t *testing.T) {
	db := openContentStageFixtureDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	claimDB := db.WithContext(ctx)
	now := time.Now().UTC()
	_, first := seedContentStageRequest(t, db, "tenant-a", models.ContentTypeNews, models.ContentStageNewsTextEmbedding, now.Add(-2*time.Minute))
	_, second := seedContentStageRequest(t, db, "tenant-a", models.ContentTypeNews, models.ContentStageNewsTextEmbedding, now.Add(-time.Minute))

	type claimResult struct {
		envelope ClaimEnvelope
		found    bool
		err      error
	}
	start := make(chan struct{})
	results := make(chan claimResult, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			ready.Done()
			<-start
			envelope, found, err := ClaimNextAnyTenant(claimDB, models.ContentStageLaneNews, "news-concurrent-test")
			results <- claimResult{envelope: envelope, found: found, err: err}
		}()
	}
	ready.Wait()
	close(start)

	claimed := map[uuid.UUID]bool{}
	for i := 0; i < 2; i++ {
		var got claimResult
		select {
		case got = <-results:
		case <-ctx.Done():
			t.Fatalf("concurrent claims did not finish before timeout: %v", ctx.Err())
		}
		if got.err != nil || !got.found {
			t.Fatalf("concurrent claim failed: found=%v err=%v", got.found, got.err)
		}
		if claimed[got.envelope.RequestID] {
			t.Fatalf("request was claimed twice: %s", got.envelope.RequestID)
		}
		claimed[got.envelope.RequestID] = true
	}
	if !claimed[first.PublicID] || !claimed[second.PublicID] {
		t.Fatalf("expected both requests to be claimed, got %v", claimed)
	}
}

func TestClaimNextAnyTenantDB_EmptyQueueReturnsNoClaim(t *testing.T) {
	db := openContentStageFixtureDB(t)
	envelope, found, err := ClaimNextAnyTenant(db, models.ContentStageLaneNews, "news-test")
	if err != nil {
		t.Fatal(err)
	}
	if found || envelope.RequestID != uuid.Nil {
		t.Fatalf("empty queue returned a claim: %#v", envelope)
	}
}
