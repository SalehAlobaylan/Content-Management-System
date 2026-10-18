package controllers

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/supply"
	"content-management-system/src/tests/testdb"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

func consumerProofDB(t *testing.T) *gorm.DB {
	t.Helper()
	if os.Getenv("CMS_TEST_ADMIN_URL") == "" && os.Getenv("CMS_TEST_DATABASE_URL") == "" {
		t.Skip("guarded disposable PostgreSQL required")
	}
	db := testdb.Open(t)
	if err := db.Exec(`CREATE TABLE content_items(public_id uuid PRIMARY KEY,tenant_id varchar(64),source_run_request_id bigint,content_source_id uuid)`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&models.ConsumerFeedSession{}, &models.PodsBoundaryObservation{}, &models.MediaSupplyEpisodeEvent{}, &models.MediaSourceYieldDaily{}); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		`CREATE UNIQUE INDEX fixture_boundary_identity ON pods_boundary_observations(tenant_id,boundary,probe_kind,probe_id,content_item_id)`,
		`CREATE UNIQUE INDEX fixture_episode_event_identity ON media_supply_episode_events(tenant_id,event_key)`,
	} {
		if err := db.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func TestPodsSupplyConsumerSessionProvesOriginalGenerationAndIdentity(t *testing.T) {
	db := consumerProofDB(t)
	now, oldGeneration, newGeneration := time.Now().UTC(), uuid.New(), uuid.New()
	item := models.ContentItem{PublicID: uuid.New(), TenantID: "tenant-a"}
	session := models.ConsumerFeedSession{ID: uuid.New(), TenantID: item.TenantID, IdentityScope: "session:installation-a", FeedType: "pods", Snapshot: datatypes.JSON([]byte(`[{"id":"` + item.PublicID.String() + `"}]`)), ActiveGenerationID: &oldGeneration, CreatedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour)}
	if err := db.Create(&session).Error; err != nil {
		t.Fatal(err)
	}
	observation, ok := podsSessionBoundary(db, item, session.ID, session.IdentityScope, "exact_view", uuid.NewString(), now)
	if !ok || *observation.GenerationID != oldGeneration || *observation.GenerationID == newGeneration {
		t.Fatal("session lost its original generation")
	}
	for _, invalid := range []struct {
		item     models.ContentItem
		identity string
		at       time.Time
	}{
		{models.ContentItem{PublicID: item.PublicID, TenantID: "tenant-b"}, session.IdentityScope, now},
		{models.ContentItem{PublicID: uuid.New(), TenantID: item.TenantID}, session.IdentityScope, now},
		{item, "session:another-installation", now}, {item, session.IdentityScope, now.Add(-20 * time.Minute)},
	} {
		if _, valid := podsSessionBoundary(db, invalid.item, session.ID, invalid.identity, "exact_view", uuid.NewString(), invalid.at); valid {
			t.Fatal("unproven session observation admitted")
		}
	}
}

func TestPodsSupplyOnlyViewsCreateExactExposure(t *testing.T) {
	db := consumerProofDB(t)
	now, generation := time.Now().UTC(), uuid.New()
	item := models.ContentItem{PublicID: uuid.New(), TenantID: "tenant-a"}
	identity := "installation-a"
	session := models.ConsumerFeedSession{ID: uuid.New(), TenantID: item.TenantID, IdentityScope: "session:" + identity, FeedType: "pods", ActiveGenerationID: &generation, CreatedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour), Snapshot: datatypes.JSON([]byte(`[{"id":"` + item.PublicID.String() + `"}]`))}
	if err := db.Create(&session).Error; err != nil {
		t.Fatal(err)
	}
	metadata, _ := json.Marshal(map[string]string{"feed_session_id": session.ID.String()})
	for _, kind := range []models.InteractionType{models.InteractionTypeMeaningful, models.InteractionTypeComplete, models.InteractionTypeView} {
		interaction := models.UserInteraction{PublicID: uuid.New(), Type: kind, SessionID: &identity, CreatedAt: now, Metadata: metadata}
		if err := db.Transaction(func(tx *gorm.DB) error { recordPodsViewBoundary(tx, item, interaction); return nil }); err != nil {
			t.Fatal(err)
		}
	}
	var rows []models.PodsBoundaryObservation
	if err := db.Find(&rows).Error; err != nil || len(rows) != 1 || rows[0].Boundary != "exact_view" {
		t.Fatalf("exposure = %+v, %v", rows, err)
	}
}

func TestPodsSupplySyntheticReturnDoesNotBecomeConsumerYield(t *testing.T) {
	db := consumerProofDB(t)
	item, source := uuid.New(), uuid.New()
	if err := db.Exec(`INSERT INTO content_items(public_id,tenant_id,content_source_id)VALUES(?,'tenant-a',?)`, item, source).Error; err != nil {
		t.Fatal(err)
	}
	row := models.PodsBoundaryObservation{TenantID: "tenant-a", ContentItemID: item, Boundary: "feed_return", ProbeKind: "direct_probe", ProbeID: uuid.NewString(), Verdict: "present", ObservedAt: time.Now().UTC()}
	if err := supply.RecordPodsBoundaryObservation(db, row); err != nil {
		t.Fatal(err)
	}
	var count int64
	db.Model(&models.MediaSourceYieldDaily{}).Count(&count)
	if count != 0 {
		t.Fatal("synthetic probe manufactured consumer yield")
	}
	row.ProbeKind = "frozen_session"
	row.ProbeID = uuid.NewString()
	if err := supply.RecordPodsBoundaryObservation(db, row); err != nil {
		t.Fatal(err)
	}
	var result models.MediaSourceYieldDaily
	if err := db.First(&result).Error; err != nil || result.PublicReturns != 1 {
		t.Fatalf("real return yield %+v %v", result, err)
	}
}

func TestPodsSupplyRecoveryRequiresConsecutiveIndependentObservations(t *testing.T) {
	db := consumerProofDB(t)
	now := time.Now().UTC()
	episode := models.MediaSupplyEpisode{PublicID: uuid.New(), TenantID: "tenant-a"}
	proof := mediaSupplyEpisodeResolutionProof{ResolvedAt: now.Add(-6 * time.Minute), ConsumerEvidenceIDs: []string{"render-1", "view-1"}}
	if clean, err := recordMediaSupplyCleanResolutionObservation(db, episode, proof); err != nil || clean {
		t.Fatal("first clean closed episode", err)
	}
	if err := appendMediaSupplyEpisodeEventPayload(db, episode, "resolution_not_clean", "failed", []byte(`{}`), now.Add(-5*time.Minute)); err != nil {
		t.Fatal(err)
	}
	proof.ResolvedAt = now.Add(-4 * time.Minute)
	proof.ConsumerEvidenceIDs = []string{"render-2", "view-2"}
	if clean, err := recordMediaSupplyCleanResolutionObservation(db, episode, proof); err != nil || clean {
		t.Fatal("clean-failed-clean closed episode", err)
	}
	proof.ResolvedAt = now.Add(-2 * time.Minute)
	if clean, err := recordMediaSupplyCleanResolutionObservation(db, episode, proof); err != nil || clean {
		t.Fatal("reused evidence closed episode", err)
	}
	proof.ResolvedAt = now
	proof.ConsumerEvidenceIDs = []string{"render-3", "view-3"}
	if clean, err := recordMediaSupplyCleanResolutionObservation(db, episode, proof); err != nil || !clean {
		t.Fatal("two fresh consecutive observations did not recover", err)
	}
}

func TestPodsSupplyRecoveryRejectsOldOrUncorrelatedConsumerProof(t *testing.T) {
	now, generation, session, item := time.Now().UTC(), uuid.New(), uuid.New(), uuid.New()
	render := models.PodsBoundaryObservation{PublicID: uuid.New(), ContentItemID: item, FeedSessionID: &session, GenerationID: &generation, Boundary: "page_render", Verdict: "present", ObservedAt: now.Add(-time.Minute)}
	view := render
	view.PublicID = uuid.New()
	view.Boundary = "exact_view"
	proof := supply.PodsExposureProof{Verdict: "return_path_observed", EvidenceCompleteness: "complete", ReturnedIDs: []string{item.String()}, ActiveGenerationID: &generation, ConsumerObservations: []models.PodsBoundaryObservation{render, view}}
	if verdict, _ := mediaSupplyConsumerEvidence(proof, now.Add(-2*time.Minute), now); verdict != supply.ResolutionObservationPresent {
		t.Fatal("correlated fresh proof refused")
	}
	if verdict, _ := mediaSupplyConsumerEvidence(proof, now.Add(-30*time.Second), now); verdict == supply.ResolutionObservationPresent {
		t.Fatal("pre-episode proof accepted")
	}
	other := uuid.New()
	proof.ConsumerObservations[1].FeedSessionID = &other
	if verdict, _ := mediaSupplyConsumerEvidence(proof, time.Time{}, now); verdict == supply.ResolutionObservationPresent {
		t.Fatal("unrelated session evidence joined")
	}
}

func TestPodsSupplyRenderIngestUsesConfiguredTenantAndFrozenGeneration(t *testing.T) {
	if os.Getenv("CMS_TEST_ADMIN_URL") == "" && os.Getenv("CMS_TEST_DATABASE_URL") == "" {
		t.Skip("guarded disposable PostgreSQL required")
	}
	db := experienceTestDB(t)
	t.Setenv("DEFAULT_TENANT_ID", "tenant-a")
	if err := db.Exec(`CREATE TABLE content_items(public_id uuid PRIMARY KEY,tenant_id varchar(64),source_run_request_id bigint)`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&models.ConsumerFeedSession{}, &models.PodsBoundaryObservation{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`CREATE UNIQUE INDEX fixture_render_identity ON pods_boundary_observations(tenant_id,boundary,probe_kind,probe_id,content_item_id)`).Error; err != nil {
		t.Fatal(err)
	}
	policy := models.DefaultExperiencePolicy("tenant-a")
	policy.IngestEnabled = true
	if err := db.Create(&policy).Error; err != nil {
		t.Fatal(err)
	}
	item, generation, sessionID := uuid.New(), uuid.New(), uuid.New()
	if err := db.Exec(`INSERT INTO content_items(public_id,tenant_id) VALUES(?,'tenant-a')`, item).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	session := models.ConsumerFeedSession{ID: sessionID, TenantID: "tenant-a", IdentityScope: "session:installation", FeedType: "pods", ActiveGenerationID: &generation, CreatedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour), Snapshot: datatypes.JSON([]byte(`[{"id":"` + item.String() + `"}]`))}
	if err := db.Create(&session).Error; err != nil {
		t.Fatal(err)
	}
	event := validRuxEvent()
	event.SchemaVersion = 2
	event.EventType = "feed_rendered"
	event.Surface = "pods"
	event.OccurredAt = now.Format(time.RFC3339Nano)
	itemID, feedID := item.String(), sessionID.String()
	event.ContentID = &itemID
	event.FeedSessionID = &feedID
	raw, _ := json.Marshal(event)
	result := ingestExperienceFixture(t, db, []json.RawMessage{raw})
	if result.Code != 200 {
		t.Fatal(result.Body.String())
	}
	var saved models.ExperienceEvent
	if err := db.First(&saved).Error; err != nil || saved.TenantID != "tenant-a" {
		t.Fatalf("wrong tenant %+v %v", saved, err)
	}
	var observed models.PodsBoundaryObservation
	if err := db.First(&observed).Error; err != nil || observed.GenerationID == nil || *observed.GenerationID != generation {
		t.Fatalf("unproven render %+v %v", observed, err)
	}
}
