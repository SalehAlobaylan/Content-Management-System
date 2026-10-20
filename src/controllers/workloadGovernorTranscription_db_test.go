package controllers

import (
	"os"
	"testing"
	"time"

	"content-management-system/src/contentstage"
	"content-management-system/src/models"
	"content-management-system/src/tests/testdb"
	"content-management-system/src/workload"
	"github.com/google/uuid"
	"gorm.io/datatypes"
)

func TestWorkloadTranscriptionDBDefersBeforeJobAndSettlesOnlyRemoteProof(t *testing.T) {
	if os.Getenv("CMS_TEST_ADMIN_URL") == "" {
		t.Skip("requires guarded disposable Neon parent")
	}
	db := testdb.Open(t)
	if err := db.AutoMigrate(&models.ContentItem{}, &models.TranscriptionConfig{}, &models.TranscriptionJob{}); err != nil {
		t.Fatal(err)
	}
	sql, err := os.ReadFile("../../migrations/20261020010000_workload_governor.sql")
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Exec(string(sql)).Error; err != nil {
		t.Fatal(err)
	}
	p, err := workload.GetPolicy(db)
	if err != nil {
		t.Fatal(err)
	}
	p.Mode = workload.ModeEnforce
	p.EnforcedClasses = datatypes.JSON([]byte(`["stt_batch"]`))
	if err = db.Create(&p).Error; err != nil {
		t.Fatal(err)
	}
	for _, resource := range []string{"media_svc", "stt_slots"} {
		state := "normal"
		if resource == "stt_slots" {
			state = "saturated"
		}
		if err = db.Create(&models.WorkloadCapacitySnapshot{Resource: resource, State: state, Available: true, Healthy: true, ObservedAt: time.Now(), Evidence: datatypes.JSON([]byte(`{}`))}).Error; err != nil {
			t.Fatal(err)
		}
	}
	workload.SetSpendProvider(workloadSpendSignal)
	t.Cleanup(func() { workload.SetSpendProvider(nil) })
	url := "https://example.test/episode.mp3"
	duration := 600
	item := models.ContentItem{PublicID: uuid.New(), TenantID: "legacy-stt", Type: models.ContentTypePodcast, Source: models.SourceTypePodcast, Status: models.ContentStatusReady, MediaURL: &url, DurationSec: &duration}
	if err = db.Create(&item).Error; err != nil {
		t.Fatal(err)
	}
	cfg := models.TranscriptionConfig{TenantID: item.TenantID, MonthlyBudgetCapUsd: 100, MonthlyWindowStart: time.Now()}
	if err = db.Create(&cfg).Error; err != nil {
		t.Fatal(err)
	}
	if _, err = createAcceptedTranscriptionJob(db, &item, models.TranscriptionTriggerStudioAutopilot); !isWorkloadDeferred(err) {
		t.Fatalf("busy transcription was admitted: %v", err)
	}
	var count int64
	if err = db.Model(&models.TranscriptionJob{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("capacity deferral created a job: %d %v", count, err)
	}
	if err = db.First(&cfg, "tenant_id=?", item.TenantID).Error; err != nil || cfg.MonthlyReservedUsd != 0 {
		t.Fatalf("capacity deferral reserved money: %+v %v", cfg, err)
	}
	if err = db.Model(&models.WorkloadDecision{}).Where("event='request' AND verdict='defer'").Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("native transaction lost deferral evidence: %d %v", count, err)
	}
	if err = db.Model(&models.WorkloadCapacitySnapshot{}).Where("resource='stt_slots'").Update("state", "normal").Error; err != nil {
		t.Fatal(err)
	}
	job, err := createAcceptedTranscriptionJob(db, &item, models.TranscriptionTriggerStudioAutopilot)
	if err != nil {
		t.Fatal(err)
	}
	if job.PublicID == uuid.Nil || job.ReservedCostUsd <= 0 {
		t.Fatal("healthy admission lost native job or budget")
	}
	if err = workload.BeginOwner(db, item.TenantID, legacySTTWorkloadOwner, job.PublicID.String()); err != nil {
		t.Fatal(err)
	}
	if err = workload.HandoffOwner(db, item.TenantID, legacySTTWorkloadOwner, job.PublicID.String()); err != nil {
		t.Fatal(err)
	}
	if err = updateTranscriptionJobTerminal(db, job.PublicID.String(), models.TranscriptionJobStatusFailed, "local acknowledgement was lost"); err != nil {
		t.Fatal(err)
	}
	var permit models.WorkloadPermit
	if err = db.Where("owner_kind=? AND owner_id=?", legacySTTWorkloadOwner, job.PublicID.String()).First(&permit).Error; err != nil {
		t.Fatal(err)
	}
	if permit.State != workload.StateHandedOff {
		t.Fatal("local terminal status fabricated remote completion")
	}
	if err = db.Where("public_id=?", job.PublicID).First(&job).Error; err != nil {
		t.Fatal(err)
	}
	settleLegacySTTReceipt(db, job, nil)
	status := models.TranscriptionJobStatusFailed
	settleLegacySTTReceipt(db, job, &status)
	if err = db.First(&permit, "id=?", permit.ID).Error; err != nil || permit.State != "released" {
		t.Fatalf("remote terminal proof did not settle: %+v %v", permit, err)
	}
	// Only table presence and cutover mode are needed for this routing fixture.
	// The complete native schema/receipts have their own DB suite.
	if err = db.Exec(`CREATE TABLE content_stage_requests(id bigserial PRIMARY KEY); CREATE TABLE content_stage_attempts(id bigserial PRIMARY KEY); CREATE TABLE content_stage_receipts(id bigserial PRIMARY KEY); CREATE TABLE content_stage_events(id bigserial PRIMARY KEY)`).Error; err != nil {
		t.Fatal(err)
	}
	if err = db.AutoMigrate(&models.ContentStageCutover{}); err != nil {
		t.Fatal(err)
	}
	if err = db.Create(&models.ContentStageCutover{TenantID: item.TenantID, Lane: models.ContentStageLanePods, Mode: models.ContentStageCutoverDurableRequired}).Error; err != nil {
		t.Fatal(err)
	}
	contentstage.ResetSchemaAvailabilityCache()
	if err = db.Model(&models.WorkloadCapacitySnapshot{}).Where("resource='stt_slots'").Update("state", "saturated").Error; err != nil {
		t.Fatal(err)
	}
	if _, err = createAcceptedTranscriptionJob(db, &item, models.TranscriptionTriggerStudioAutopilot); err != nil {
		t.Fatalf("promoted stage was double-governed at legacy admission: %v", err)
	}
	if err = db.Model(&models.WorkloadPermit{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("promoted stage created a second legacy reservation: %d %v", count, err)
	}
}
