package operator

import (
	"context"
	"testing"

	"content-management-system/src/models"
)

func TestCoordinatorPersistsContextBeforeDeterministicDegradedResponse(t *testing.T) {
	db := fabricTestDB(t)
	if err := db.AutoMigrate(&models.OperatorInvestigation{}, &models.OperatorInvestigationEvent{}, &models.OperatorEvidence{}); err != nil {
		t.Fatal(err)
	}
	_ = db.Exec("DELETE FROM operator_evidence").Error
	_ = db.Exec("DELETE FROM operator_investigation_events").Error
	_ = db.Exec("DELETE FROM operator_investigations").Error
	source := models.ContentSource{TenantID: "tenant-a", Name: "Source", Type: models.SourceTypeRSS, Category: models.SourceCategoryMedia}
	if err := db.Create(&source).Error; err != nil {
		t.Fatal(err)
	}
	fabric := NewContextFabric(db, DefaultAdapterRegistry())
	coordinator := NewInvestigationCoordinator(fabric, NewInvestigationStore(db), nil)
	result, err := coordinator.Run(context.Background(), approvalAccess(), RuntimePolicy{LaunchMode: LaunchModeShadow, ReadEnabled: true, LLMEnabled: false}, InvestigationInput{VisibleContext: approvalVisibleContext(source.PublicID.String()), Intent: IntentExplain, Locale: "en", Message: "Why is this pending?", Tier: "fast"})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Degraded || result.Investigation.ID == 0 || len(result.Packet.Evidence) == 0 || len(result.Response.Blocks) == 0 {
		t.Fatalf("unexpected coordinator result: %#v", result)
	}
	var events []models.OperatorInvestigationEvent
	if err := db.Where("investigation_id=?", result.Investigation.ID).Order("sequence ASC").Find(&events).Error; err != nil {
		t.Fatal(err)
	}
	if len(events) < 5 || events[0].EventType != "accepted" || events[1].EventType != "context_collecting" || events[2].EventType != "packet_ready" || events[len(events)-1].EventType != "done" {
		t.Fatalf("context must precede response events: %#v", events)
	}
}
