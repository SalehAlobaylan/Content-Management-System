package controllers

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"content-management-system/src/supply"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

func validSourceRunReceiptBody(t *testing.T) sourceRunReceiptBody {
	t.Helper()
	payload := []byte(`{"count":1,"kind":"provider_page"}`)
	digest := sha256.Sum256(payload)
	return sourceRunReceiptBody{
		TenantID: "tenant-a", ProducerEventKey: "event-a", SourceRunRequestID: uuid.NewString(), SourceRunAttemptID: uuid.NewString(),
		ExecutionUnitID: uuid.NewString(), ContentSourceID: uuid.NewString(), UnitJobID: "source-unit:abc", AttemptFenceToken: uuid.NewString(),
		ExecutionLeaseToken: uuid.NewString(), SchemaVersion: supply.ContractVersion, Producer: "aggregation", Stage: "fetch", EventType: "provider_page",
		Outcome: "new_items", Sequence: 1, ProducedAt: "2026-08-09T12:00:00Z", Payload: payload, PayloadDigest: hex.EncodeToString(digest[:]),
	}
}

func TestSourceRunReceiptBodyRejectsTamperedPayloadDigest(t *testing.T) {
	body := validSourceRunReceiptBody(t)
	body.PayloadDigest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if _, err := body.toInput(); err == nil {
		t.Fatal("tampered receipt payload digest was accepted")
	}
}

func TestDecodeSourceRunReceiptRejectsUnknownFields(t *testing.T) {
	gin.SetMode(gin.TestMode)
	body := validSourceRunReceiptBody(t)
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw[:len(raw)-1], []byte(`,"unexpected":true}`)...)
	req := httptest.NewRequest("POST", "/internal/source-run-receipts", bytes.NewReader(raw))
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx.Request = req
	if _, err := decodeSourceRunReceipt(ctx); err == nil {
		t.Fatal("receipt decoder accepted an unknown field")
	}
}

func TestSourceRunReceiptInputRequiresCurrentContractVersion(t *testing.T) {
	body := validSourceRunReceiptBody(t)
	body.SchemaVersion = "source-run/v0"
	if _, err := body.toInput(); err == nil {
		t.Fatal("unsupported source-run receipt schema version was accepted")
	}
}

func TestSourceRunReceiptUsesRegisteredOutcomeVocabulary(t *testing.T) {
	body := validSourceRunReceiptBody(t)
	body.Outcome = "invented"
	if _, err := body.toInput(); err == nil {
		t.Fatal("receipt parser must reject an unregistered outcome before persistence")
	}
}

func TestSourceRunReceiptRetentionEnvelopePreservesTheContractShape(t *testing.T) {
	body := validSourceRunReceiptBody(t)
	body.ExecutionLeaseExpiresAt = "2026-08-09T12:30:00Z"
	envelope, err := sourceRunReceiptEnvelope(body)
	if err != nil {
		t.Fatalf("retention envelope: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(envelope, &decoded); err != nil {
		t.Fatalf("decode retention envelope: %v", err)
	}
	if decoded["tenantId"] != body.TenantID || decoded["producerEventKey"] != body.ProducerEventKey || decoded["executionLeaseExpiresAt"] != body.ExecutionLeaseExpiresAt {
		t.Fatalf("retention envelope lost a CMS/worker contract identity: %#v", decoded)
	}
}
