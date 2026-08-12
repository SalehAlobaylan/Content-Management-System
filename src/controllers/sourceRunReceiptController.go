package controllers

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/supply"
	"content-management-system/src/utils"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

const maxSourceRunReceiptBytes int64 = 64 << 10

type sourceRunReceiptBody struct {
	TenantID                string          `json:"tenant_id"`
	ProducerEventKey        string          `json:"producer_event_key"`
	SourceRunRequestID      string          `json:"source_run_request_id"`
	SourceRunAttemptID      string          `json:"source_run_attempt_id"`
	ExecutionUnitID         string          `json:"execution_unit_id"`
	ContentSourceID         string          `json:"content_source_id"`
	UnitJobID               string          `json:"unit_job_id"`
	AttemptFenceToken       string          `json:"attempt_fence_token"`
	ExecutionLeaseToken     string          `json:"execution_lease_token"`
	ExecutionLeaseExpiresAt string          `json:"execution_lease_expires_at"`
	SchemaVersion           string          `json:"schema_version"`
	Producer                string          `json:"producer"`
	Stage                   string          `json:"stage"`
	EventType               string          `json:"event_type"`
	Outcome                 string          `json:"outcome"`
	Sequence                int64           `json:"sequence"`
	PageID                  string          `json:"page_id"`
	BatchID                 string          `json:"batch_id"`
	FinalPage               bool            `json:"final_page"`
	CausationID             string          `json:"causation_id"`
	ProducedAt              string          `json:"produced_at"`
	Payload                 json.RawMessage `json:"payload"`
	PayloadDigest           string          `json:"payload_digest"`
}

// InternalCreateSourceRunReceipt accepts immutable producer evidence only. It
// is deliberately separate from the legacy source-run telemetry endpoint: a
// receipt cannot be used to synthesize a queue state or bypass a unit lease.
func InternalCreateSourceRunReceipt(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	if !supply.ProjectionWorkerHealthy(time.Now().UTC()) {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Source-run evidence projection is unavailable"})
		return
	}
	body, err := decodeSourceRunReceipt(c)
	if err != nil {
		recordSourceRunReceiptRejection(db, c, "invalid_body")
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid source-run receipt"})
		return
	}
	principal, ok := utils.GetMachinePrincipal(c)
	if !ok || string(principal) != strings.TrimSpace(body.Producer) {
		recordSourceRunReceiptRejection(db, c, "producer_mismatch")
		c.JSON(http.StatusForbidden, gin.H{"message": "Source-run receipt producer is not authorized"})
		return
	}
	input, err := body.toInput()
	if err != nil {
		recordSourceRunReceiptRejection(db, c, "invalid_contract")
		c.JSON(http.StatusUnprocessableEntity, gin.H{"message": "Source-run receipt contract is invalid"})
		return
	}
	receipt, created, err := supply.RecordReceipt(db, input)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			recordSourceRunReceiptRejection(db, c, "unknown_unit")
			c.JSON(http.StatusNotFound, gin.H{"message": "Source-run receipt target was not found"})
			return
		}
		recordSourceRunReceiptRejection(db, c, "stale_or_invalid_transition")
		c.JSON(http.StatusConflict, gin.H{"message": "Source-run receipt lease or fence is no longer current"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": receipt.PublicID, "duplicate": !created, "observed_at": receipt.ObservedAt})
}

// InternalRetainSourceRunReceipt records only an immutable delivery envelope.
// It is available even when projection is paused, because retention is a
// convergence path rather than an admission of completed work.
func InternalRetainSourceRunReceipt(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	body, err := decodeSourceRunReceipt(c)
	if err != nil {
		recordSourceRunReceiptRejection(db, c, "invalid_retention_body")
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid retained source-run receipt"})
		return
	}
	principal, ok := utils.GetMachinePrincipal(c)
	if !ok || string(principal) != strings.TrimSpace(body.Producer) {
		recordSourceRunReceiptRejection(db, c, "retention_producer_mismatch")
		c.JSON(http.StatusForbidden, gin.H{"message": "Source-run receipt producer is not authorized"})
		return
	}
	input, err := body.toInput()
	if err != nil {
		recordSourceRunReceiptRejection(db, c, "invalid_retention_contract")
		c.JSON(http.StatusUnprocessableEntity, gin.H{"message": "Source-run receipt contract is invalid"})
		return
	}
	envelope, err := sourceRunReceiptEnvelope(body)
	if err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"message": "Source-run receipt envelope is invalid"})
		return
	}
	retained, created, err := supply.RetainReceipt(db, input, envelope)
	if err != nil {
		writeSourceRunContractError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": retained.PublicID, "duplicate": !created, "state": retained.State})
}

// InternalMarkSourceRunReceiptDelivered records the outcome of normal receipt
// delivery. The client chooses only the immutable producer key it registered;
// CMS verifies the matching receipt ledger row before changing retention state.
func InternalMarkSourceRunReceiptDelivered(c *gin.Context) {
	var body struct {
		TenantID         string `json:"tenant_id"`
		ProducerEventKey string `json:"producer_event_key"`
	}
	if err := decodeStrictJSON(c, &body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid retained receipt delivery"})
		return
	}
	retained, err := supply.MarkRetainedReceiptDelivered(c.MustGet("db").(*gorm.DB), strings.TrimSpace(body.TenantID), strings.TrimSpace(body.ProducerEventKey))
	if err != nil {
		writeSourceRunContractError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": retained.PublicID, "state": retained.State})
}

func sourceRunReceiptEnvelope(body sourceRunReceiptBody) (datatypes.JSON, error) {
	if len(body.Payload) == 0 || !json.Valid(body.Payload) {
		return nil, errors.New("receipt payload is invalid")
	}
	value := map[string]any{
		"contractVersion": supply.ContractVersion, "tenantId": body.TenantID, "producerEventKey": body.ProducerEventKey,
		"sourceRunRequestId": body.SourceRunRequestID, "sourceRunAttemptId": body.SourceRunAttemptID, "executionUnitId": body.ExecutionUnitID,
		"contentSourceId": body.ContentSourceID, "unitJobId": body.UnitJobID, "attemptFenceToken": body.AttemptFenceToken,
		"executionLeaseToken": body.ExecutionLeaseToken, "executionLeaseExpiresAt": body.ExecutionLeaseExpiresAt, "schemaVersion": body.SchemaVersion, "producer": body.Producer,
		"stage": body.Stage, "eventType": body.EventType, "outcome": body.Outcome, "sequence": body.Sequence,
		"pageId": body.PageID, "batchId": body.BatchID, "finalPage": body.FinalPage, "causationId": body.CausationID,
		"producedAt": body.ProducedAt, "payloadDigest": body.PayloadDigest,
	}
	var payload any
	if err := json.Unmarshal(body.Payload, &payload); err != nil {
		return nil, err
	}
	value["payload"] = payload
	bytes, err := json.Marshal(value)
	return datatypes.JSON(bytes), err
}

func decodeSourceRunReceipt(c *gin.Context) (sourceRunReceiptBody, error) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxSourceRunReceiptBytes)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	var body sourceRunReceiptBody
	if err := decoder.Decode(&body); err != nil {
		return sourceRunReceiptBody{}, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return sourceRunReceiptBody{}, errors.New("source-run receipt body has trailing data")
	}
	return body, nil
}

func (body sourceRunReceiptBody) toInput() (supply.ReceiptInput, error) {
	parseID := func(raw string) (uuid.UUID, error) {
		return uuid.Parse(strings.TrimSpace(raw))
	}
	requestID, err := parseID(body.SourceRunRequestID)
	if err != nil {
		return supply.ReceiptInput{}, err
	}
	attemptID, err := parseID(body.SourceRunAttemptID)
	if err != nil {
		return supply.ReceiptInput{}, err
	}
	unitID, err := parseID(body.ExecutionUnitID)
	if err != nil {
		return supply.ReceiptInput{}, err
	}
	sourceID, err := parseID(body.ContentSourceID)
	if err != nil {
		return supply.ReceiptInput{}, err
	}
	fence, err := parseID(body.AttemptFenceToken)
	if err != nil {
		return supply.ReceiptInput{}, err
	}
	lease, err := parseID(body.ExecutionLeaseToken)
	if err != nil {
		return supply.ReceiptInput{}, err
	}
	producedAt, err := time.Parse(time.RFC3339, strings.TrimSpace(body.ProducedAt))
	if err != nil {
		return supply.ReceiptInput{}, err
	}
	if strings.TrimSpace(body.SchemaVersion) != supply.ContractVersion {
		return supply.ReceiptInput{}, errors.New("unsupported source-run receipt schema version")
	}
	if body.Sequence < 0 || !supply.IsAllowedReceipt(supply.ReceiptStage(strings.TrimSpace(body.Stage)), supply.ReceiptEvent(strings.TrimSpace(body.EventType))) || !supply.IsKnownOutcome(supply.SourceRunOutcome(strings.TrimSpace(body.Outcome))) {
		return supply.ReceiptInput{}, errors.New("source-run receipt uses an unregistered stage, event, or outcome")
	}
	var payload map[string]any
	if len(body.Payload) == 0 || json.Unmarshal(body.Payload, &payload) != nil {
		return supply.ReceiptInput{}, errors.New("receipt payload must be a JSON object")
	}
	payloadBytes := bytes.TrimSpace(body.Payload)
	digest := sha256.Sum256(payloadBytes)
	if !strings.EqualFold(strings.TrimSpace(body.PayloadDigest), hex.EncodeToString(digest[:])) {
		return supply.ReceiptInput{}, errors.New("receipt payload digest does not match")
	}
	return supply.ReceiptInput{
		TenantID: strings.TrimSpace(body.TenantID), ProducerEventKey: strings.TrimSpace(body.ProducerEventKey),
		SourceRunRequestID: requestID, SourceRunAttemptID: attemptID, ExecutionUnitID: unitID, ContentSourceID: sourceID,
		UnitJobID: strings.TrimSpace(body.UnitJobID), AttemptFenceToken: fence, ExecutionLeaseToken: lease,
		SchemaVersion: strings.TrimSpace(body.SchemaVersion), Producer: strings.TrimSpace(body.Producer),
		Stage: strings.TrimSpace(body.Stage), EventType: strings.TrimSpace(body.EventType), Outcome: strings.TrimSpace(body.Outcome),
		Sequence: body.Sequence, PageID: strings.TrimSpace(body.PageID), BatchID: strings.TrimSpace(body.BatchID),
		FinalPage: body.FinalPage, CausationID: strings.TrimSpace(body.CausationID), Payload: datatypes.JSON(payloadBytes),
		PayloadDigest: strings.ToLower(strings.TrimSpace(body.PayloadDigest)), ProducedAt: producedAt,
	}, nil
}

// Invalid, unauthenticated, or untrusted data never retains a request body.
// This audit is intentionally best effort: an unavailable audit table must not
// turn a rejected receipt into accepted source-run evidence.
func recordSourceRunReceiptRejection(db *gorm.DB, c *gin.Context, reason string) {
	if db == nil {
		return
	}
	_ = db.Create(&models.SourceRunReceiptRejectionAudit{
		PublicID:     uuid.New(),
		Reason:       reason,
		RemoteClass:  "internal",
		PayloadBytes: c.Request.ContentLength,
		ObservedAt:   time.Now().UTC(),
	}).Error
}
