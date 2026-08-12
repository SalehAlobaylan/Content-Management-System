package controllers

import (
	"content-management-system/src/models"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

const (
	mediaSourceRunTraceUnitLimit           = 64
	mediaSourceRunTraceReceiptLimit        = 128
	mediaSourceRunTraceVerificationLimit   = 32
	mediaSourceRunTraceReconciliationLimit = 128
	mediaSourceRunTraceItemLimit           = 128
	mediaSourceRunTraceAttemptLimit        = 32
	mediaSourceRunTraceTitleRuneLimit      = 300
)

// mediaSourceRunTraceResponse is a deliberately redacted, bounded diagnostic
// projection. It exposes CMS evidence identities and lifecycle state, never a
// queue job, provider configuration, receipt payload, lease/fence token, or
// mutable recovery capability.
type mediaSourceRunTraceResponse struct {
	SchemaVersion        string                              `json:"schema_version"`
	GeneratedAt          time.Time                           `json:"generated_at"`
	Request              mediaSourceRunTraceRequest          `json:"request"`
	Truncation           mediaSourceRunTraceTruncation       `json:"truncation"`
	Attempts             []mediaSourceRunTraceAttempt        `json:"attempts"`
	Units                []mediaSourceRunTraceUnit           `json:"units"`
	Receipts             []mediaSourceRunTraceReceipt        `json:"receipts"`
	VerificationTasks    []mediaSourceRunTraceVerification   `json:"verification_tasks"`
	ReconciliationEvents []mediaSourceRunTraceReconciliation `json:"reconciliation_events"`
	AttributedItems      []mediaSourceRunTraceItem           `json:"attributed_items"`
}

type mediaSourceRunTraceTruncation struct {
	Attempts             bool `json:"attempts"`
	Units                bool `json:"units"`
	Receipts             bool `json:"receipts"`
	VerificationTasks    bool `json:"verification_tasks"`
	ReconciliationEvents bool `json:"reconciliation_events"`
	AttributedItems      bool `json:"attributed_items"`
}

type mediaSourceRunTraceRequest struct {
	ID             uuid.UUID  `json:"id"`
	SourceID       uuid.UUID  `json:"source_id"`
	SourceName     string     `json:"source_name"`
	State          string     `json:"state"`
	EvidenceState  string     `json:"evidence_state"`
	Lane           string     `json:"lane"`
	Purpose        string     `json:"purpose"`
	RequestedAt    time.Time  `json:"requested_at"`
	AcceptedAt     *time.Time `json:"accepted_at,omitempty"`
	StartedAt      *time.Time `json:"started_at,omitempty"`
	FinishedAt     *time.Time `json:"finished_at,omitempty"`
	VerifiedAt     *time.Time `json:"verified_at,omitempty"`
	FailureClass   string     `json:"failure_class,omitempty"`
	FailureSummary string     `json:"failure_summary,omitempty"`
}

type mediaSourceRunTraceAttempt struct {
	ID                     uuid.UUID  `json:"id"`
	Number                 int        `json:"number"`
	State                  string     `json:"state"`
	StartedAt              *time.Time `json:"started_at,omitempty"`
	FinishedAt             *time.Time `json:"finished_at,omitempty"`
	VerificationRequiredAt *time.Time `json:"verification_required_at,omitempty"`
	FailureClass           string     `json:"failure_class,omitempty"`
	FailureSummary         string     `json:"failure_summary,omitempty"`
}

type mediaSourceRunTraceUnit struct {
	ID                   uuid.UUID  `json:"id"`
	AttemptID            uuid.UUID  `json:"attempt_id"`
	ParentUnitID         *uuid.UUID `json:"parent_unit_id,omitempty"`
	UnitType             string     `json:"unit_type"`
	PageID               string     `json:"page_id,omitempty"`
	BatchID              string     `json:"batch_id,omitempty"`
	State                string     `json:"state"`
	VerificationRequired bool       `json:"verification_required"`
	TerminalOutcome      string     `json:"terminal_outcome,omitempty"`
	EffectStartedAt      *time.Time `json:"effect_started_at,omitempty"`
	StartedAt            *time.Time `json:"started_at,omitempty"`
	FinishedAt           *time.Time `json:"finished_at,omitempty"`
}

type mediaSourceRunTraceReceipt struct {
	ID          uuid.UUID  `json:"id"`
	AttemptID   uuid.UUID  `json:"attempt_id"`
	UnitID      uuid.UUID  `json:"unit_id"`
	ContentID   *uuid.UUID `json:"content_id,omitempty"`
	Stage       string     `json:"stage"`
	EventType   string     `json:"event_type"`
	Outcome     string     `json:"outcome"`
	Sequence    int64      `json:"sequence"`
	PageID      string     `json:"page_id,omitempty"`
	BatchID     string     `json:"batch_id,omitempty"`
	FinalPage   bool       `json:"final_page"`
	CausationID string     `json:"causation_id,omitempty"`
	ProducedAt  time.Time  `json:"produced_at"`
	ObservedAt  time.Time  `json:"observed_at"`
}

type mediaSourceRunTraceVerification struct {
	ID               uuid.UUID  `json:"id"`
	AttemptID        *uuid.UUID `json:"attempt_id,omitempty"`
	UnitID           *uuid.UUID `json:"unit_id,omitempty"`
	Stage            string     `json:"stage"`
	EvidenceBoundary string     `json:"evidence_boundary"`
	CausationID      string     `json:"causation_id"`
	State            string     `json:"state"`
	AttemptCount     int        `json:"attempt_count"`
	NotBeforeAt      *time.Time `json:"not_before_at,omitempty"`
	DeadlineAt       *time.Time `json:"deadline_at,omitempty"`
	TerminalVerdict  string     `json:"terminal_verdict,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
}

type mediaSourceRunTraceReconciliation struct {
	ID          uuid.UUID  `json:"id"`
	AttemptID   *uuid.UUID `json:"attempt_id,omitempty"`
	UnitID      *uuid.UUID `json:"unit_id,omitempty"`
	TaskID      uuid.UUID  `json:"task_id"`
	Stage       string     `json:"stage"`
	Verdict     string     `json:"verdict"`
	ScopeType   string     `json:"scope_type"`
	ScopeID     string     `json:"scope_id"`
	CausationID string     `json:"causation_id"`
	ObservedAt  time.Time  `json:"observed_at"`
}

type mediaSourceRunTraceItem struct {
	ID               uuid.UUID  `json:"id"`
	ParentID         *uuid.UUID `json:"parent_id,omitempty"`
	Type             string     `json:"type"`
	Status           string     `json:"status"`
	Title            string     `json:"title,omitempty"`
	IsFeedUnit       bool       `json:"is_feed_unit"`
	FeedVisibility   string     `json:"feed_visibility"`
	ChapteringStatus *string    `json:"chaptering_status,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
}

func GetMediaCirculationSourceRunTrace(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	requestID, err := mediaSourceRunTraceRequestID(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid source-run request ID"})
		return
	}

	trace, err := loadMediaSourceRunTrace(c.MustGet("db").(*gorm.DB), principal.TenantID, requestID)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, gin.H{"message": "Source-run request not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Could not load source-run trace"})
		return
	}
	c.JSON(http.StatusOK, trace)
}

func mediaSourceRunTraceRequestID(raw string) (uuid.UUID, error) {
	return uuid.Parse(strings.TrimSpace(raw))
}

func loadMediaSourceRunTrace(db *gorm.DB, tenantID string, requestID uuid.UUID) (mediaSourceRunTraceResponse, error) {
	var request models.SourceRunRequest
	if err := db.Where("tenant_id = ? AND public_id = ? AND lane = ?", tenantID, requestID, models.SourceCategoryMedia).First(&request).Error; err != nil {
		return mediaSourceRunTraceResponse{}, err
	}
	var source models.ContentSource
	if err := db.Where("tenant_id = ? AND public_id = ?", tenantID, request.ContentSourceID).First(&source).Error; err != nil && err != gorm.ErrRecordNotFound {
		return mediaSourceRunTraceResponse{}, err
	}

	trace := mediaSourceRunTraceResponse{
		SchemaVersion: "media-source-run-trace/v1",
		GeneratedAt:   time.Now().UTC(),
		Request: mediaSourceRunTraceRequest{
			ID: request.PublicID, SourceID: request.ContentSourceID, SourceName: source.Name,
			State: request.State, EvidenceState: request.EvidenceState, Lane: request.Lane, Purpose: request.Purpose,
			RequestedAt: request.RequestedAt, AcceptedAt: request.AcceptedAt, StartedAt: request.StartedAt,
			FinishedAt: request.FinishedAt, VerifiedAt: request.VerifiedAt, FailureClass: request.FailureClass,
			FailureSummary: request.FailureSummary,
		},
		Attempts:             []mediaSourceRunTraceAttempt{},
		Units:                []mediaSourceRunTraceUnit{},
		Receipts:             []mediaSourceRunTraceReceipt{},
		VerificationTasks:    []mediaSourceRunTraceVerification{},
		ReconciliationEvents: []mediaSourceRunTraceReconciliation{},
		AttributedItems:      []mediaSourceRunTraceItem{},
	}

	var attempts []models.SourceRunAttempt
	if err := db.Where("tenant_id = ? AND source_run_request_id = ?", tenantID, request.PublicID).Order("attempt_number DESC, public_id DESC").Limit(mediaSourceRunTraceAttemptLimit + 1).Find(&attempts).Error; err != nil {
		return mediaSourceRunTraceResponse{}, err
	}
	trace.Truncation.Attempts = len(attempts) > mediaSourceRunTraceAttemptLimit
	if trace.Truncation.Attempts {
		attempts = attempts[:mediaSourceRunTraceAttemptLimit]
	}
	sort.Slice(attempts, func(i, j int) bool { return attempts[i].AttemptNumber < attempts[j].AttemptNumber })
	for _, attempt := range attempts {
		trace.Attempts = append(trace.Attempts, mediaSourceRunTraceAttempt{
			ID: attempt.PublicID, Number: attempt.AttemptNumber, State: attempt.State, StartedAt: attempt.StartedAt,
			FinishedAt: attempt.FinishedAt, VerificationRequiredAt: attempt.VerificationRequiredAt,
			FailureClass: attempt.FailureClass, FailureSummary: attempt.FailureSummary,
		})
	}

	var units []models.SourceRunExecutionUnit
	if err := db.Where("tenant_id = ? AND source_run_request_id = ?", tenantID, request.PublicID).Order("created_at DESC, public_id DESC").Limit(mediaSourceRunTraceUnitLimit + 1).Find(&units).Error; err != nil {
		return mediaSourceRunTraceResponse{}, err
	}
	trace.Truncation.Units = len(units) > mediaSourceRunTraceUnitLimit
	if trace.Truncation.Units {
		units = units[:mediaSourceRunTraceUnitLimit]
	}
	sort.Slice(units, func(i, j int) bool { return units[i].CreatedAt.Before(units[j].CreatedAt) })
	for _, unit := range units {
		trace.Units = append(trace.Units, mediaSourceRunTraceUnit{
			ID: unit.PublicID, AttemptID: unit.SourceRunAttemptID, ParentUnitID: unit.ParentUnitID,
			UnitType: unit.UnitType, PageID: unit.PageID, BatchID: unit.BatchID, State: unit.State,
			VerificationRequired: unit.VerificationRequired, TerminalOutcome: unit.TerminalOutcome,
			EffectStartedAt: unit.EffectStartedAt, StartedAt: unit.StartedAt, FinishedAt: unit.FinishedAt,
		})
	}

	var receipts []models.SourceRunReceipt
	if err := db.Where("tenant_id = ? AND source_run_request_id = ?", tenantID, request.PublicID).Order("observed_at DESC, public_id DESC").Limit(mediaSourceRunTraceReceiptLimit + 1).Find(&receipts).Error; err != nil {
		return mediaSourceRunTraceResponse{}, err
	}
	trace.Truncation.Receipts = len(receipts) > mediaSourceRunTraceReceiptLimit
	if trace.Truncation.Receipts {
		receipts = receipts[:mediaSourceRunTraceReceiptLimit]
	}
	for _, receipt := range receipts {
		trace.Receipts = append(trace.Receipts, mediaSourceRunTraceReceipt{
			ID: receipt.PublicID, AttemptID: receipt.SourceRunAttemptID, UnitID: receipt.ExecutionUnitID,
			ContentID: receipt.ContentItemID, Stage: receipt.Stage, EventType: receipt.EventType, Outcome: receipt.Outcome,
			Sequence: receipt.Sequence, PageID: receipt.PageID, BatchID: receipt.BatchID, FinalPage: receipt.FinalPage,
			CausationID: receipt.CausationID, ProducedAt: receipt.ProducedAt, ObservedAt: receipt.ObservedAt,
		})
	}

	var tasks []models.SourceRunVerificationTask
	if err := db.Where("tenant_id = ? AND source_run_request_id = ?", tenantID, request.PublicID).Order("created_at DESC, public_id DESC").Limit(mediaSourceRunTraceVerificationLimit + 1).Find(&tasks).Error; err != nil {
		return mediaSourceRunTraceResponse{}, err
	}
	trace.Truncation.VerificationTasks = len(tasks) > mediaSourceRunTraceVerificationLimit
	if trace.Truncation.VerificationTasks {
		tasks = tasks[:mediaSourceRunTraceVerificationLimit]
	}
	for _, task := range tasks {
		trace.VerificationTasks = append(trace.VerificationTasks, mediaSourceRunTraceVerification{
			ID: task.PublicID, AttemptID: task.SourceRunAttemptID, UnitID: task.ExecutionUnitID, Stage: task.Stage,
			EvidenceBoundary: task.EvidenceBoundary, CausationID: task.CausationID, State: task.State,
			AttemptCount: task.AttemptCount, NotBeforeAt: task.NotBeforeAt, DeadlineAt: task.DeadlineAt,
			TerminalVerdict: task.TerminalVerdict, CreatedAt: task.CreatedAt,
		})
	}

	var events []models.SourceRunReconciliationEvent
	if err := db.Where("tenant_id = ? AND source_run_request_id = ?", tenantID, request.PublicID).Order("observed_at DESC, public_id DESC").Limit(mediaSourceRunTraceReconciliationLimit + 1).Find(&events).Error; err != nil {
		return mediaSourceRunTraceResponse{}, err
	}
	trace.Truncation.ReconciliationEvents = len(events) > mediaSourceRunTraceReconciliationLimit
	if trace.Truncation.ReconciliationEvents {
		events = events[:mediaSourceRunTraceReconciliationLimit]
	}
	for _, event := range events {
		trace.ReconciliationEvents = append(trace.ReconciliationEvents, mediaSourceRunTraceReconciliation{
			ID: event.PublicID, AttemptID: event.SourceRunAttemptID, UnitID: event.ExecutionUnitID,
			TaskID: event.VerificationTaskID, Stage: event.Stage, Verdict: event.Verdict,
			ScopeType: event.ScopeType, ScopeID: event.ScopeID, CausationID: event.CausationID, ObservedAt: event.ObservedAt,
		})
	}

	var items []models.ContentItem
	if err := db.Where("tenant_id = ? AND source_run_request_id = ?", tenantID, request.ID).Order("created_at DESC, public_id DESC").Limit(mediaSourceRunTraceItemLimit + 1).Find(&items).Error; err != nil {
		return mediaSourceRunTraceResponse{}, err
	}
	trace.Truncation.AttributedItems = len(items) > mediaSourceRunTraceItemLimit
	if trace.Truncation.AttributedItems {
		items = items[:mediaSourceRunTraceItemLimit]
	}
	for _, item := range items {
		trace.AttributedItems = append(trace.AttributedItems, mediaSourceRunTraceItem{
			ID: item.PublicID, ParentID: item.ParentContentItemID, Type: string(item.Type), Status: string(item.Status),
			Title: traceContentTitle(item.Title), IsFeedUnit: item.IsFeedUnit, FeedVisibility: item.FeedVisibility,
			ChapteringStatus: item.ChapteringStatus, CreatedAt: item.CreatedAt,
		})
	}
	return trace, nil
}

func traceContentTitle(title *string) string {
	if title == nil {
		return ""
	}
	value := []rune(strings.TrimSpace(*title))
	if len(value) <= mediaSourceRunTraceTitleRuneLimit {
		return string(value)
	}
	return string(value[:mediaSourceRunTraceTitleRuneLimit]) + "…"
}
