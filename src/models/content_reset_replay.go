package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

// Replay branches never replace a source's live scheduling/checkpoint fields.
// Spec and page bindings are immutable; native receipts supply progress.
type ContentResetReplayBranch struct {
	ID              uint           `gorm:"primaryKey" json:"-"`
	PublicID        uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid()" json:"id"`
	TenantID        string         `json:"tenant_id"`
	CampaignID      uint           `json:"-"`
	RevisionID      uint           `json:"-"`
	ContentSourceID uuid.UUID      `gorm:"type:uuid" json:"content_source_id"`
	ManifestHash    string         `json:"manifest_hash"`
	Spec            datatypes.JSON `gorm:"type:jsonb" json:"-"`
	SpecHash        string         `json:"spec_hash"`
	CreatedAt       time.Time      `json:"created_at"`
}

func (ContentResetReplayBranch) TableName() string { return "content_reset_replay_branches" }

type ContentResetReplayPage struct {
	ID                 uint      `gorm:"primaryKey" json:"-"`
	PublicID           uuid.UUID `gorm:"type:uuid;default:gen_random_uuid()" json:"id"`
	TenantID           string    `json:"tenant_id"`
	BranchID           uuid.UUID `gorm:"type:uuid" json:"branch_id"`
	Ordinal            int       `json:"ordinal"`
	SourceRunRequestID uuid.UUID `gorm:"type:uuid" json:"source_run_request_id"`
	InputCursor        string    `json:"-"`
	InputCursorHash    string    `json:"input_cursor_hash"`
	CreatedAt          time.Time `json:"created_at"`
}

func (ContentResetReplayPage) TableName() string { return "content_reset_replay_pages" }

// A checkpoint records provider observation, separately from processing and
// delivery. It grants no publication or live-checkpoint authority.
type ContentResetReplayCheckpoint struct {
	ID                uint      `gorm:"primaryKey" json:"-"`
	PublicID          uuid.UUID `gorm:"type:uuid;default:gen_random_uuid()" json:"id"`
	TenantID          string    `json:"tenant_id"`
	PageID            uuid.UUID `gorm:"type:uuid" json:"page_id"`
	ProviderReceiptID uuid.UUID `gorm:"type:uuid" json:"provider_receipt_id"`
	NextCursor        string    `json:"-"`
	NextCursorHash    string    `json:"next_cursor_hash"`
	Exhausted         bool      `json:"exhausted"`
	Observed          int       `json:"observed"`
	Admitted          int       `json:"admitted"`
	OutsideWindow     int       `json:"outside_window"`
	ObservedBytes     int64     `json:"observed_bytes"`
	CreatedAt         time.Time `json:"created_at"`
}

func (ContentResetReplayCheckpoint) TableName() string { return "content_reset_replay_checkpoints" }

// Provider admission is released independently from consumer verification.
type ContentResetProviderRelease struct {
	ID                 uint           `gorm:"primaryKey" json:"-"`
	PublicID           uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid()" json:"id"`
	TenantID           string         `json:"tenant_id"`
	PageID             uuid.UUID      `gorm:"type:uuid" json:"page_id"`
	SourceRunRequestID uuid.UUID      `gorm:"type:uuid" json:"source_run_request_id"`
	SourceRunAttemptID uuid.UUID      `gorm:"type:uuid" json:"source_run_attempt_id"`
	CommandID          uuid.UUID      `gorm:"type:uuid" json:"command_id"`
	Proof              datatypes.JSON `gorm:"type:jsonb" json:"proof"`
	ProofHash          string         `json:"proof_hash"`
	ReleasedAt         time.Time      `json:"released_at"`
}

func (ContentResetProviderRelease) TableName() string { return "content_reset_provider_releases" }

// ContentResetReplayHandoff is the immutable proof that a replay branch settled
// and its source returned to ordinary live scheduling. It records the observed
// provider boundary; it does not carry an opaque live cursor.
type ContentResetReplayHandoff struct {
	ID              uint      `gorm:"primaryKey" json:"-"`
	PublicID        uuid.UUID `gorm:"type:uuid;default:gen_random_uuid()" json:"id"`
	TenantID        string    `json:"tenant_id"`
	CampaignID      uint      `json:"-"`
	RevisionID      uint      `json:"-"`
	BranchID        uuid.UUID `gorm:"type:uuid" json:"branch_id"`
	ContentSourceID uuid.UUID `gorm:"type:uuid" json:"content_source_id"`
	Pages           int       `json:"pages"`
	ObservedUntil   time.Time `json:"observed_until"`
	SpecHash        string    `json:"spec_hash"`
	CommandID       uuid.UUID `gorm:"type:uuid" json:"command_id"`
	HandedOffAt     time.Time `json:"handed_off_at"`
}

func (ContentResetReplayHandoff) TableName() string { return "content_reset_replay_handoffs" }
