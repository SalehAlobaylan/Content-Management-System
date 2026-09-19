package models

import (
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"time"
)

type MediaChapterDraft struct {
	PublicID                    uuid.UUID      `gorm:"primaryKey;type:uuid" json:"id"`
	TenantID                    string         `json:"-"`
	ParentContentItemID         uuid.UUID      `json:"parent_id"`
	Revision                    int64          `json:"revision"`
	BaseProcessingGeneration    int64          `json:"base_processing_generation"`
	InputFingerprint            string         `json:"input_fingerprint"`
	TranscriptDigest            string         `json:"transcript_digest"`
	PolicyDigest                string         `json:"policy_digest"`
	Plan                        datatypes.JSON `json:"plan"`
	Provenance                  string         `json:"provenance"`
	Author                      string         `json:"author"`
	Validation                  datatypes.JSON `json:"validation"`
	CreatedAt                   time.Time      `json:"created_at"`
	AppliedRequestID            *uuid.UUID     `json:"applied_request_id,omitempty"`
	AppliedProcessingGeneration *int64         `json:"applied_processing_generation,omitempty"`
	ApplicationKey              *uuid.UUID     `json:"-"`
	AppliedAt                   *time.Time     `json:"applied_at,omitempty"`
}

func (MediaChapterDraft) TableName() string { return "media_chapter_drafts" }
