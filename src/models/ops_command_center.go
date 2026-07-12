package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

type OpsAttentionState struct {
	ID                  uint       `gorm:"primaryKey" json:"-"`
	TenantID            string     `gorm:"type:varchar(64);not null;uniqueIndex:idx_ops_attention_state_key,priority:1" json:"tenant_id"`
	AttentionKey        string     `gorm:"type:varchar(255);not null;uniqueIndex:idx_ops_attention_state_key,priority:2" json:"attention_key"`
	State               string     `gorm:"type:varchar(16);not null" json:"state"`
	SnoozedUntil        *time.Time `gorm:"type:timestamp" json:"snoozed_until,omitempty"`
	BaselineFingerprint string     `gorm:"type:varchar(255);not null" json:"baseline_fingerprint"`
	BaselineSeverity    string     `gorm:"type:varchar(16);not null" json:"baseline_severity"`
	BaselineCount       int        `gorm:"not null" json:"baseline_count"`
	ActorID             string     `gorm:"type:varchar(128);not null" json:"actor_id"`
	ActorEmail          string     `gorm:"type:varchar(255);not null" json:"actor_email"`
	CreatedAt           time.Time  `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt           time.Time  `gorm:"autoUpdateTime" json:"updated_at"`
}

func (OpsAttentionState) TableName() string { return "ops_attention_state" }

type OpsBriefingCursor struct {
	ID         uint      `gorm:"primaryKey" json:"-"`
	TenantID   string    `gorm:"type:varchar(64);not null;uniqueIndex:idx_ops_briefing_cursor,priority:1" json:"tenant_id"`
	AdminID    string    `gorm:"type:varchar(128);not null;uniqueIndex:idx_ops_briefing_cursor,priority:2" json:"admin_id"`
	LastSeenAt time.Time `gorm:"type:timestamp;not null" json:"last_seen_at"`
	CreatedAt  time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt  time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (OpsBriefingCursor) TableName() string { return "ops_briefing_cursors" }

type OpsFleetCommand struct {
	ID              uint           `gorm:"primaryKey" json:"-"`
	PublicID        uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID        string         `gorm:"type:varchar(64);not null;uniqueIndex:idx_ops_command_idempotency,priority:1" json:"tenant_id"`
	Command         string         `gorm:"type:varchar(24);not null" json:"command"`
	Scope           string         `gorm:"type:varchar(96);not null" json:"scope"`
	Reason          string         `gorm:"type:text;not null" json:"reason"`
	TTLMinutes      *int           `json:"ttl_minutes,omitempty"`
	IdempotencyKey  string         `gorm:"type:varchar(128);not null;uniqueIndex:idx_ops_command_idempotency,priority:2" json:"-"`
	SourceCommandID *uuid.UUID     `gorm:"type:uuid" json:"source_command_id,omitempty"`
	ActorID         string         `gorm:"type:varchar(128);not null" json:"actor_id"`
	ActorEmail      string         `gorm:"type:varchar(255);not null" json:"actor_email"`
	Status          string         `gorm:"type:varchar(16);not null" json:"status"`
	Counts          datatypes.JSON `gorm:"type:jsonb" json:"counts"`
	CreatedAt       time.Time      `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt       time.Time      `gorm:"autoUpdateTime" json:"updated_at"`
}

func (OpsFleetCommand) TableName() string { return "ops_fleet_commands" }

type OpsFleetCommandAction struct {
	ID                 uint       `gorm:"primaryKey" json:"-"`
	PublicID           uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	CommandID          uint       `gorm:"not null;index" json:"-"`
	SourceActionID     *uint      `json:"source_action_id,omitempty"`
	MemberKey          string     `gorm:"type:varchar(64);not null;index" json:"member_key"`
	LaneKey            string     `gorm:"type:varchar(64);not null;index" json:"lane_key"`
	TenantID           string     `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	PriorPausedUntil   *time.Time `gorm:"type:timestamp" json:"prior_paused_until,omitempty"`
	WrittenPausedUntil *time.Time `gorm:"type:timestamp" json:"written_paused_until,omitempty"`
	Outcome            string     `gorm:"type:varchar(24);not null" json:"outcome"`
	Guardrail          string     `gorm:"type:varchar(64)" json:"guardrail,omitempty"`
	Reason             string     `gorm:"type:text" json:"reason,omitempty"`
	CreatedAt          time.Time  `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt          time.Time  `gorm:"autoUpdateTime" json:"updated_at"`
}

func (OpsFleetCommandAction) TableName() string { return "ops_fleet_command_actions" }
