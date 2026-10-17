package economics

import (
	"encoding/json"
	"time"
)

const MaxRows = 500
const MaxKeys = 50000
const MaxEnvelope = 256 * 1024
const RuleVersion = "resource-v1"

var Resources = []string{"ai", "database", "app_compute", "storage", "delivery", "communications", "overhead"}

func ValidResource(s string) bool {
	for _, r := range Resources {
		if s == r {
			return true
		}
	}
	return false
}

// Registered operations bound cardinality and are versioned in code. Profiles must be calibrated before dollars are allocated.
var Operations = map[string]string{"pods_page": "pods", "news_page": "news", "search": "search", "content_read": "content", "transcript_read": "transcription", "interaction": "engagement", "upload": "upload", "transcription_request": "transcription", "auth": "auth", "unknown": "unknown", "processing": "processing", "storage": "storage", "playback": "pods", "news_open": "news"}
var Units = map[string]bool{"requests": true, "response_bytes": true, "errors": true, "work_ms": true, "job_elapsed_ms": true, "job_attempts": true, "job_failures": true, "db_work": true, "media_seconds": true, "news_opens": true, "news_open_events": true, "byte_seconds": true, "owned_byte_seconds": true, "items": true, "audio_seconds": true, "tokens": true}

type Policy struct {
	CollectionSince   *time.Time `json:"collection_since"`
	TenantID          string     `json:"tenant_id" gorm:"primaryKey"`
	CollectionEnabled bool       `json:"collection_enabled"`
	ReportsEnabled    bool       `json:"reports_enabled"`
	ImportsEnabled    bool       `json:"imports_enabled"`
	AllocationEnabled bool       `json:"allocation_enabled"`
	UpdatedBy         string     `json:"updated_by"`
	UpdatedAt         time.Time  `json:"updated_at"`
}

func (Policy) TableName() string { return "economics_policies" }

type Usage struct {
	TenantID  string    `json:"-"`
	Day       time.Time `json:"day"`
	Subject   string    `json:"subject"`
	Operation string    `json:"operation"`
	Unit      string    `json:"unit"`
	Variant   string    `json:"variant"`
	Value     int64     `json:"value"`
}

func (Usage) TableName() string { return "economics_usage_daily" }

type Batch struct {
	Health      bool      `json:"health"`
	Closed      bool      `json:"closed"`
	Version     int       `json:"schema_version"`
	TenantID    string    `json:"tenant_id"`
	Environment string    `json:"environment"`
	Epoch       string    `json:"epoch"`
	Sequence    int64     `json:"sequence"`
	StartedAt   time.Time `json:"started_at"`
	EndedAt     time.Time `json:"ended_at"`
	Lost        int64     `json:"lost"`
	Rows        []Usage   `json:"rows"`
}
type Source struct {
	ID         string          `json:"id"`
	TenantID   string          `json:"-"`
	Provider   string          `json:"provider"`
	Resource   string          `json:"resource"`
	Scope      string          `json:"scope"`
	Currency   string          `json:"currency"`
	Capability string          `json:"capability"`
	Status     string          `json:"status"`
	Note       string          `json:"note"`
	Config     json.RawMessage `json:"config" gorm:"type:jsonb"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

func (Source) TableName() string { return "economics_sources" }

type Cost struct {
	OccurredAt     *time.Time `json:"occurred_at"`
	PostedAt       *time.Time `json:"posted_at"`
	Observation    string     `gorm:"-" json:"-"`
	AppliedRule    string     `gorm:"-" json:"-"`
	ID             string     `json:"id"`
	TenantID       string     `json:"-"`
	SourceID       string     `json:"source_id"`
	LogicalKey     string     `json:"logical_key"`
	Revision       int        `json:"revision"`
	Provider       string     `json:"provider"`
	Resource       string     `json:"resource"`
	Feature        string     `json:"feature"`
	PeriodStart    time.Time  `json:"period_start"`
	PeriodEnd      time.Time  `json:"period_end"`
	Net            Amount     `json:"net" gorm:"type:numeric(30,12)"`
	BeforeBenefits *Amount    `json:"before_benefits" gorm:"type:numeric(30,12)"`
	Currency       string     `json:"currency"`
	Destination    string     `json:"destination"`
	Subject        string     `json:"subject"`
	AttributionRef string     `json:"-"`
	ContentID      string     `json:"content_id"`
	Method         string     `json:"method"`
	Metric         string     `json:"metric"`
	AllocableBPS   int        `json:"allocable_bps"`
	Reconciled     bool       `json:"reconciled"`
	Reason         string     `json:"reason"`
	Actor          string     `json:"actor"`
	Checksum       string     `json:"checksum"`
	CreatedAt      time.Time  `json:"created_at"`
}

func (Cost) TableName() string { return "economics_cost_revisions" }

type Attribution struct {
	ID           string    `json:"id"`
	TenantID     string    `json:"tenant_id"`
	Source       string    `json:"source"`
	Subject      string    `json:"subject"`
	OwnerSubject string    `json:"owner_subject"`
	Feature      string    `json:"feature"`
	Benefit      string    `json:"benefit"`
	ContentID    string    `json:"content_id"`
	CreatedAt    time.Time `json:"created_at"`
}

func (Attribution) TableName() string { return "economics_attributions" }

type Issue struct {
	ID            string          `json:"id"`
	TenantID      string          `json:"-"`
	Kind          string          `json:"kind"`
	Scope         string          `json:"scope"`
	Status        string          `json:"status"`
	Headline      string          `json:"headline"`
	Evidence      json.RawMessage `json:"evidence" gorm:"type:jsonb"`
	FirstSeenAt   time.Time       `json:"first_seen_at"`
	LastSeenAt    time.Time       `json:"last_seen_at"`
	Confirmations int             `json:"confirmations"`
	CloseReason   string          `json:"close_reason"`
}

func (Issue) TableName() string { return "economics_issues" }

type Run struct {
	ID          string     `json:"id"`
	TenantID    string     `json:"-"`
	Status      string     `json:"status"`
	StartedAt   time.Time  `json:"started_at"`
	CompletedAt *time.Time `json:"completed_at"`
	Rows        int        `json:"rows"`
	Error       string     `json:"error"`
}

func (Run) TableName() string { return "economics_runs" }

type Summary struct {
	ExpenseRevision int       `json:"expense_revision"`
	ExpenseKey      string    `json:"expense_key"`
	TenantID        string    `json:"-"`
	Revision        string    `json:"revision"`
	Grain           string    `json:"grain"`
	Day             time.Time `json:"day"`
	Subject         string    `json:"subject"`
	Resource        string    `json:"resource"`
	Feature         string    `json:"feature"`
	Direct          Amount    `json:"direct" gorm:"type:numeric(30,12)"`
	Shared          Amount    `json:"shared" gorm:"type:numeric(30,12)"`
	BeforeBenefits  *Amount   `json:"before_benefits" gorm:"type:numeric(30,12)"`
	Reconciled      bool      `json:"reconciled"`
	Method          string    `json:"method"`
	SourceID        string    `json:"source_id"`
	RuleVersion     string    `json:"rule_version"`
	PublishedAt     time.Time `json:"published_at"`
}

func (Summary) TableName() string { return "economics_cost_summaries" }
