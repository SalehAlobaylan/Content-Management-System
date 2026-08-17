package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

// OperatorPolicy retains legacy launch/switch fields plus spend configuration.
// Runtime disable-only controls live in operator_capability_controls so normal
// Operator availability never depends on an environment launch mode.
type OperatorPolicy struct {
	TenantID                  string    `gorm:"primaryKey;type:varchar(64)" json:"tenant_id"`
	LaunchMode                string    `gorm:"type:varchar(16);not null" json:"launch_mode"`
	ReadEnabled               bool      `json:"read_enabled"`
	LLMEnabled                bool      `json:"llm_enabled"`
	ExecutionEnabled          bool      `json:"execution_enabled"`
	SchedulesEnabled          bool      `json:"schedules_enabled"`
	InteractiveSoftSpendLimit int64     `json:"interactive_soft_spend_limit"`
	DeepHardSpendLimit        int64     `json:"deep_hard_spend_limit"`
	UpdatedBy                 string    `json:"updated_by,omitempty"`
	CreatedAt                 time.Time `json:"created_at"`
	UpdatedAt                 time.Time `json:"updated_at"`
}

func (OperatorPolicy) TableName() string { return "operator_policies" }

type OperatorCapabilityControl struct {
	ID             uint       `gorm:"primaryKey" json:"-"`
	PublicID       uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID       string     `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	CapabilityKind string     `gorm:"type:varchar(16);not null" json:"capability_kind"`
	CapabilityKey  string     `gorm:"type:varchar(160);not null" json:"capability_key"`
	Disabled       bool       `json:"disabled"`
	Reason         string     `json:"reason"`
	ExpiresAt      *time.Time `json:"expires_at,omitempty"`
	ActorID        string     `json:"actor_id"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

func (OperatorCapabilityControl) TableName() string { return "operator_capability_controls" }

type OperatorThread struct {
	ID             uint       `gorm:"primaryKey" json:"-"`
	PublicID       uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID       string     `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	CreatorID      string     `json:"creator_id"`
	Title          string     `json:"title,omitempty"`
	Locale         string     `json:"locale"`
	LastActivityAt time.Time  `json:"last_activity_at"`
	ExpiresAt      time.Time  `json:"expires_at"`
	DeletedAt      *time.Time `json:"deleted_at,omitempty"`
	PinnedAt       *time.Time `json:"pinned_at,omitempty"`
	ArchivedAt     *time.Time `json:"archived_at,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

func (OperatorThread) TableName() string { return "operator_threads" }

// OperatorMessage is deletable conversation content. It intentionally has no
// relationship to action plans, approvals, or plan events: deleting a chat
// must never erase the permanent execution ledger.
type OperatorMessage struct {
	ID              uint           `gorm:"primaryKey" json:"-"`
	PublicID        uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	ThreadID        uint           `json:"-"`
	TenantID        string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	ActorType       string         `json:"actor_type"`
	ActorID         string         `json:"actor_id,omitempty"`
	MessageKind     string         `json:"message_kind"`
	InvestigationID *uint          `json:"-"`
	PlanID          *uint          `json:"-"`
	Content         datatypes.JSON `gorm:"type:jsonb" json:"content"`
	CreatedAt       time.Time      `json:"created_at"`
}

func (OperatorMessage) TableName() string { return "operator_messages" }

type OperatorInvestigation struct {
	ID                uint           `gorm:"primaryKey" json:"-"`
	PublicID          uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID          string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	ThreadID          *uint          `json:"-"`
	ActorID           string         `json:"actor_id"`
	State             string         `json:"state"`
	VisibleContext    datatypes.JSON `gorm:"type:jsonb" json:"visible_context"`
	Request           datatypes.JSON `gorm:"type:jsonb" json:"-"`
	PacketFingerprint string         `json:"packet_fingerprint,omitempty"`
	ClaimToken        *uuid.UUID     `gorm:"type:uuid" json:"-"`
	ClaimExpiresAt    *time.Time     `json:"claim_expires_at,omitempty"`
	Locale            string         `json:"locale"`
	StartedAt         time.Time      `json:"started_at"`
	FinishedAt        *time.Time     `json:"finished_at,omitempty"`
	ReadAt            *time.Time     `json:"read_at,omitempty"`
	ExpiresAt         time.Time      `json:"expires_at"`
	ErrorClass        string         `json:"error_class,omitempty"`
	CreatedAt         time.Time      `json:"created_at"`
	UpdatedAt         time.Time      `json:"updated_at"`
}

func (OperatorInvestigation) TableName() string { return "operator_investigations" }

type OperatorInvestigationEvent struct {
	ID              uint           `gorm:"primaryKey" json:"-"`
	InvestigationID uint           `json:"-"`
	TenantID        string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	Sequence        int64          `json:"sequence"`
	EventType       string         `json:"event_type"`
	Payload         datatypes.JSON `gorm:"type:jsonb" json:"payload"`
	CreatedAt       time.Time      `json:"created_at"`
}

func (OperatorInvestigationEvent) TableName() string { return "operator_investigation_events" }

type OperatorEvidence struct {
	ID                 uint           `gorm:"primaryKey" json:"-"`
	PublicID           uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	InvestigationID    uint           `json:"-"`
	TenantID           string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	EvidenceID         string         `json:"evidence_id"`
	Authority          string         `json:"authority"`
	Domain             string         `json:"domain"`
	AdapterKey         string         `json:"adapter_key"`
	AdapterVersion     string         `json:"adapter_version"`
	RequiredPermission string         `json:"required_permission"`
	RecordRefs         datatypes.JSON `gorm:"type:jsonb" json:"record_refs"`
	DeepLink           string         `json:"deep_link"`
	ObservedAt         time.Time      `json:"observed_at"`
	FetchedAt          time.Time      `json:"fetched_at"`
	MaxAgeSeconds      int            `json:"max_age_seconds"`
	ExpiresAt          time.Time      `json:"expires_at"`
	ContentHash        string         `json:"content_hash"`
	SourceVersion      string         `json:"source_version"`
	Availability       string         `json:"availability"`
	CreatedAt          time.Time      `json:"created_at"`
}

func (OperatorEvidence) TableName() string { return "operator_evidence" }

// OperatorRecommendation is the durable, CMS-authored recommendation record.
// Feedback changes its presentation state only; it never creates action
// authority or changes the underlying domain object.
type OperatorRecommendation struct {
	ID                uint           `gorm:"primaryKey" json:"-"`
	PublicID          uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	InvestigationID   *uint          `json:"-"`
	TenantID          string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	RecommendationKey string         `gorm:"type:varchar(150);not null" json:"recommendation_key"`
	SubjectType       string         `gorm:"type:varchar(80);not null" json:"subject_type"`
	SubjectID         string         `gorm:"type:varchar(200);not null" json:"subject_id"`
	Rank              int            `json:"rank"`
	State             string         `gorm:"type:varchar(24);not null" json:"state"`
	EvidenceIDs       datatypes.JSON `gorm:"type:jsonb" json:"evidence_ids"`
	Payload           datatypes.JSON `gorm:"type:jsonb" json:"payload"`
	ExpiresAt         time.Time      `json:"expires_at"`
	CreatedAt         time.Time      `json:"created_at"`
	UpdatedAt         time.Time      `json:"updated_at"`
}

func (OperatorRecommendation) TableName() string { return "operator_recommendations" }

type OperatorRecommendationFeedback struct {
	ID               uint       `gorm:"primaryKey" json:"-"`
	PublicID         uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	RecommendationID uint       `json:"-"`
	TenantID         string     `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	ActorID          string     `gorm:"type:varchar(255);not null" json:"actor_id"`
	FeedbackKind     string     `gorm:"type:varchar(24);not null" json:"feedback_kind"`
	Reason           string     `json:"reason,omitempty"`
	ExpiresAt        *time.Time `json:"expires_at,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
}

func (OperatorRecommendationFeedback) TableName() string { return "operator_recommendation_feedback" }

// OperatorSchedule persists only a read investigation template. It stores no
// plan, confirmation, bearer token, executor, or action target.
type OperatorSchedule struct {
	ID                  uint           `gorm:"primaryKey" json:"-"`
	PublicID            uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID            string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	CreatorID           string         `gorm:"type:varchar(255);not null" json:"creator_id"`
	OwnerID             string         `gorm:"type:varchar(255);not null" json:"owner_id"`
	State               string         `gorm:"type:varchar(24);not null" json:"state"`
	Scope               datatypes.JSON `gorm:"type:jsonb" json:"scope"`
	Template            datatypes.JSON `gorm:"type:jsonb" json:"template"`
	Locale              string         `gorm:"type:varchar(8);not null" json:"locale"`
	Cadence             string         `gorm:"type:varchar(100);not null" json:"cadence"`
	RequiredPermissions datatypes.JSON `gorm:"type:jsonb" json:"required_permissions"`
	AccessVersion       string         `gorm:"type:varchar(200);not null" json:"access_version"`
	NextRunAt           *time.Time     `json:"next_run_at,omitempty"`
	PausedReason        string         `gorm:"type:varchar(200)" json:"paused_reason,omitempty"`
	CreatedAt           time.Time      `json:"created_at"`
	UpdatedAt           time.Time      `json:"updated_at"`
}

func (OperatorSchedule) TableName() string { return "operator_schedules" }

type OperatorScheduleRun struct {
	ID                    uint       `gorm:"primaryKey" json:"-"`
	PublicID              uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	ScheduleID            uint       `json:"-"`
	TenantID              string     `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	State                 string     `gorm:"type:varchar(24);not null" json:"state"`
	ClaimToken            *uuid.UUID `gorm:"type:uuid" json:"-"`
	ClaimExpiresAt        *time.Time `json:"claim_expires_at,omitempty"`
	ResultInvestigationID *uint      `json:"-"`
	PauseReason           string     `gorm:"type:varchar(200)" json:"pause_reason,omitempty"`
	StartedAt             time.Time  `json:"started_at"`
	FinishedAt            *time.Time `json:"finished_at,omitempty"`
	CreatedAt             time.Time  `json:"created_at"`
	UpdatedAt             time.Time  `json:"updated_at"`
}

func (OperatorScheduleRun) TableName() string { return "operator_schedule_runs" }

type OperatorScheduleEvent struct {
	ID         uint           `gorm:"primaryKey" json:"-"`
	ScheduleID uint           `json:"-"`
	TenantID   string         `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	Sequence   int64          `json:"sequence"`
	EventType  string         `gorm:"type:varchar(48);not null" json:"event_type"`
	ActorID    string         `gorm:"type:varchar(255)" json:"actor_id,omitempty"`
	Payload    datatypes.JSON `gorm:"type:jsonb" json:"payload"`
	CreatedAt  time.Time      `json:"created_at"`
}

func (OperatorScheduleEvent) TableName() string { return "operator_schedule_events" }

// An investigation share is an invitation, not an evidence grant. Every read
// rechecks the receiver's own IAM permissions against each evidence row.
type OperatorInvestigationShare struct {
	ID              uint       `gorm:"primaryKey" json:"-"`
	PublicID        uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	InvestigationID uint       `json:"-"`
	TenantID        string     `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	RecipientID     string     `gorm:"type:varchar(255);not null" json:"recipient_id"`
	CreatedBy       string     `gorm:"type:varchar(255);not null" json:"created_by"`
	State           string     `gorm:"type:varchar(16);not null" json:"state"`
	CreatedAt       time.Time  `json:"created_at"`
	RevokedAt       *time.Time `json:"revoked_at,omitempty"`
}

func (OperatorInvestigationShare) TableName() string { return "operator_investigation_shares" }

// OperatorShadowRun is a redacted, CMS-owned qualification record. It holds
// only packet metadata and timing, never evidence payloads, prompts, model
// output, admin credentials, or action authority.
type OperatorShadowRun struct {
	ID                uint       `gorm:"primaryKey" json:"-"`
	PublicID          uuid.UUID  `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID          string     `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	ActorID           string     `gorm:"type:varchar(255);not null" json:"actor_id"`
	AccessVersionHash string     `gorm:"type:char(64);not null" json:"access_version_hash"`
	Domain            string     `gorm:"type:varchar(100);not null" json:"domain"`
	Locale            string     `gorm:"type:varchar(8);not null" json:"locale"`
	State             string     `gorm:"type:varchar(24);not null" json:"state"`
	PacketFingerprint string     `gorm:"type:char(64)" json:"packet_fingerprint,omitempty"`
	EvidenceCount     int        `json:"evidence_count"`
	UnknownCount      int        `json:"unknown_count"`
	ConflictCount     int        `json:"conflict_count"`
	LatencyMS         int64      `json:"latency_ms"`
	ErrorClass        string     `gorm:"type:varchar(120)" json:"error_class,omitempty"`
	StartedAt         time.Time  `json:"started_at"`
	FinishedAt        *time.Time `json:"finished_at,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
}

func (OperatorShadowRun) TableName() string { return "operator_shadow_runs" }

// OperatorShadowEnrollment is a deliberate tenant/admin qualification scope.
// Shadow never invents a default tenant or a synthetic administrator.
type OperatorShadowEnrollment struct {
	ID         uint      `gorm:"primaryKey" json:"-"`
	PublicID   uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID   string    `gorm:"type:varchar(64);not null;index" json:"tenant_id"`
	UserID     string    `gorm:"type:varchar(255);not null" json:"user_id"`
	State      string    `gorm:"type:varchar(16);not null" json:"state"`
	EnrolledBy string    `gorm:"type:varchar(255);not null" json:"enrolled_by"`
	Reason     string    `gorm:"type:varchar(500);not null" json:"reason"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

func (OperatorShadowEnrollment) TableName() string { return "operator_shadow_enrollments" }

// OperatorShadowQualificationFailure records a readiness failure even when no
// shadow run can be created (for example, no tenant is enrolled or IAM is
// unavailable). It is a redacted ledger, not a retry queue.
type OperatorShadowQualificationFailure struct {
	ID         uint      `gorm:"primaryKey" json:"-"`
	PublicID   uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	TenantID   string    `gorm:"type:varchar(64);index" json:"tenant_id,omitempty"`
	ActorID    string    `gorm:"type:varchar(255)" json:"actor_id,omitempty"`
	Domain     string    `gorm:"type:varchar(100)" json:"domain,omitempty"`
	Locale     string    `gorm:"type:varchar(8)" json:"locale,omitempty"`
	Failure    string    `gorm:"type:varchar(80);not null" json:"failure"`
	ObservedAt time.Time `json:"observed_at"`
	CreatedAt  time.Time `json:"created_at"`
}

func (OperatorShadowQualificationFailure) TableName() string {
	return "operator_shadow_qualification_failures"
}

// OperatorShadowAssessment is an immutable, redacted reviewer or isolated
// fixture verdict for one real CMS shadow snapshot. It is deliberately kept
// separate from OperatorShadowRun so a completed snapshot can never be
// rewritten to improve a qualification score after the fact.
type OperatorShadowAssessment struct {
	ID                           uint      `gorm:"primaryKey" json:"-"`
	PublicID                     uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	ShadowRunID                  uint      `gorm:"uniqueIndex" json:"-"`
	EvaluationCaseID             string    `gorm:"type:varchar(160);not null;uniqueIndex" json:"evaluation_case_id"`
	Cohort                       string    `gorm:"type:varchar(16);not null" json:"cohort"`
	Grounded                     bool      `json:"grounded"`
	UsefulRating                 int       `json:"useful_rating"`
	DomainToolSelectionCorrect   bool      `json:"domain_tool_selection_correct"`
	UnsupportedCertaintyCritical int       `json:"unsupported_certainty_critical"`
	FaultCase                    string    `gorm:"type:varchar(80)" json:"fault_case,omitempty"`
	Outcome                      string    `gorm:"type:varchar(24);not null" json:"outcome"`
	ReviewerID                   string    `gorm:"type:varchar(255);not null" json:"reviewer_id"`
	Provenance                   string    `gorm:"type:varchar(32);not null" json:"provenance"`
	ResultFingerprint            string    `gorm:"type:char(64);not null" json:"result_fingerprint"`
	CreatedAt                    time.Time `json:"created_at"`
}

func (OperatorShadowAssessment) TableName() string { return "operator_shadow_assessments" }

// OperatorShadowReport is a CMS-owned immutable snapshot of qualification
// rows. Signoffs bind to ReportDigest; sealing does not edit the payload.
type OperatorShadowReport struct {
	ID                  uint           `gorm:"primaryKey" json:"-"`
	PublicID            uuid.UUID      `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	SchemaVersion       string         `gorm:"type:varchar(64);not null" json:"schema_version"`
	EnvironmentIdentity string         `gorm:"type:varchar(64);not null" json:"environment_identity"`
	LaunchMode          string         `gorm:"type:varchar(16);not null" json:"launch_mode"`
	State               string         `gorm:"type:varchar(16);not null" json:"state"`
	Payload             datatypes.JSON `gorm:"type:jsonb;not null" json:"-"`
	ReportDigest        string         `gorm:"type:char(64);not null;uniqueIndex" json:"report_digest"`
	Seal                string         `gorm:"type:char(64)" json:"seal,omitempty"`
	CreatedAt           time.Time      `json:"created_at"`
	SealedAt            *time.Time     `json:"sealed_at,omitempty"`
}

func (OperatorShadowReport) TableName() string { return "operator_shadow_reports" }

type OperatorShadowReportRun struct {
	ID       uint `gorm:"primaryKey" json:"-"`
	ReportID uint `gorm:"uniqueIndex:idx_operator_shadow_report_run" json:"-"`
	RunID    uint `gorm:"uniqueIndex:idx_operator_shadow_report_run" json:"-"`
}

func (OperatorShadowReportRun) TableName() string { return "operator_shadow_report_runs" }

type OperatorShadowReportSignoff struct {
	ID           uint      `gorm:"primaryKey" json:"-"`
	PublicID     uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
	ReportID     uint      `gorm:"uniqueIndex:idx_operator_shadow_report_role" json:"-"`
	Role         string    `gorm:"type:varchar(32);not null;uniqueIndex:idx_operator_shadow_report_role" json:"role"`
	ActorID      string    `gorm:"type:varchar(255);not null" json:"actor_id"`
	ReportDigest string    `gorm:"type:char(64);not null" json:"report_digest"`
	SignedAt     time.Time `json:"signed_at"`
	CreatedAt    time.Time `json:"created_at"`
}

func (OperatorShadowReportSignoff) TableName() string { return "operator_shadow_report_signoffs" }
