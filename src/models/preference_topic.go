package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/pgvector/pgvector-go"
	"gorm.io/datatypes"
)

// TopicCategory is the parent layer of the canonical preference vocabulary.
type TopicCategory struct {
	ID        uint      `gorm:"primaryKey" json:"-"`
	TenantID  string    `gorm:"type:varchar(64);not null;default:'default';uniqueIndex:idx_topic_categories_tenant_slug,priority:1" json:"tenant_id"`
	Slug      string    `gorm:"type:text;not null;uniqueIndex:idx_topic_categories_tenant_slug,priority:2" json:"slug"`
	LabelAR   string    `gorm:"type:text;not null" json:"label_ar"`
	LabelEN   string    `gorm:"type:text;not null" json:"label_en"`
	SortOrder int       `gorm:"type:integer;not null;default:0" json:"sort_order"`
	Active    bool      `gorm:"not null;default:true" json:"active"`
	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (TopicCategory) TableName() string { return "topic_categories" }

// Topic is the canonical user/content subject vocabulary. Stories are event
// clusters; Topics are stable subjects users can declare or learn affinity for.
type Topic struct {
	ID           uint             `gorm:"primaryKey" json:"-"`
	PublicID     uuid.UUID        `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex:idx_topics_public_id" json:"id"`
	TenantID     string           `gorm:"type:varchar(64);not null;default:'default';uniqueIndex:idx_topics_tenant_slug,priority:1;index:idx_topics_tenant" json:"tenant_id"`
	Slug         string           `gorm:"type:text;not null;uniqueIndex:idx_topics_tenant_slug,priority:2" json:"slug"`
	LabelAR      string           `gorm:"type:text;not null" json:"label_ar"`
	LabelEN      string           `gorm:"type:text;not null" json:"label_en"`
	CategorySlug string           `gorm:"type:text;index:idx_topics_category" json:"category_slug,omitempty"`
	Centroid     *pgvector.Vector `gorm:"type:vector(1024)" json:"-"`
	MemberCount  int              `gorm:"type:integer;not null;default:0" json:"member_count"`
	Active       bool             `gorm:"not null;default:true" json:"active"`
	Featured     bool             `gorm:"not null;default:false" json:"featured"`
	CreatedFrom  string           `gorm:"type:text;not null;default:'mined'" json:"created_from"`
	// NeedsRemap is the explicit dirty-state boundary the Preferences Autopilot
	// consumes (plan §0.1.2). Human label/category/activation/approval changes set
	// it; the autopilot's dirty sweep clears it only after a successful limited
	// remap. Derived member-count/centroid writes use UpdateColumn and MUST NOT set
	// it, or routine maintenance would create an endless sweep loop.
	NeedsRemap bool      `gorm:"not null;default:false;index:idx_topics_needs_remap" json:"needs_remap"`
	CreatedAt  time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt  time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (Topic) TableName() string { return "topics" }

type TopicProposal struct {
	ID                uint           `gorm:"primaryKey" json:"id"`
	TenantID          string         `gorm:"type:varchar(64);not null;default:'default';uniqueIndex:idx_topic_proposals_tenant_slug,priority:1" json:"tenant_id"`
	SuggestedSlug     string         `gorm:"type:text;not null;uniqueIndex:idx_topic_proposals_tenant_slug,priority:2" json:"suggested_slug"`
	SuggestedLabelAR  string         `gorm:"type:text" json:"suggested_label_ar,omitempty"`
	SuggestedLabelEN  string         `gorm:"type:text" json:"suggested_label_en,omitempty"`
	SuggestedCategory string         `gorm:"type:text" json:"suggested_category,omitempty"`
	Evidence          datatypes.JSON `gorm:"type:jsonb" json:"evidence,omitempty"`
	Status            string         `gorm:"type:text;not null;default:'pending';index:idx_topic_proposals_status" json:"status"`
	MergedInto        *uuid.UUID     `gorm:"type:uuid" json:"merged_into,omitempty"`
	ResolvedBy        string         `gorm:"type:text" json:"resolved_by,omitempty"`
	ResolvedAt        *time.Time     `json:"resolved_at,omitempty"`

	// Preferences Autopilot advisor columns (plan §11). Scored state lives on the
	// proposal so the Console queue can sort by it directly. Confidence + flags are
	// deterministic; the cached embedding + input hash keep Enrichment calls bounded
	// and let the scorer skip already-embedded, unchanged proposals.
	Confidence         *float64         `gorm:"type:double precision" json:"confidence,omitempty"`
	AutopilotFlags     datatypes.JSON   `gorm:"type:jsonb" json:"autopilot_flags,omitempty"`
	Embedding          *pgvector.Vector `gorm:"type:vector(1024)" json:"-"`
	EmbeddingInputHash string           `gorm:"type:varchar(64)" json:"-"`
	EmbeddedAt         *time.Time       `gorm:"type:timestamp" json:"embedded_at,omitempty"`
	EnrichedAt         *time.Time       `gorm:"type:timestamp" json:"enriched_at,omitempty"`
	// Frozen prediction for the trust ladder (§15): recorded before human
	// resolution, compared on resolution. V1 records evidence only.
	PredictedVerdict  string     `gorm:"type:varchar(24)" json:"predicted_verdict,omitempty"`
	PredictedAt       *time.Time `gorm:"type:timestamp" json:"predicted_at,omitempty"`
	PredictionVersion string     `gorm:"type:varchar(24)" json:"prediction_version,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
}

func (TopicProposal) TableName() string { return "topic_proposals" }

type ContentItemTopic struct {
	ContentItemID uuid.UUID `gorm:"type:uuid;primaryKey;index:idx_cit_content" json:"content_item_id"`
	TopicID       uuid.UUID `gorm:"type:uuid;primaryKey;index:idx_cit_topic" json:"topic_id"`
	Score         float64   `gorm:"type:double precision;not null" json:"score"`
	CreatedAt     time.Time `gorm:"autoCreateTime" json:"created_at"`
}

func (ContentItemTopic) TableName() string { return "content_item_topics" }

type StoryTopic struct {
	StoryID   uuid.UUID `gorm:"type:uuid;primaryKey;index:idx_story_topics_story" json:"story_id"`
	TopicID   uuid.UUID `gorm:"type:uuid;primaryKey;index:idx_story_topics_topic" json:"topic_id"`
	Score     float64   `gorm:"type:double precision;not null" json:"score"`
	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
}

func (StoryTopic) TableName() string { return "story_topics" }

type UserTopicPref struct {
	TenantID  string    `gorm:"type:varchar(64);primaryKey;index:idx_user_topic_prefs_user" json:"tenant_id"`
	UserID    uuid.UUID `gorm:"type:uuid;primaryKey;index:idx_user_topic_prefs_user" json:"user_id"`
	TopicID   uuid.UUID `gorm:"type:uuid;primaryKey;index:idx_user_topic_prefs_topic" json:"topic_id"`
	State     string    `gorm:"type:text;not null" json:"state"`
	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (UserTopicPref) TableName() string { return "user_topic_prefs" }

type UserTopicAffinity struct {
	TenantID  string    `gorm:"type:varchar(64);primaryKey;index:idx_user_topic_affinity_user" json:"tenant_id"`
	UserID    uuid.UUID `gorm:"type:uuid;primaryKey;index:idx_user_topic_affinity_user" json:"user_id"`
	TopicID   uuid.UUID `gorm:"type:uuid;primaryKey;index:idx_user_topic_affinity_topic" json:"topic_id"`
	Score     float64   `gorm:"type:double precision;not null" json:"score"`
	Declared  bool      `gorm:"not null;default:false" json:"declared"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (UserTopicAffinity) TableName() string { return "user_topic_affinity" }

type UserCategoryAffinity struct {
	TenantID     string    `gorm:"type:varchar(64);primaryKey;index:idx_user_category_affinity_user" json:"tenant_id"`
	UserID       uuid.UUID `gorm:"type:uuid;primaryKey;index:idx_user_category_affinity_user" json:"user_id"`
	CategorySlug string    `gorm:"type:text;primaryKey" json:"category_slug"`
	Score        float64   `gorm:"type:double precision;not null" json:"score"`
	UpdatedAt    time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (UserCategoryAffinity) TableName() string { return "user_category_affinity" }

type PreferenceSettings struct {
	ID                uint      `gorm:"primaryKey" json:"-"`
	TenantID          string    `gorm:"type:varchar(64);not null;uniqueIndex:idx_preference_settings_tenant" json:"tenant_id"`
	ForYouEnabled     bool      `gorm:"not null;default:false" json:"foryou_enabled"`
	NewsEnabled       bool      `gorm:"not null;default:false" json:"news_enabled"`
	WForYou           float64   `gorm:"type:double precision;not null;default:0.30" json:"w_foryou"`
	WNews             float64   `gorm:"type:double precision;not null;default:0.15" json:"w_news"`
	WeightComplete    float64   `gorm:"type:double precision;not null;default:1.0" json:"weight_complete"`
	WeightBookmark    float64   `gorm:"type:double precision;not null;default:0.9" json:"weight_bookmark"`
	WeightShare       float64   `gorm:"type:double precision;not null;default:0.9" json:"weight_share"`
	WeightLike        float64   `gorm:"type:double precision;not null;default:0.7" json:"weight_like"`
	WeightComment     float64   `gorm:"type:double precision;not null;default:0.5" json:"weight_comment"`
	WeightView        float64   `gorm:"type:double precision;not null;default:0.2" json:"weight_view"`
	DecayHalfLifeDays float64   `gorm:"type:double precision;not null;default:30" json:"decay_half_life_days"`
	DeclaredPrior     float64   `gorm:"type:double precision;not null;default:3.0" json:"declared_prior"`
	CategoryDiscount  float64   `gorm:"type:double precision;not null;default:0.5" json:"category_discount"`
	CreatedAt         time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt         time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (PreferenceSettings) TableName() string { return "preference_settings" }

type PreferenceStat struct {
	ID               uint      `gorm:"primaryKey" json:"-"`
	TenantID         string    `gorm:"type:varchar(64);not null;uniqueIndex:idx_preference_stats_tenant_day,priority:1" json:"tenant_id"`
	Day              time.Time `gorm:"type:date;not null;uniqueIndex:idx_preference_stats_tenant_day,priority:2" json:"day"`
	UsersWithPrefs   int64     `gorm:"type:bigint;not null;default:0" json:"users_with_prefs"`
	BoostedServes    int64     `gorm:"type:bigint;not null;default:0" json:"boosted_serves"`
	TotalServes      int64     `gorm:"type:bigint;not null;default:0" json:"total_serves"`
	RecomputeRuns    int64     `gorm:"type:bigint;not null;default:0" json:"recompute_runs"`
	RecomputeMsTotal int64     `gorm:"type:bigint;not null;default:0" json:"recompute_ms_total"`
	CreatedAt        time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt        time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (PreferenceStat) TableName() string { return "preference_stats" }
