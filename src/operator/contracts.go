// Package operator defines the stable, service-owned contracts used by Wahb
// Operator. It deliberately contains no HTTP or database concerns so contract
// validation can be shared by controllers, workers, and tests.
package operator

import (
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"
)

// IsInternalDeepLink accepts only a canonical Console-relative path. Evidence
// and effects can be persisted for days, so this boundary must not rely on a
// renderer to reject protocol-relative or malformed navigation values.
func IsInternalDeepLink(value string) bool {
	if !strings.HasPrefix(value, "/") || strings.HasPrefix(value, "//") || strings.Contains(value, "\\") {
		return false
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	parsed, err := url.ParseRequestURI(value)
	return err == nil && !parsed.IsAbs() && parsed.Host == "" && parsed.Path != ""
}

const ContractVersion = "wahb-operator/v1"

var (
	ErrInvalidContract = errors.New("invalid wahb operator contract")
	ErrForbiddenTool   = errors.New("forbidden wahb operator tool capability")
)

type Intent string

const (
	IntentExplain     Intent = "explain"
	IntentInvestigate Intent = "investigate"
	IntentRecommend   Intent = "recommend"
	IntentResolve     Intent = "resolve"
	IntentCompare     Intent = "compare"
)

var validIntents = map[Intent]struct{}{
	IntentExplain: {}, IntentInvestigate: {}, IntentRecommend: {}, IntentResolve: {}, IntentCompare: {},
}

type SubjectRef struct {
	Type  string `json:"type"`
	ID    string `json:"id"`
	Label string `json:"label,omitempty"`
}

type ExplicitSelection struct {
	Mode      string   `json:"mode"`
	IDs       []string `json:"ids,omitempty"`
	Count     int      `json:"count"`
	Truncated bool     `json:"truncated,omitempty"`
}

type DraftContext struct {
	Kind                 string         `json:"kind"`
	SafeNormalizedFields map[string]any `json:"safe_normalized_fields"`
}

// VisibleContext is the only browser contribution to an investigation. Its
// values are navigation hints, never operational evidence or plan arguments.
type VisibleContext struct {
	SchemaVersion    string             `json:"schema_version"`
	Domain           string             `json:"domain"`
	View             string             `json:"view"`
	Filters          map[string]any     `json:"filters"`
	Subjects         []SubjectRef       `json:"subjects"`
	Selection        *ExplicitSelection `json:"selection,omitempty"`
	Draft            *DraftContext      `json:"draft,omitempty"`
	AvailableIntents []Intent           `json:"available_intents"`
}

func (context VisibleContext) Validate() error {
	if context.SchemaVersion != ContractVersion {
		return fmt.Errorf("%w: unsupported visible-context schema version", ErrInvalidContract)
	}
	if strings.TrimSpace(context.Domain) == "" || strings.TrimSpace(context.View) == "" {
		return fmt.Errorf("%w: domain and view are required", ErrInvalidContract)
	}
	for key, value := range context.Filters {
		if strings.TrimSpace(key) == "" || !safeFilterValue(value) {
			return fmt.Errorf("%w: unsafe filter %q", ErrInvalidContract, key)
		}
	}
	for _, subject := range context.Subjects {
		if strings.TrimSpace(subject.Type) == "" || strings.TrimSpace(subject.ID) == "" {
			return fmt.Errorf("%w: subject type and id are required", ErrInvalidContract)
		}
	}
	if context.Selection != nil {
		if context.Selection.Mode != "explicit" && context.Selection.Mode != "filtered" {
			return fmt.Errorf("%w: invalid selection mode", ErrInvalidContract)
		}
		if context.Selection.Count < 0 || (context.Selection.Mode == "explicit" && (context.Selection.Truncated || len(context.Selection.IDs) != context.Selection.Count)) {
			return fmt.Errorf("%w: invalid explicit selection", ErrInvalidContract)
		}
	}
	if len(context.AvailableIntents) == 0 {
		return fmt.Errorf("%w: at least one intent is required", ErrInvalidContract)
	}
	for _, intent := range context.AvailableIntents {
		if _, ok := validIntents[intent]; !ok {
			return fmt.Errorf("%w: invalid intent %q", ErrInvalidContract, intent)
		}
	}
	return nil
}

func safeFilterValue(value any) bool {
	switch typed := value.(type) {
	case string, bool, float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	case []string:
		return len(typed) <= 100
	case []any:
		if len(typed) > 100 {
			return false
		}
		for _, item := range typed {
			if _, ok := item.(string); !ok {
				return false
			}
		}
		return true
	default:
		return false
	}
}

type EvidenceAvailability string

const (
	EvidenceAvailable   EvidenceAvailability = "available"
	EvidencePartial     EvidenceAvailability = "partial"
	EvidenceStale       EvidenceAvailability = "stale"
	EvidenceUnavailable EvidenceAvailability = "unavailable"
	EvidenceConflicting EvidenceAvailability = "conflicting"
)

type EvidenceAuthority string

const (
	EvidenceLive      EvidenceAuthority = "live"
	EvidenceDerived   EvidenceAuthority = "derived"
	EvidenceTemporal  EvidenceAuthority = "temporal"
	EvidenceRetrieved EvidenceAuthority = "retrieved"
	EvidenceMemory    EvidenceAuthority = "memory"
)

type EvidenceRef struct {
	EvidenceID         string               `json:"evidence_id"`
	Authority          EvidenceAuthority    `json:"authority"`
	Domain             string               `json:"domain"`
	AdapterKey         string               `json:"adapter_key"`
	AdapterVersion     string               `json:"adapter_version"`
	TenantID           string               `json:"tenant_id"`
	RequiredPermission string               `json:"required_permission"`
	RecordRefs         []SubjectRef         `json:"record_refs"`
	DeepLink           string               `json:"deep_link"`
	ObservedAt         time.Time            `json:"observed_at"`
	FetchedAt          time.Time            `json:"fetched_at"`
	MaxAgeSeconds      int                  `json:"max_age_seconds"`
	ExpiresAt          time.Time            `json:"expires_at"`
	ContentHash        string               `json:"content_hash"`
	SourceVersion      string               `json:"source_version"`
	Availability       EvidenceAvailability `json:"availability"`
}

func (evidence EvidenceRef) Validate() error {
	if strings.TrimSpace(evidence.EvidenceID) == "" || strings.TrimSpace(evidence.Domain) == "" || strings.TrimSpace(evidence.AdapterKey) == "" || strings.TrimSpace(evidence.AdapterVersion) == "" || strings.TrimSpace(evidence.TenantID) == "" || strings.TrimSpace(evidence.RequiredPermission) == "" || strings.TrimSpace(evidence.ContentHash) == "" || strings.TrimSpace(evidence.SourceVersion) == "" {
		return fmt.Errorf("%w: evidence identity is incomplete", ErrInvalidContract)
	}
	if evidence.Authority != EvidenceLive && evidence.Authority != EvidenceDerived && evidence.Authority != EvidenceTemporal && evidence.Authority != EvidenceRetrieved && evidence.Authority != EvidenceMemory {
		return fmt.Errorf("%w: invalid evidence authority", ErrInvalidContract)
	}
	if evidence.ObservedAt.IsZero() || evidence.FetchedAt.IsZero() || evidence.MaxAgeSeconds <= 0 || evidence.ExpiresAt.Before(evidence.FetchedAt) {
		return fmt.Errorf("%w: evidence freshness is invalid", ErrInvalidContract)
	}
	if !IsInternalDeepLink(evidence.DeepLink) {
		return fmt.Errorf("%w: evidence deep link is not internal", ErrInvalidContract)
	}
	if evidence.Availability != EvidenceAvailable && evidence.Availability != EvidencePartial && evidence.Availability != EvidenceStale && evidence.Availability != EvidenceUnavailable && evidence.Availability != EvidenceConflicting {
		return fmt.Errorf("%w: invalid evidence availability", ErrInvalidContract)
	}
	return nil
}

type Fact struct {
	Key         string   `json:"key"`
	Value       any      `json:"value"`
	EvidenceIDs []string `json:"evidence_ids"`
}

// Recommendation is CMS-authored, evidence-bound navigation guidance. It is
// not an action plan and cannot grant authority; a manual-only item must stay
// a deep link to its existing human-owned workflow.
type Recommendation struct {
	ID          string   `json:"id"`
	Kind        string   `json:"kind"`
	Title       string   `json:"title"`
	Summary     string   `json:"summary"`
	DeepLink    string   `json:"deep_link"`
	EvidenceIDs []string `json:"evidence_ids"`
	ManualOnly  bool     `json:"manual_only"`
}

func (recommendation Recommendation) Validate(evidenceIDs map[string]struct{}) error {
	if strings.TrimSpace(recommendation.ID) == "" || strings.TrimSpace(recommendation.Kind) == "" || strings.TrimSpace(recommendation.Title) == "" || strings.TrimSpace(recommendation.Summary) == "" || !IsInternalDeepLink(recommendation.DeepLink) || len(recommendation.EvidenceIDs) == 0 || len(recommendation.EvidenceIDs) > 20 {
		return fmt.Errorf("%w: invalid recommendation", ErrInvalidContract)
	}
	for _, evidenceID := range recommendation.EvidenceIDs {
		if _, ok := evidenceIDs[evidenceID]; !ok {
			return fmt.Errorf("%w: recommendation cites unknown evidence", ErrInvalidContract)
		}
	}
	return nil
}

type DecisionPacket struct {
	SchemaVersion       string           `json:"schema_version"`
	PacketID            string           `json:"packet_id"`
	Fingerprint         string           `json:"fingerprint"`
	TenantID            string           `json:"tenant_id"`
	ActorID             string           `json:"actor_id"`
	VisibleContext      VisibleContext   `json:"visible_context"`
	CollectionStartedAt time.Time        `json:"collection_started_at"`
	CollectionEndedAt   time.Time        `json:"collection_ended_at"`
	Completeness        string           `json:"completeness"`
	Facts               []Fact           `json:"facts"`
	Evidence            []EvidenceRef    `json:"evidence"`
	Recommendations     []Recommendation `json:"recommendations,omitempty"`
	Warnings            []string         `json:"warnings,omitempty"`
	Unknowns            []string         `json:"unknowns,omitempty"`
	Conflicts           []string         `json:"conflicts,omitempty"`
}

func (packet DecisionPacket) Validate() error {
	if packet.SchemaVersion != ContractVersion || strings.TrimSpace(packet.PacketID) == "" || strings.TrimSpace(packet.Fingerprint) == "" || strings.TrimSpace(packet.TenantID) == "" || strings.TrimSpace(packet.ActorID) == "" {
		return fmt.Errorf("%w: packet identity is incomplete", ErrInvalidContract)
	}
	if packet.CollectionEndedAt.Before(packet.CollectionStartedAt) || (packet.Completeness != "complete" && packet.Completeness != "partial") {
		return fmt.Errorf("%w: packet collection metadata is invalid", ErrInvalidContract)
	}
	if err := packet.VisibleContext.Validate(); err != nil {
		return err
	}
	evidenceIDs := make(map[string]struct{}, len(packet.Evidence))
	for _, evidence := range packet.Evidence {
		if err := evidence.Validate(); err != nil {
			return err
		}
		if evidence.TenantID != packet.TenantID {
			return fmt.Errorf("%w: evidence tenant mismatch", ErrInvalidContract)
		}
		if _, exists := evidenceIDs[evidence.EvidenceID]; exists {
			return fmt.Errorf("%w: duplicate evidence", ErrInvalidContract)
		}
		evidenceIDs[evidence.EvidenceID] = struct{}{}
	}
	for _, fact := range packet.Facts {
		if strings.TrimSpace(fact.Key) == "" || len(fact.EvidenceIDs) == 0 {
			return fmt.Errorf("%w: facts require a key and evidence", ErrInvalidContract)
		}
		for _, evidenceID := range fact.EvidenceIDs {
			if _, ok := evidenceIDs[evidenceID]; !ok {
				return fmt.Errorf("%w: fact cites unknown evidence", ErrInvalidContract)
			}
		}
	}
	if len(packet.Recommendations) > 4 {
		return fmt.Errorf("%w: packets support one primary and at most three secondary recommendations", ErrInvalidContract)
	}
	seenRecommendations := make(map[string]struct{}, len(packet.Recommendations))
	for _, recommendation := range packet.Recommendations {
		if err := recommendation.Validate(evidenceIDs); err != nil {
			return err
		}
		if _, exists := seenRecommendations[recommendation.ID]; exists {
			return fmt.Errorf("%w: duplicate recommendation", ErrInvalidContract)
		}
		seenRecommendations[recommendation.ID] = struct{}{}
	}
	seenConflicts := make(map[string]struct{}, len(packet.Conflicts))
	for _, conflict := range packet.Conflicts {
		if strings.TrimSpace(conflict) == "" {
			return fmt.Errorf("%w: conflicts must be explicit", ErrInvalidContract)
		}
		if _, exists := seenConflicts[conflict]; exists {
			return fmt.Errorf("%w: duplicate conflict", ErrInvalidContract)
		}
		seenConflicts[conflict] = struct{}{}
	}
	return nil
}

type RiskTier string

const (
	RiskRead    RiskTier = "read"
	RiskRoutine RiskTier = "routine"
	RiskHigh    RiskTier = "high_impact"
)

type ToolDescriptor struct {
	Key                string   `json:"key"`
	Version            string   `json:"version"`
	OwnerDomain        string   `json:"owner_domain"`
	TargetType         string   `json:"target_type"`
	ArgumentSchema     string   `json:"argument_schema"`
	RequiredPermission string   `json:"required_permission"`
	RiskTier           RiskTier `json:"risk_tier"`
	Batchable          bool     `json:"batchable"`
	TargetCap          int      `json:"target_cap"`
	Executor           string   `json:"executor"`
	Monitor            string   `json:"monitor"`
	Verifier           string   `json:"verifier"`
	Idempotency        string   `json:"idempotency"`
	Cancellation       string   `json:"cancellation"`
	Rollback           string   `json:"rollback"`
	Contingencies      []string `json:"contingencies"`
	AffectedDomains    []string `json:"affected_domains"`
	LocalizedActionKey string   `json:"localized_action_key"`
}

func (descriptor ToolDescriptor) Validate() error {
	for _, value := range []string{descriptor.Key, descriptor.Version, descriptor.OwnerDomain, descriptor.TargetType, descriptor.ArgumentSchema, descriptor.RequiredPermission, descriptor.Executor, descriptor.Monitor, descriptor.Verifier, descriptor.Idempotency, descriptor.Cancellation, descriptor.Rollback, descriptor.LocalizedActionKey} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%w: tool descriptor has a required empty field", ErrInvalidContract)
		}
	}
	if descriptor.RiskTier != RiskRead && descriptor.RiskTier != RiskRoutine && descriptor.RiskTier != RiskHigh {
		return fmt.Errorf("%w: invalid risk tier", ErrInvalidContract)
	}
	if descriptor.TargetCap < 1 || descriptor.TargetCap > 20 || (!descriptor.Batchable && descriptor.TargetCap != 1) {
		return fmt.Errorf("%w: tool target cap must be 1..20 and non-batch tools must target one record", ErrInvalidContract)
	}
	if len(descriptor.AffectedDomains) == 0 || len(descriptor.Contingencies) == 0 || isForbiddenTool(descriptor) {
		return ErrForbiddenTool
	}
	for _, contingency := range descriptor.Contingencies {
		if strings.TrimSpace(contingency) == "" {
			return fmt.Errorf("%w: tool descriptor contingencies must be explicit", ErrInvalidContract)
		}
	}
	return nil
}

func isForbiddenTool(descriptor ToolDescriptor) bool {
	if PermanentlyForbiddenOperatorTool(descriptor.Key) || PermanentlyForbiddenOperatorTool(descriptor.OwnerDomain) || PermanentlyForbiddenOperatorTool(descriptor.Executor) {
		return true
	}
	if FeedRecoveryOperatorToolForbidden(descriptor.Key) {
		return true
	}
	if RetentionOperatorToolForbidden(descriptor.Key) {
		return true
	}
	needle := strings.ToLower(strings.Join([]string{descriptor.Key, descriptor.OwnerDomain, descriptor.TargetType, descriptor.ArgumentSchema, descriptor.Executor}, " "))
	for _, forbidden := range []string{"delete", "purge", "iam.", "password", "secret", "token", "migration", "raw sql", "shell", "restart", "arbitrary url", "queue"} {
		if strings.Contains(needle, forbidden) {
			return true
		}
	}
	return false
}

// SortedEvidenceIDs provides stable serialization input for plan fingerprints.
func SortedEvidenceIDs(evidence []EvidenceRef) []string {
	ids := make([]string, 0, len(evidence))
	for _, item := range evidence {
		ids = append(ids, item.EvidenceID)
	}
	sort.Strings(ids)
	return ids
}
