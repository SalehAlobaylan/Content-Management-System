package controllers

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"content-management-system/src/contentreset"
	"content-management-system/src/models"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	contentResetPlanPageSize = 500
	contentResetPlanTTL      = 30 * time.Minute
	contentResetPlanningTTL  = 24 * time.Hour
)

type contentResetScope struct {
	Kind                 string                 `json:"kind"`
	SourceIDs            []uuid.UUID            `json:"source_ids,omitempty"`
	ContentItemIDs       []uuid.UUID            `json:"content_item_ids,omitempty"`
	From                 string                 `json:"from,omitempty"`
	To                   string                 `json:"to,omitempty"`
	Statuses             []models.ContentStatus `json:"statuses,omitempty"`
	ProcessingGeneration *int64                 `json:"processing_generation,omitempty"`
}

type contentResetReplay struct {
	Mode        string      `json:"mode"`
	WindowDays  int         `json:"window_days,omitempty"`
	SourceScope string      `json:"source_scope,omitempty"`
	SourceIDs   []uuid.UUID `json:"source_ids,omitempty"`
}

type contentResetPlanRequest struct {
	Operation                          string             `json:"operation"`
	Lane                               string             `json:"lane"`
	Scope                              contentResetScope  `json:"scope"`
	CoveragePolicy                     string             `json:"coverage_policy"`
	InteractionPolicy                  string             `json:"interaction_policy"`
	IntakeAfter                        string             `json:"intake_after"`
	Replay                             contentResetReplay `json:"replay"`
	ProcessingStrategy                 string             `json:"processing_strategy"`
	CapacityStrategy                   string             `json:"capacity_strategy"`
	NewsAvailabilityExceptionRequested bool               `json:"news_availability_exception_requested,omitempty"`
	NewsAvailabilityExceptionReason    string             `json:"news_availability_exception_reason,omitempty"`
}

type contentResetBlocker struct {
	Code       string `json:"code"`
	Owner      string `json:"owner"`
	Reason     string `json:"reason"`
	NextAction string `json:"next_action"`
}

type contentResetCampaignDetail struct {
	Campaign models.ContentResetCampaign `json:"campaign"`
	Revision models.ContentResetRevision `json:"revision"`
}

type contentResetPublicEvidence struct {
	ID           uuid.UUID      `json:"id"`
	EvidenceKey  string         `json:"evidence_key"`
	EvidenceType string         `json:"evidence_type"`
	Owner        string         `json:"owner"`
	Payload      datatypes.JSON `json:"payload"`
	PayloadHash  string         `json:"payload_hash"`
	ObservedAt   time.Time      `json:"observed_at"`
}

var contentResetPreviewEvidenceTypes = []string{
	"planning_started",
	"planning_batch",
	"planning_completed",
	"preview_cancelled",
	"operator_decision",
}

type contentResetCandidate struct {
	ID                   int64                `gorm:"column:id"`
	PublicID             uuid.UUID            `gorm:"column:public_id"`
	TenantID             string               `gorm:"column:tenant_id"`
	Type                 models.ContentType   `gorm:"column:type"`
	Source               models.SourceType    `gorm:"column:source"`
	Status               models.ContentStatus `gorm:"column:status"`
	ProcessingGeneration int64                `gorm:"column:processing_generation"`
	ContentSourceID      *uuid.UUID           `gorm:"column:content_source_id"`
	IdempotencyKey       *string              `gorm:"column:idempotency_key"`
	OriginalURL          *string              `gorm:"column:original_url"`
	SourceEpisodeID      *string              `gorm:"column:source_episode_id"`
	Metadata             datatypes.JSON       `gorm:"column:metadata"`
	UpdatedAt            time.Time            `gorm:"column:updated_at"`
	CreatedAt            time.Time            `gorm:"column:created_at"`
	PublishedAt          *time.Time           `gorm:"column:published_at"`
	StoryID              *uuid.UUID           `gorm:"column:story_id"`
	ParentContentItemID  *uuid.UUID           `gorm:"column:parent_content_item_id"`
}

type contentResetReplaySourceSnapshot struct {
	ID            uuid.UUID         `json:"id"`
	Category      string            `json:"category"`
	Type          models.SourceType `json:"type"`
	ConfigVersion int64             `json:"config_version"`
}

type contentResetRegisteredIdentityRow struct {
	ContentItemID   uuid.UUID `gorm:"column:content_item_id"`
	ContentSourceID uuid.UUID `gorm:"column:content_source_id"`
	UpstreamItemID  string    `gorm:"column:upstream_item_id"`
}

type contentResetRegisteredIdentity struct {
	Hash    string
	Quality string
}

func hashContentResetValue(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	data, err = contentResetCanonicalJSON(data)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func contentResetCanonicalJSON(data []byte) ([]byte, error) {
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("evidence payload contains multiple JSON values")
		}
		return nil, err
	}
	return json.Marshal(value)
}

func appendContentResetEvidence(tx *gorm.DB, campaign models.ContentResetCampaign, revision *models.ContentResetRevision, key, evidenceType, owner string, payload any) error {
	if tx == nil || campaign.ID == 0 || strings.TrimSpace(key) == "" || strings.TrimSpace(evidenceType) == "" || strings.TrimSpace(owner) == "" {
		return errors.New("Content Reset evidence requires a campaign, key, type, and owner")
	}
	canonical, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode Content Reset evidence: %w", err)
	}
	hash := hashContentResetValue(json.RawMessage(canonical))
	entry := models.ContentResetEvidence{
		CampaignID: campaign.ID, TenantID: campaign.TenantID,
		EvidenceKey: key, EvidenceType: evidenceType, Owner: owner,
		Payload: datatypes.JSON(canonical), PayloadHash: hash, ObservedAt: time.Now().UTC(),
	}
	if revision != nil {
		id := revision.ID
		entry.RevisionID = &id
	}
	result := tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "campaign_id"}, {Name: "evidence_key"}},
		DoNothing: true,
	}).Create(&entry)
	if result.Error != nil {
		return fmt.Errorf("persist Content Reset evidence: %w", result.Error)
	}
	if result.RowsAffected == 1 {
		return nil
	}
	var existing models.ContentResetEvidence
	if err := tx.Where("tenant_id = ? AND campaign_id = ? AND evidence_key = ?", campaign.TenantID, campaign.ID, key).First(&existing).Error; err != nil {
		return err
	}
	previous, err := contentResetCanonicalJSON(existing.Payload)
	if err != nil || existing.PayloadHash != hash || string(previous) != string(canonical) || existing.EvidenceType != evidenceType || existing.Owner != owner {
		return errors.New("Content Reset evidence key already exists with different content")
	}
	return nil
}

func decodeContentResetJSON(c *gin.Context, destination any) error {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request must contain one JSON value")
		}
		return err
	}
	return nil
}

func normalizeContentResetIDs(ids []uuid.UUID) ([]uuid.UUID, error) {
	result := append([]uuid.UUID(nil), ids...)
	sort.Slice(result, func(i, j int) bool { return result[i].String() < result[j].String() })
	for i := 1; i < len(result); i++ {
		if result[i] == result[i-1] {
			return nil, fmt.Errorf("duplicate identifier %s", result[i])
		}
	}
	return result, nil
}

func validateContentResetRequest(req *contentResetPlanRequest) error {
	req.Operation = strings.ToLower(strings.TrimSpace(req.Operation))
	req.Lane = strings.ToLower(strings.TrimSpace(req.Lane))
	req.Scope.Kind = strings.ToLower(strings.TrimSpace(req.Scope.Kind))
	req.CoveragePolicy = strings.ToLower(strings.TrimSpace(req.CoveragePolicy))
	req.InteractionPolicy = strings.ToLower(strings.TrimSpace(req.InteractionPolicy))
	req.IntakeAfter = strings.ToLower(strings.TrimSpace(req.IntakeAfter))
	req.Replay.Mode = strings.ToLower(strings.TrimSpace(req.Replay.Mode))
	req.Replay.SourceScope = strings.ToLower(strings.TrimSpace(req.Replay.SourceScope))
	req.ProcessingStrategy = strings.ToLower(strings.TrimSpace(req.ProcessingStrategy))
	req.CapacityStrategy = strings.ToLower(strings.TrimSpace(req.CapacityStrategy))
	req.NewsAvailabilityExceptionReason = strings.TrimSpace(req.NewsAvailabilityExceptionReason)
	if req.Operation != "clear" && req.Operation != "fresh_start" && req.Operation != "empty" {
		return errors.New("operation must be clear, fresh_start, or empty")
	}
	if req.Lane != "news" && req.Lane != "pods" && req.Lane != "both" {
		return errors.New("lane must be news, pods, or both")
	}
	if req.NewsAvailabilityExceptionRequested {
		if req.Lane != "news" && req.Lane != "both" {
			return errors.New("News availability exception requires the News lane")
		}
		if len([]rune(req.NewsAvailabilityExceptionReason)) < 12 || len([]rune(req.NewsAvailabilityExceptionReason)) > 500 {
			return errors.New("News availability exception requires a reason from 12 to 500 characters")
		}
	} else if req.NewsAvailabilityExceptionReason != "" {
		return errors.New("News availability exception reason requires an explicit request")
	}
	if req.Scope.Kind != "all_lane" && req.Scope.Kind != "source_ids" && req.Scope.Kind != "explicit_ids" && req.Scope.Kind != "published_between" && req.Scope.Kind != "created_between" {
		return errors.New("scope kind must be all_lane, source_ids, explicit_ids, published_between, or created_between")
	}
	if req.CoveragePolicy == "" {
		req.CoveragePolicy = "preserve_protected"
	}
	if req.CoveragePolicy != "preserve_protected" && req.CoveragePolicy != "require_exact" {
		return errors.New("coverage_policy must be preserve_protected or require_exact")
	}
	if req.Operation == "empty" {
		if req.CoveragePolicy != "require_exact" {
			return errors.New("empty requires coverage_policy=require_exact")
		}
		if req.Scope.Kind != "all_lane" && req.Scope.Kind != "source_ids" {
			return errors.New("empty supports only an entire lane or complete source scope")
		}
		if len(req.Scope.Statuses) > 0 {
			return errors.New("empty cannot filter by status; all content in the selected lane or sources must be included")
		}
		if req.Scope.ProcessingGeneration != nil {
			return errors.New("empty cannot filter by processing generation; all content in the selected lane or sources must be included")
		}
	}
	if req.InteractionPolicy == "" {
		req.InteractionPolicy = "protect"
	}
	if req.InteractionPolicy != "protect" && req.InteractionPolicy != "preserve_history" {
		return errors.New("interaction_policy must be protect or preserve_history")
	}
	if req.IntakeAfter != "continue" && req.IntakeAfter != "paused" {
		return errors.New("intake_after must be continue or paused")
	}
	if req.Operation == "empty" && req.IntakeAfter != "paused" {
		return errors.New("empty requires intake_after=paused")
	}
	if req.Operation == "fresh_start" && req.IntakeAfter != "continue" {
		return errors.New("fresh_start requires intake_after=continue")
	}
	if req.Operation != "fresh_start" && req.Replay.Mode != "" && req.Replay.Mode != "none" {
		return errors.New("replay is only valid for fresh_start")
	}
	if req.Operation != "fresh_start" && (req.Replay.WindowDays != 0 || req.Replay.SourceScope != "" || len(req.Replay.SourceIDs) > 0) {
		return errors.New("replay options are only valid for fresh_start")
	}
	if req.Operation == "fresh_start" {
		if req.Replay.Mode != "bounded_recent" && req.Replay.Mode != "from_now" && req.Replay.Mode != "available_history" && req.Replay.Mode != "exact_rebuild" {
			return errors.New("fresh_start requires a supported replay mode")
		}
		if req.Replay.Mode == "bounded_recent" && (req.Replay.WindowDays < 1 || req.Replay.WindowDays > 365) {
			return errors.New("bounded_recent requires window_days between 1 and 365")
		}
		if req.Replay.SourceScope != "all_active_lane_sources" && req.Replay.SourceScope != "explicit_sources" {
			return errors.New("replay source_scope must be all_active_lane_sources or explicit_sources")
		}
		if req.Replay.SourceScope == "explicit_sources" && len(req.Replay.SourceIDs) == 0 {
			return errors.New("explicit replay requires source_ids")
		}
		if req.Replay.SourceScope == "all_active_lane_sources" && len(req.Replay.SourceIDs) != 0 {
			return errors.New("source_ids are only valid with explicit_sources")
		}
		if req.Replay.Mode != "bounded_recent" && req.Replay.WindowDays != 0 {
			return errors.New("window_days is only valid with bounded_recent")
		}
		if req.Replay.Mode == "exact_rebuild" && req.Scope.Kind != "explicit_ids" {
			return errors.New("exact_rebuild requires an explicit_ids scope")
		}
	} else {
		req.Replay = contentResetReplay{Mode: "none"}
	}
	if req.Operation == "fresh_start" {
		if req.ProcessingStrategy == "" {
			req.ProcessingStrategy = "copy_verified"
		}
		if req.ProcessingStrategy != "copy_verified" && req.ProcessingStrategy != "recompute" {
			return errors.New("processing_strategy must be copy_verified or recompute")
		}
		if req.CapacityStrategy == "" {
			req.CapacityStrategy = "build_first"
		}
		if req.CapacityStrategy != "build_first" && req.CapacityStrategy != "clear_first" {
			return errors.New("capacity_strategy must be build_first or clear_first")
		}
	} else {
		if req.ProcessingStrategy != "" && req.ProcessingStrategy != "none" {
			return errors.New("processing_strategy is only valid for fresh_start")
		}
		if req.CapacityStrategy != "" && req.CapacityStrategy != "none" {
			return errors.New("capacity_strategy is only valid for fresh_start")
		}
		req.ProcessingStrategy = "none"
		req.CapacityStrategy = "none"
	}

	var err error
	req.Scope.SourceIDs, err = normalizeContentResetIDs(req.Scope.SourceIDs)
	if err != nil || len(req.Scope.SourceIDs) > 500 {
		return errors.New("scope source_ids must be unique and contain at most 500 entries")
	}
	req.Scope.ContentItemIDs, err = normalizeContentResetIDs(req.Scope.ContentItemIDs)
	if err != nil || len(req.Scope.ContentItemIDs) > 5000 {
		return errors.New("scope content_item_ids must be unique and contain at most 5000 entries")
	}
	req.Replay.SourceIDs, err = normalizeContentResetIDs(req.Replay.SourceIDs)
	if err != nil || len(req.Replay.SourceIDs) > 500 {
		return errors.New("replay source_ids must be unique and contain at most 500 entries")
	}
	if req.Scope.Kind == "source_ids" && len(req.Scope.SourceIDs) == 0 {
		return errors.New("source_ids scope requires source_ids")
	}
	if req.Scope.Kind != "source_ids" && len(req.Scope.SourceIDs) != 0 {
		return errors.New("scope source_ids are only valid for source_ids scope")
	}
	if req.Scope.Kind == "explicit_ids" && len(req.Scope.ContentItemIDs) == 0 {
		return errors.New("explicit_ids scope requires content_item_ids")
	}
	if req.Scope.Kind != "explicit_ids" && len(req.Scope.ContentItemIDs) != 0 {
		return errors.New("content_item_ids are only valid for explicit_ids scope")
	}
	if req.Scope.Kind == "published_between" || req.Scope.Kind == "created_between" {
		from, fromErr := time.Parse(time.RFC3339, req.Scope.From)
		to, toErr := time.Parse(time.RFC3339, req.Scope.To)
		if fromErr != nil || toErr != nil || !from.Before(to) {
			return errors.New("date interval requires an RFC3339 from before to")
		}
		req.Scope.From = from.UTC().Format(time.RFC3339Nano)
		req.Scope.To = to.UTC().Format(time.RFC3339Nano)
	} else if req.Scope.From != "" || req.Scope.To != "" {
		return errors.New("from and to are only valid for a date interval scope")
	}
	if req.Scope.Statuses != nil {
		allowed := map[models.ContentStatus]bool{
			models.ContentStatusPending: true, models.ContentStatusProcessing: true,
			models.ContentStatusReady: true, models.ContentStatusFailed: true,
			models.ContentStatusArchived: true,
		}
		seen := map[models.ContentStatus]bool{}
		for i, status := range req.Scope.Statuses {
			normalized := models.ContentStatus(strings.ToUpper(strings.TrimSpace(string(status))))
			if !allowed[normalized] || seen[normalized] {
				return errors.New("statuses must be unique known content statuses")
			}
			seen[normalized] = true
			req.Scope.Statuses[i] = normalized
		}
		sort.Slice(req.Scope.Statuses, func(i, j int) bool { return req.Scope.Statuses[i] < req.Scope.Statuses[j] })
	}
	if req.Scope.ProcessingGeneration != nil && *req.Scope.ProcessingGeneration < 1 {
		return errors.New("processing_generation must be a positive generation number")
	}
	return nil
}

func contentResetLaneTypes(lane string) []models.ContentType {
	switch lane {
	case "news":
		return []models.ContentType{models.ContentTypeNews, models.ContentTypeArticle, models.ContentTypeTweet, models.ContentTypeComment}
	case "pods":
		return []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}
	default:
		return []models.ContentType{models.ContentTypeNews, models.ContentTypeArticle, models.ContentTypeTweet, models.ContentTypeComment, models.ContentTypeVideo, models.ContentTypePodcast}
	}
}

func applyContentResetSelection(q *gorm.DB, req contentResetPlanRequest, includeDate bool) *gorm.DB {
	q = q.Where("type IN ?", contentResetLaneTypes(req.Lane))
	if len(req.Scope.Statuses) > 0 {
		q = q.Where("status IN ?", req.Scope.Statuses)
	}
	if req.Scope.ProcessingGeneration != nil {
		q = q.Where("processing_generation = ?", *req.Scope.ProcessingGeneration)
	}
	switch req.Scope.Kind {
	case "source_ids":
		q = q.Where("content_source_id IN ?", req.Scope.SourceIDs)
	case "explicit_ids":
		q = q.Where("public_id IN ?", req.Scope.ContentItemIDs)
	case "published_between":
		if includeDate {
			q = q.Where("published_at >= ? AND published_at < ?", req.Scope.From, req.Scope.To)
		}
	case "created_between":
		if includeDate {
			q = q.Where("created_at >= ? AND created_at < ?", req.Scope.From, req.Scope.To)
		}
	}
	return q
}

// captureContentResetSelectionBoundary briefly serializes against content
// inserts for this tenant. The per-tenant counter lock ensures that an insert
// waiting on the lock receives a sequence above the captured boundary, even if
// its database ID or created_at value was assigned before the preview began.
func captureContentResetSelectionBoundary(db *gorm.DB, tenant string) (int64, int64, time.Time, error) {
	var inventoryHighwater int64
	var idHighwater int64
	var boundaryAt time.Time
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(`
			INSERT INTO tenant_content_inventory_counters (tenant_id, current_sequence)
			VALUES (?, 0)
			ON CONFLICT (tenant_id) DO NOTHING`, tenant).Error; err != nil {
			return err
		}
		if err := tx.Raw(`SELECT current_sequence FROM tenant_content_inventory_counters WHERE tenant_id = ? FOR UPDATE`, tenant).Scan(&inventoryHighwater).Error; err != nil {
			return err
		}
		if err := tx.Exec(`DELETE FROM content_reset_inventory_deletions
			WHERE tenant_id = ? AND deleted_at < clock_timestamp() - INTERVAL '25 hours'`, tenant).Error; err != nil {
			return err
		}
		boundaryAt = time.Now().UTC()
		return tx.Model(&models.ContentItem{}).
			Where("tenant_id = ? AND inventory_sequence <= ?", tenant, inventoryHighwater).
			Select("COALESCE(MAX(id), 0)").Scan(&idHighwater).Error
	})
	return idHighwater, inventoryHighwater, boundaryAt, err
}

func validateContentResetSourceIDs(db *gorm.DB, tenant, lane string, ids []uuid.UUID, requireActive bool) error {
	if len(ids) == 0 {
		return nil
	}
	query := db.Model(&models.ContentSource{}).Where("tenant_id = ? AND public_id IN ?", tenant, ids)
	if lane == "news" {
		query = query.Where("category = ?", models.SourceCategoryNews)
	} else if lane == "pods" {
		query = query.Where("category = ?", models.SourceCategoryMedia)
	} else {
		query = query.Where("category IN ?", []string{models.SourceCategoryNews, models.SourceCategoryMedia})
	}
	if requireActive {
		query = query.Where("is_active = TRUE")
	}
	var count int64
	if err := query.Count(&count).Error; err != nil {
		return err
	}
	if count != int64(len(ids)) {
		return errors.New("one or more source identifiers are missing, inactive, or outside the selected lane")
	}
	return nil
}

func resolveContentResetReplaySources(db *gorm.DB, tenant, lane string, replay contentResetReplay) ([]contentResetReplaySourceSnapshot, error) {
	var query = db.Model(&models.ContentSource{}).
		Select("public_id, category, type, source_config_version").
		Where("tenant_id = ? AND is_active = TRUE", tenant)
	if lane == "news" {
		query = query.Where("category = ?", models.SourceCategoryNews)
	} else if lane == "pods" {
		query = query.Where("category = ?", models.SourceCategoryMedia)
	} else {
		query = query.Where("category IN ?", []string{models.SourceCategoryNews, models.SourceCategoryMedia})
	}
	if replay.SourceScope == "explicit_sources" {
		query = query.Where("public_id IN ?", replay.SourceIDs)
	}
	var sources []models.ContentSource
	if err := query.Order("public_id ASC").Find(&sources).Error; err != nil {
		return nil, err
	}
	if replay.SourceScope == "explicit_sources" && len(sources) != len(replay.SourceIDs) {
		return nil, errors.New("a replay source changed or left the selected lane at the planning boundary")
	}
	if len(sources) == 0 {
		return nil, errors.New("fresh_start requires at least one active replay source at the planning boundary")
	}
	snapshot := make([]contentResetReplaySourceSnapshot, 0, len(sources))
	for _, source := range sources {
		snapshot = append(snapshot, contentResetReplaySourceSnapshot{
			ID: source.PublicID, Category: source.Category, Type: source.Type,
			ConfigVersion: source.SourceConfigVersion,
		})
	}
	return snapshot, nil
}

func contentResetSourceSupportsReplay(sourceType models.SourceType, mode string) bool {
	if mode != "bounded_recent" && mode != "available_history" {
		return false
	}
	switch strings.ToUpper(strings.TrimSpace(string(sourceType))) {
	case string(models.SourceTypePodcast), "REDDIT":
		return true
	default:
		return false
	}
}

func GetContentResetCapabilities(c *gin.Context) {
	if _, ok := requireAdminPrincipal(c); !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	refreshContentResetQualifications(db)
	installed, qualified := 0, 0
	for contract := range contentResetOwners {
		installed++
		if contentResetQualificationValue(contract) != "" {
			qualified++
		}
	}
	executionEnabled := installed > 0 && qualified == installed
	operation := func(op, lane string) gin.H {
		_, blockers := contentResetContractFor(contentResetPlanRequest{
			Operation: op, Lane: lane, CapacityStrategy: "build_first", ProcessingStrategy: "copy_verified",
			InteractionPolicy: "protect", CoveragePolicy: "preserve_protected", IntakeAfter: "continue",
			Replay: contentResetReplay{Mode: "bounded_recent", SourceScope: "all_active_lane_sources"},
		})
		body := gin.H{"preview": true, "execute": executionEnabled, "installed_owners": installed, "qualified_owners": qualified}
		if len(blockers) > 0 {
			body["execute"] = false
			body["blockers"] = blockers
		}
		return body
	}
	c.JSON(http.StatusOK, gin.H{
		"execution_enabled":    executionEnabled,
		"execution_permission": "content_reset:execute",
		"release_qualification": gin.H{
			"registry": "content_reset_owner_qualifications", "rubric": contentreset.OwnerQualificationRubricVersion,
			"installed_owners": installed, "qualified_owners": qualified,
			"reason": "Execution is enabled only when every installed owner contract carries a durable qualification row matching this build",
		},
		"preview_validation": gin.H{
			"available": true, "read_only": true,
			"endpoint":         "/admin/content-reset/campaigns/:id/validation",
			"approval_enabled": executionEnabled,
		},
		"operations": gin.H{
			"clear":       operation("clear", "both"),
			"fresh_start": operation("fresh_start", "both"),
			"empty":       operation("empty", "both"),
		},
		"lanes": gin.H{
			"news": operation("clear", "news"),
			"pods": operation("clear", "pods"),
			"both": operation("clear", "both"),
		},
		"permissions": gin.H{
			"read": "content_reset:read", "plan": "content_reset:plan", "execute": "content_reset:execute",
			"execution_route_available": true,
		},
		"publication_controls": gin.H{
			"publish": true, "rollback": true, "authorize_cleanup": true,
			"reason": "Publication, rollback and delayed cleanup require their own reauthenticated milestone approval",
		},
		"staged_media_delivery": gin.H{
			"private": contentResetStagedDeliveryPrivate(), "urls_withheld": !contentResetStagedDeliveryPrivate(),
			"reason": "Candidate media currently follows the ordinary public delivery path; Pods Fresh Start stays blocked and staged playback URLs are withheld until a private or signed delivery boundary is qualified",
		},
		"news_availability_exception": gin.H{
			"request_in_preview": true, "approve": false, "execute": false,
			"reason": "a frozen preview request does not override Retention or archive protections",
		},
		"interaction_history_policy": gin.H{
			"available": false, "default": "protect",
			"reason": "interaction-preserving retirement has no qualified consumer history contract",
		},
		"protected_interaction_policy": "protect",
		"supported_scope_kinds":        []string{"all_lane", "source_ids", "explicit_ids", "published_between", "created_between"},
	})
}

func CreateContentResetCampaign(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	key := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if len(key) < 8 || len(key) > 128 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Idempotency-Key must contain 8 to 128 characters"})
		return
	}
	var req contentResetPlanRequest
	if err := decodeContentResetJSON(c, &req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid Content Reset plan request"})
		return
	}
	if err := validateContentResetRequest(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	requestBytes, err := json.Marshal(req)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid Content Reset request"})
		return
	}
	requestHash := hashContentResetValue(req)
	var existing models.ContentResetCampaign
	if err := db.Where("tenant_id = ? AND idempotency_key = ?", principal.TenantID, key).First(&existing).Error; err == nil {
		if existing.RequestHash != requestHash {
			c.JSON(http.StatusConflict, gin.H{"error": "idempotency key was already used with different intent"})
			return
		}
		c.JSON(http.StatusOK, loadContentResetDetail(db, principal.TenantID, existing.PublicID))
		return
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to check Content Reset idempotency"})
		return
	}
	if err := validateContentResetSourceIDs(db, principal.TenantID, req.Lane, req.Scope.SourceIDs, false); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid selection sources"})
		return
	}
	if req.Replay.SourceScope == "explicit_sources" {
		if err := validateContentResetSourceIDs(db, principal.TenantID, req.Lane, req.Replay.SourceIDs, true); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid replay sources"})
			return
		}
	}
	highwater, inventoryHighwater, planningBoundaryAt, err := captureContentResetSelectionBoundary(db, principal.TenantID)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "failed to establish a stable content inventory boundary"})
		return
	}
	replaySourceSnapshot := []contentResetReplaySourceSnapshot{}
	if req.Operation == "fresh_start" {
		var resolveErr error
		replaySourceSnapshot, resolveErr = resolveContentResetReplaySources(db, principal.TenantID, req.Lane, req.Replay)
		if resolveErr != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "unable to freeze replay sources at the planning boundary"})
			return
		}
	}
	replaySourceBytes, _ := json.Marshal(replaySourceSnapshot)
	replaySourceHash := hashContentResetValue(replaySourceSnapshot)
	var unknownDateCount int64
	if req.Scope.Kind == "published_between" {
		unknownQuery := applyContentResetSelection(db.Model(&models.ContentItem{}).Where("tenant_id = ? AND id <= ? AND inventory_sequence <= ? AND published_at IS NULL", principal.TenantID, highwater, inventoryHighwater), req, false)
		if err := unknownQuery.Count(&unknownDateCount).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to count content with unknown publication dates"})
			return
		}
	}
	var unattributedCount int64
	if req.Scope.Kind == "source_ids" {
		unattributedRequest := req
		unattributedRequest.Scope = contentResetScope{Kind: "all_lane", Statuses: req.Scope.Statuses}
		unattributedQuery := applyContentResetSelection(db.Model(&models.ContentItem{}).Where("tenant_id = ? AND id <= ? AND inventory_sequence <= ? AND content_source_id IS NULL", principal.TenantID, highwater, inventoryHighwater), unattributedRequest, false)
		if err := unattributedQuery.Count(&unattributedCount).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to count items with ambiguous source attribution"})
			return
		}
	}
	campaign := models.ContentResetCampaign{
		TenantID: principal.TenantID, Operation: req.Operation, Lane: req.Lane,
		State: "planning", CurrentRevision: 1, IdempotencyKey: key,
		RequestHash: requestHash, CreatedBy: principal.Email,
	}
	revision := models.ContentResetRevision{
		TenantID: principal.TenantID, Revision: 1, Request: datatypes.JSON(requestBytes),
		State: "planning", SelectionHighwater: highwater, ManifestChainHash: strings.Repeat("0", 64),
		InventoryHighwater: inventoryHighwater,
		UnknownDateCount:   unknownDateCount, UnattributedCount: unattributedCount,
		ReplaySourceSnapshot: datatypes.JSON(replaySourceBytes), ReplaySourceHash: replaySourceHash,
		Blockers:  datatypes.JSON([]byte("[]")),
		CreatedBy: principal.Email, PlanningStartedAt: planningBoundaryAt,
	}
	planningExpiresAt := planningBoundaryAt.Add(contentResetPlanningTTL)
	revision.ExpiresAt = &planningExpiresAt
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&campaign).Error; err != nil {
			return err
		}
		revision.CampaignID = campaign.ID
		if err := tx.Create(&revision).Error; err != nil {
			return err
		}
		return appendContentResetEvidence(tx, campaign, &revision, "planning-started", "planning_started", "cms/content-reset", map[string]any{
			"operation": campaign.Operation, "lane": campaign.Lane,
			"request_hash": campaign.RequestHash, "selection_highwater": revision.SelectionHighwater,
			"inventory_highwater": revision.InventoryHighwater,
			"planning_started_at": revision.PlanningStartedAt.UTC(),
			"replay_source_hash":  revision.ReplaySourceHash,
		})
	}); err != nil {
		// A concurrent retry may have won the unique idempotency key race.
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			var raced models.ContentResetCampaign
			if lookupErr := db.Where("tenant_id = ? AND idempotency_key = ?", principal.TenantID, key).First(&raced).Error; lookupErr == nil && raced.RequestHash == requestHash {
				c.JSON(http.StatusOK, loadContentResetDetail(db, principal.TenantID, raced.PublicID))
				return
			}
			c.JSON(http.StatusConflict, gin.H{"error": "Content Reset campaign conflicts with an existing operation"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create Content Reset campaign"})
		return
	}
	if err := advanceContentResetPlanning(db, principal.TenantID, campaign.PublicID, 0); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "campaign created but planning did not advance; retry with the current scan cursor"})
		return
	}
	c.JSON(http.StatusCreated, loadContentResetDetail(db, principal.TenantID, campaign.PublicID))
}

func contentResetProviderIdentity(item contentResetCandidate) (string, string) {
	if contentResetLaneForType(item.Type) == "news" {
		identity, err := retentionTombstoneIdentityForIngest(item.TenantID, derefStr(item.IdempotencyKey), derefStr(item.OriginalURL))
		if err == nil {
			return identity, "legacy_ingest_candidate"
		}
		return "", "legacy_unresolved"
	}
	if item.ContentSourceID == nil {
		return "", "legacy_unresolved"
	}
	key, kind := "", ""
	if item.SourceEpisodeID != nil {
		key, kind = strings.TrimSpace(*item.SourceEpisodeID), "source_episode_id"
	}
	if key == "" {
		var metadata map[string]any
		if json.Unmarshal(item.Metadata, &metadata) == nil {
			candidates := []struct{ name, kind string }{
				{"videoId", "youtube_video_id"},
				{"guid", "podcast_guid"},
				{"messageId", "telegram_message_id"},
				{"source_item_key", "source_item_key"},
			}
			for _, candidate := range candidates {
				if value, ok := metadata[candidate.name].(string); ok && strings.TrimSpace(value) != "" {
					key, kind = strings.TrimSpace(value), candidate.kind
					break
				}
			}
		}
	}
	if key == "" {
		return "", "legacy_unresolved"
	}
	return hashContentResetValue([]string{
		"source-item-identity-candidate/v1", item.TenantID, item.ContentSourceID.String(),
		string(item.Source), kind, key,
	}), "provider_key_candidate"
}

func contentResetTargetSnapshot(item contentResetCandidate) map[string]any {
	identityHash, identityQuality := contentResetProviderIdentity(item)
	return map[string]any{
		"content_item_id": item.PublicID, "lane": contentResetLaneForType(item.Type),
		"type": item.Type, "source_type": item.Source, "status": item.Status,
		"processing_generation":   item.ProcessingGeneration,
		"content_source_id":       item.ContentSourceID,
		"source_identity_hash":    identityHash,
		"source_identity_quality": identityQuality,
		"created_at":              item.CreatedAt.UTC(),
		"updated_at":              item.UpdatedAt.UTC(), "published_at": item.PublishedAt,
		"story_id": item.StoryID, "parent_content_item_id": item.ParentContentItemID,
	}
}

func contentResetRegisteredIdentities(tx *gorm.DB, tenant string, items []contentResetCandidate) (map[uuid.UUID]contentResetRegisteredIdentity, error) {
	result := make(map[uuid.UUID]contentResetRegisteredIdentity)
	if len(items) == 0 || !tx.Migrator().HasTable(&models.SourceItemIdentity{}) {
		return result, nil
	}
	ids := make([]uuid.UUID, 0, len(items))
	itemByID := make(map[uuid.UUID]contentResetCandidate, len(items))
	for _, item := range items {
		ids = append(ids, item.PublicID)
		itemByID[item.PublicID] = item
	}
	var rows []contentResetRegisteredIdentityRow
	if err := tx.Table("source_item_identities AS registry").
		Select("registry.current_content_item_id AS content_item_id, registry.content_source_id, registry.upstream_item_id").
		Joins("JOIN source_item_instances AS instance ON instance.tenant_id = registry.tenant_id AND instance.identity_id = registry.id AND instance.instance_generation = registry.current_instance_generation AND instance.content_item_id = registry.current_content_item_id AND instance.state = ?", "active").
		Joins("JOIN content_items AS content ON content.tenant_id = registry.tenant_id AND content.public_id = registry.current_content_item_id").
		Where("registry.tenant_id = ? AND registry.current_instance_generation > 0 AND registry.current_content_item_id IN ?", tenant, ids).
		Find(&rows).Error; err != nil {
		return nil, err
	}
	counts := make(map[uuid.UUID]int, len(rows))
	hashes := make(map[uuid.UUID]string, len(rows))
	for _, row := range rows {
		item, exists := itemByID[row.ContentItemID]
		if !exists || item.ContentSourceID == nil || *item.ContentSourceID != row.ContentSourceID || strings.TrimSpace(row.UpstreamItemID) == "" {
			continue
		}
		counts[row.ContentItemID]++
		hashes[row.ContentItemID] = hashContentResetValue([]string{
			"source-item-identity-candidate/v1", tenant, row.ContentSourceID.String(),
			string(item.Source), "source_upstream_observation", row.UpstreamItemID,
		})
	}
	for itemID, count := range counts {
		if count == 1 {
			result[itemID] = contentResetRegisteredIdentity{Hash: hashes[itemID], Quality: "registered_provider_identity"}
		} else {
			result[itemID] = contentResetRegisteredIdentity{Quality: "registered_identity_ambiguous"}
		}
	}
	return result, nil
}

func contentResetLaneForType(contentType models.ContentType) string {
	if contentType == models.ContentTypeVideo || contentType == models.ContentTypePodcast {
		return "pods"
	}
	return "news"
}

func contentResetProtectionReasons(db *gorm.DB, tenant string, candidates []uuid.UUID) (map[uuid.UUID][]string, error) {
	reasons := make(map[uuid.UUID][]string, len(candidates))
	add := func(ids []uuid.UUID, reason string) {
		for _, id := range ids {
			reasons[id] = append(reasons[id], reason)
		}
	}
	var ids []uuid.UUID
	if err := db.Model(&models.UserInteraction{}).Where("content_item_id IN ?", candidates).Distinct().Pluck("content_item_id", &ids).Error; err != nil {
		return nil, fmt.Errorf("interaction protection evidence failed: %w", err)
	}
	add(ids, "consumer_interaction")
	ids = nil
	if err := db.Model(&models.RetentionHold{}).Where("tenant_id = ? AND target_type = 'content' AND target_id IN ? AND released_at IS NULL AND (expires_at IS NULL OR expires_at > ?)", tenant, candidates, time.Now().UTC()).Distinct().Pluck("target_id", &ids).Error; err != nil {
		return nil, fmt.Errorf("content hold evidence failed: %w", err)
	}
	add(ids, "content_hold")
	ids = nil
	if err := db.Raw(`
		SELECT DISTINCT c.public_id
		FROM content_items c
		JOIN retention_holds h ON h.target_id = c.story_id
		WHERE c.tenant_id = ? AND c.public_id IN ?
		  AND h.tenant_id = ? AND h.target_type = 'story' AND h.released_at IS NULL
		  AND (h.expires_at IS NULL OR h.expires_at > ?)`, tenant, candidates, tenant, time.Now().UTC()).Scan(&ids).Error; err != nil {
		return nil, fmt.Errorf("story hold evidence failed: %w", err)
	}
	add(ids, "story_hold")
	ids = nil
	if err := db.Model(&models.ModerationReport{}).Where("tenant_id = ? AND target_type = ? AND target_id IN ? AND status = ?", tenant, models.ModerationTargetContent, candidates, "open").Distinct().Pluck("target_id", &ids).Error; err != nil {
		return nil, fmt.Errorf("moderation evidence failed: %w", err)
	}
	add(ids, "open_moderation_report")
	ids = nil
	if err := db.Model(&models.ContentFlag{}).Where("tenant_id = ? AND content_item_id IN ?", tenant, candidates).Distinct().Pluck("content_item_id", &ids).Error; err != nil {
		return nil, fmt.Errorf("editorial flag evidence failed: %w", err)
	}
	add(ids, "editorial_flag")
	return reasons, nil
}

func advanceContentResetPlanning(db *gorm.DB, tenant string, campaignID uuid.UUID, expectedCursor int64) error {
	return db.Transaction(func(tx *gorm.DB) error {
		var campaign models.ContentResetCampaign
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id = ? AND public_id = ?", tenant, campaignID).First(&campaign).Error; err != nil {
			return err
		}
		var revision models.ContentResetRevision
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id = ? AND campaign_id = ? AND revision = ?", tenant, campaign.ID, campaign.CurrentRevision).First(&revision).Error; err != nil {
			return err
		}
		if campaign.State != "planning" || revision.State != "planning" {
			return errors.New("Content Reset planning is not active")
		}
		if revision.ScanCursor != expectedCursor {
			return fmt.Errorf("stale planning cursor: expected %d, current %d", expectedCursor, revision.ScanCursor)
		}
		if revision.ExpiresAt != nil && time.Now().UTC().After(*revision.ExpiresAt) {
			return errors.New("Content Reset planning window expired")
		}
		var req contentResetPlanRequest
		if err := json.Unmarshal(revision.Request, &req); err != nil {
			return err
		}
		query := applyContentResetSelection(tx.Model(&models.ContentItem{}).Where("tenant_id = ? AND id > ? AND id <= ? AND inventory_sequence <= ?", tenant, revision.ScanCursor, revision.SelectionHighwater, revision.InventoryHighwater), req, true)
		var candidates []contentResetCandidate
		if err := query.Select("id, public_id, tenant_id, type, source, status, processing_generation, content_source_id, idempotency_key, original_url, source_episode_id, metadata, updated_at, created_at, published_at, story_id, parent_content_item_id").Order("id ASC").Limit(contentResetPlanPageSize).Find(&candidates).Error; err != nil {
			return err
		}
		if len(candidates) == 0 {
			return finalizeContentResetPlanning(tx, &campaign, &revision, req)
		}
		registeredIdentities, err := contentResetRegisteredIdentities(tx, tenant, candidates)
		if err != nil {
			return fmt.Errorf("read registered source-item identities: %w", err)
		}
		ids := make([]uuid.UUID, 0, len(candidates))
		for _, item := range candidates {
			ids = append(ids, item.PublicID)
		}
		protected, err := retentionProtectedContentIDs(tx, tenant, ids)
		if err != nil {
			return err
		}
		protectionReasons, err := contentResetProtectionReasons(tx, tenant, ids)
		if err != nil {
			return err
		}
		targets := make([]models.ContentResetTarget, 0, len(candidates))
		chain := revision.ManifestChainHash
		protectedCount := revision.ProtectedCount
		for _, item := range candidates {
			isProtected := protected[item.PublicID]
			disposition := "selected"
			reason := ""
			reasonCodes := protectionReasons[item.PublicID]
			if isProtected && len(reasonCodes) == 0 {
				reasonCodes = []string{"owner_protection_unknown"}
			}
			if isProtected {
				protectedCount++
				reason = strings.Join(reasonCodes, ",")
				if len(reason) > 64 {
					reason = reason[:64]
				}
				if req.InteractionPolicy == "preserve_history" && containsContentResetReason(reasonCodes, "consumer_interaction") {
					disposition = "blocked"
				} else if req.CoveragePolicy == "preserve_protected" {
					disposition = "preserve"
				} else {
					disposition = "blocked"
				}
			}
			if reasonCodes == nil {
				reasonCodes = []string{}
			}
			snapshot := contentResetTargetSnapshot(item)
			if registered, exists := registeredIdentities[item.PublicID]; exists {
				snapshot["source_identity_hash"] = registered.Hash
				snapshot["source_identity_quality"] = registered.Quality
			}
			snapshotHash := hashContentResetValue(snapshot)
			reasonJSON, _ := json.Marshal(reasonCodes)
			targets = append(targets, models.ContentResetTarget{
				RevisionID: revision.ID, TenantID: tenant, ContentItemID: item.PublicID,
				ItemOrdinal: item.ID, Lane: contentResetLaneForType(item.Type),
				Disposition: disposition, Protected: isProtected, ProtectionReason: reason,
				ProtectionEvidence: datatypes.JSON(reasonJSON),
				SnapshotHash:       snapshotHash, Snapshot: mustMarshalContentResetJSON(snapshot),
			})
			chain = hashContentResetValue([]any{
				chain, item.ID, snapshotHash, disposition, isProtected, reasonCodes,
			})
		}
		if err := tx.CreateInBatches(&targets, contentResetPlanPageSize).Error; err != nil {
			return err
		}
		lastID := candidates[len(candidates)-1].ID
		if err := tx.Model(&revision).Updates(map[string]any{
			"scan_cursor":         lastID,
			"target_count":        revision.TargetCount + int64(len(candidates)),
			"protected_count":     protectedCount,
			"manifest_chain_hash": chain,
		}).Error; err != nil {
			return err
		}
		revision.ScanCursor = lastID
		revision.TargetCount += int64(len(candidates))
		revision.ProtectedCount = protectedCount
		revision.ManifestChainHash = chain
		if err := appendContentResetEvidence(tx, campaign, &revision, fmt.Sprintf("planning-batch:%d", lastID), "planning_batch", "cms/content-reset", map[string]any{
			"cursor_from": expectedCursor, "cursor_to": lastID,
			"scanned_count": len(candidates), "target_count": revision.TargetCount,
			"protected_count":     revision.ProtectedCount,
			"inventory_highwater": revision.InventoryHighwater,
			"manifest_chain_hash": revision.ManifestChainHash,
		}); err != nil {
			return err
		}
		if len(candidates) < contentResetPlanPageSize {
			return finalizeContentResetPlanning(tx, &campaign, &revision, req)
		}
		return nil
	})
}

func containsContentResetReason(reasons []string, expected string) bool {
	for _, reason := range reasons {
		if reason == expected {
			return true
		}
	}
	return false
}

func mustMarshalContentResetJSON(value any) datatypes.JSON {
	data, _ := json.Marshal(value)
	return datatypes.JSON(data)
}

func contentResetReplaySourcesChanged(tx *gorm.DB, tenant string, revision models.ContentResetRevision) (bool, error) {
	var snapshot []contentResetReplaySourceSnapshot
	if err := json.Unmarshal(revision.ReplaySourceSnapshot, &snapshot); err != nil {
		return false, fmt.Errorf("decode frozen replay source snapshot: %w", err)
	}
	if hashContentResetValue(snapshot) != revision.ReplaySourceHash {
		return false, errors.New("frozen replay source snapshot hash does not match its content")
	}
	if len(snapshot) == 0 {
		return false, nil
	}
	ids := make([]uuid.UUID, 0, len(snapshot))
	for _, source := range snapshot {
		ids = append(ids, source.ID)
	}
	var current []models.ContentSource
	if err := tx.Select("public_id, category, type, source_config_version, is_active").
		Where("tenant_id = ? AND public_id IN ?", tenant, ids).
		Find(&current).Error; err != nil {
		return false, fmt.Errorf("read replay sources at planning completion: %w", err)
	}
	if len(current) != len(snapshot) {
		return true, nil
	}
	expectedByID := make(map[uuid.UUID]contentResetReplaySourceSnapshot, len(snapshot))
	for _, source := range snapshot {
		expectedByID[source.ID] = source
	}
	for _, source := range current {
		expected, found := expectedByID[source.PublicID]
		if !found || !source.IsActive || source.Category != expected.Category ||
			source.Type != expected.Type || source.SourceConfigVersion != expected.ConfigVersion {
			return true, nil
		}
	}
	return false, nil
}

func countContentResetDeletionsSinceBoundary(tx *gorm.DB, tenant string, revision models.ContentResetRevision, req contentResetPlanRequest) (int64, error) {
	query := tx.Model(&models.ContentResetInventoryDeletion{}).
		Where("tenant_id = ? AND inventory_sequence > ?", tenant, revision.InventoryHighwater)
	if req.Scope.Kind == "explicit_ids" {
		query = query.Where("content_item_id IN ?", req.Scope.ContentItemIDs)
	} else {
		// For source and date scopes, attribution or dates could change before a
		// deletion. Block on any deletion in the selected lane instead of risking
		// a missing boundary member.
		query = query.Where("type IN ?", contentResetLaneTypes(req.Lane))
	}
	var count int64
	if err := query.Count(&count).Error; err != nil {
		return 0, fmt.Errorf("read content deletions since preview boundary: %w", err)
	}
	return count, nil
}

func finalizeContentResetPlanning(tx *gorm.DB, campaign *models.ContentResetCampaign, revision *models.ContentResetRevision, req contentResetPlanRequest) error {
	// Owner installation and release qualification are evaluated from the
	// durable registry. Policy blockers below are added regardless of owner
	// availability, because they describe requested scope that cannot be
	// approved even by a fully qualified executor.
	blockers := []contentResetBlocker{}
	_, contractBlockers := contentResetContractFor(req)
	blockers = append(blockers, contractBlockers...)
	deletionsSinceBoundary, err := countContentResetDeletionsSinceBoundary(tx, campaign.TenantID, *revision, req)
	if err != nil {
		return err
	}
	if deletionsSinceBoundary > 0 {
		blockers = append(blockers, contentResetBlocker{
			Code: "content_deleted_during_planning", Owner: "cms/content",
			Reason:     fmt.Sprintf("%d content rows in the selected lane or exact-ID set were deleted after the inventory boundary; this preview no longer represents that boundary.", deletionsSinceBoundary),
			NextAction: "Cancel this preview and create a new one against the current content inventory.",
		})
	}
	if campaign.Operation == "fresh_start" {
		var replaySources []contentResetReplaySourceSnapshot
		if err := json.Unmarshal(revision.ReplaySourceSnapshot, &replaySources); err != nil {
			return fmt.Errorf("decode frozen Content Reset replay sources: %w", err)
		}
		var unsupportedSources []uuid.UUID
		for _, source := range replaySources {
			if !contentResetSourceSupportsReplay(source.Type, req.Replay.Mode) {
				unsupportedSources = append(unsupportedSources, source.ID)
			}
		}
		if len(unsupportedSources) > 0 {
			blockers = append(blockers, contentResetBlocker{
				Code: "source_replay_unsupported", Owner: "aggregation/source-adapters",
				Reason:     fmt.Sprintf("%d frozen replay sources have no installed bounded-page contract for the selected replay mode; ordinary replayable-listing observation is insufficient.", len(unsupportedSources)),
				NextAction: "Install and qualify the selected provider/mode contract before approving replay.",
			})
		}
	}
	if campaign.Operation == "fresh_start" {
		changed, err := contentResetReplaySourcesChanged(tx, campaign.TenantID, *revision)
		if err != nil {
			return err
		}
		if changed {
			blockers = append(blockers, contentResetBlocker{
				Code: "replay_source_changed_during_planning", Owner: "cms/source-run",
				Reason:     "A frozen replay source was removed, deactivated, moved to another lane, or had its configuration version changed while inventory was being planned.",
				NextAction: "Cancel this preview and create a new one from the current source configuration.",
			})
		}
		var unresolvedIdentityCount int64
		if err := tx.Model(&models.ContentResetTarget{}).
			Where("tenant_id = ? AND revision_id = ? AND snapshot->>'source_identity_quality' IN ?", campaign.TenantID, revision.ID, []string{"legacy_unresolved", "legacy_ingest_candidate", "provider_key_candidate", "registered_identity_ambiguous"}).
			Count(&unresolvedIdentityCount).Error; err != nil {
			return err
		}
		if unresolvedIdentityCount > 0 {
			blockers = append(blockers, contentResetBlocker{
				Code: "selected_identity_unresolved", Owner: "cms/source-identity",
				Reason:     fmt.Sprintf("%d selected items have no unambiguous stable source-specific reconstruction key in the frozen snapshot.", unresolvedIdentityCount),
				NextAction: "Reconcile the provider identity or resolve provider aliases for each item before approving a reconstruction campaign.",
			})
		}
	}
	if req.NewsAvailabilityExceptionRequested {
		blockers = append(blockers, contentResetBlocker{
			Code: "news_availability_exception_not_qualified", Owner: "cms/retention",
			Reason:     "The preview records the requested exception and reason in its immutable manifest, but no administrator approval or News retirement adapter can authorize it. Existing recent-News and archive protections remain in force.",
			NextAction: "Keep the protected News items or wait until the exact manifest-bound exception and owner cleanup path are qualified.",
		})
	}
	if req.InteractionPolicy == "preserve_history" {
		blockers = append(blockers, contentResetBlocker{Code: "interaction_history_policy_not_qualified", Owner: "wahb-platform/cms", Reason: "Retaining consumer history while retiring content has no qualified history-view contract.", NextAction: "Keep interacted content protected until consumer history behavior is implemented and qualified."})
	}
	if req.CoveragePolicy == "require_exact" && revision.ProtectedCount > 0 {
		blockers = append(blockers, contentResetBlocker{Code: "protected_scope", Owner: "cms/retention", Reason: "The exact selected scope contains protected content.", NextAction: "Review listed protected items or choose the preserve-protected policy."})
	}
	if req.Scope.Kind == "published_between" && revision.UnknownDateCount > 0 {
		blockers = append(blockers, contentResetBlocker{Code: "content_missing_publish_timestamp", Owner: "cms/content", Reason: "Some content in the selected lane has no publication timestamp, so date-window coverage is incomplete.", NextAction: "Use an explicit selection or resolve the unknown timestamps before exact coverage."})
	}
	if req.Scope.Kind == "source_ids" && revision.UnattributedCount > 0 {
		blockers = append(blockers, contentResetBlocker{Code: "legacy_source_identity_ambiguous", Owner: "cms/source-identity", Reason: "Some content in the selected lane has no canonical source ID and cannot be safely assigned to or excluded from the requested source scope.", NextAction: "Reconcile legacy source identity before source-scoped execution."})
	}
	if req.Scope.Kind == "explicit_ids" && revision.TargetCount != int64(len(req.Scope.ContentItemIDs)) {
		revision.MissingExplicitCount = len(req.Scope.ContentItemIDs) - int(revision.TargetCount)
		if revision.MissingExplicitCount < 0 {
			revision.MissingExplicitCount = 0
		}
		blockers = append(blockers, contentResetBlocker{Code: "explicit_targets_missing", Owner: "cms/content-reset", Reason: "One or more requested identifiers do not match the tenant, lane, or status filter.", NextAction: "Refresh the selection and create a new plan with the exact intended identifiers."})
	}
	manifestHash := hashContentResetValue(map[string]any{
		"request_hash": hashContentResetValue(req), "selection_highwater": revision.SelectionHighwater,
		"inventory_highwater": revision.InventoryHighwater,
		"target_count":        revision.TargetCount, "protected_count": revision.ProtectedCount,
		"unknown_date_count": revision.UnknownDateCount, "unattributed_count": revision.UnattributedCount,
		"deletions_since_boundary": deletionsSinceBoundary,
		"replay_source_hash":       revision.ReplaySourceHash, "chain_hash": revision.ManifestChainHash,
		"blockers": blockers,
	})
	blockerJSON, err := json.Marshal(blockers)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	expires := now.Add(contentResetPlanTTL)
	revision.State = "previewed"
	campaign.State = "previewed"
	if len(blockers) > 0 {
		revision.State = "blocked"
		campaign.State = "blocked"
	}
	revision.ManifestHash = &manifestHash
	revision.Blockers = datatypes.JSON(blockerJSON)
	revision.PlanningCompletedAt = &now
	revision.ExpiresAt = &expires
	if err := tx.Model(revision).Updates(map[string]any{
		"state": revision.State, "manifest_hash": manifestHash,
		"blockers": revision.Blockers, "planning_completed_at": now,
		"expires_at": expires, "missing_explicit_count": revision.MissingExplicitCount,
	}).Error; err != nil {
		return err
	}
	if err := tx.Model(campaign).Update("state", campaign.State).Error; err != nil {
		return err
	}
	return appendContentResetEvidence(tx, *campaign, revision, "planning-completed", "planning_completed", "cms/content-reset", map[string]any{
		"state": campaign.State, "revision": revision.Revision,
		"manifest_hash": manifestHash, "target_count": revision.TargetCount,
		"inventory_highwater":      revision.InventoryHighwater,
		"deletions_since_boundary": deletionsSinceBoundary,
		"protected_count":          revision.ProtectedCount,
		"unknown_date_count":       revision.UnknownDateCount,
		"unattributed_count":       revision.UnattributedCount,
		"missing_explicit_count":   revision.MissingExplicitCount,
		"blockers":                 blockers,
		"planning_completed_at":    now,
	})
}

func loadContentResetDetail(db *gorm.DB, tenant string, id uuid.UUID) contentResetCampaignDetail {
	var result contentResetCampaignDetail
	if db.Where("tenant_id = ? AND public_id = ?", tenant, id).First(&result.Campaign).Error != nil {
		return result
	}
	_ = db.Where("tenant_id = ? AND campaign_id = ? AND revision = ?", tenant, result.Campaign.ID, result.Campaign.CurrentRevision).First(&result.Revision).Error
	return result
}

func ListContentResetCampaigns(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	var campaigns []models.ContentResetCampaign
	if err := c.MustGet("db").(*gorm.DB).Where("tenant_id = ?", principal.TenantID).Order("created_at DESC").Limit(50).Find(&campaigns).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list Content Reset campaigns"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": campaigns})
}

func GetContentResetCampaign(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "campaign not found"})
		return
	}
	result := loadContentResetDetail(c.MustGet("db").(*gorm.DB), principal.TenantID, id)
	if result.Campaign.PublicID == uuid.Nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "campaign not found"})
		return
	}
	c.JSON(http.StatusOK, result)
}

func AdvanceContentResetPlanning(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "campaign not found"})
		return
	}
	var request struct {
		ExpectedCursor *int64 `json:"expected_cursor"`
	}
	if err := decodeContentResetJSON(c, &request); err != nil || request.ExpectedCursor == nil || *request.ExpectedCursor < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "expected_cursor is required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	if err := advanceContentResetPlanning(db, principal.TenantID, id, *request.ExpectedCursor); err != nil {
		if strings.Contains(err.Error(), "stale planning cursor") {
			c.JSON(http.StatusConflict, gin.H{"error": "planning cursor changed; reload the campaign"})
			return
		}
		c.JSON(http.StatusConflict, gin.H{"error": "Content Reset planning could not advance"})
		return
	}
	c.JSON(http.StatusOK, loadContentResetDetail(db, principal.TenantID, id))
}

func ListContentResetTargets(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "campaign not found"})
		return
	}
	var campaign models.ContentResetCampaign
	if err := c.MustGet("db").(*gorm.DB).Where("tenant_id = ? AND public_id = ?", principal.TenantID, id).First(&campaign).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "campaign not found"})
		return
	}
	var revision models.ContentResetRevision
	if err := c.MustGet("db").(*gorm.DB).Where("tenant_id = ? AND campaign_id = ? AND revision = ?", principal.TenantID, campaign.ID, campaign.CurrentRevision).First(&revision).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "campaign revision not found"})
		return
	}
	cursor, cursorErr := strconv.ParseInt(c.DefaultQuery("cursor", "0"), 10, 64)
	limit, limitErr := strconv.Atoi(c.DefaultQuery("limit", "200"))
	if cursorErr != nil || cursor < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cursor must be a non-negative integer"})
		return
	}
	if limitErr != nil || limit < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "limit must be a positive integer"})
		return
	}
	if limit > 500 {
		limit = 500
	}
	db := c.MustGet("db").(*gorm.DB)
	var targets []models.ContentResetTarget
	if err := db.Where("tenant_id = ? AND revision_id = ? AND item_ordinal > ?", principal.TenantID, revision.ID, cursor).Order("item_ordinal ASC").Limit(limit + 1).Find(&targets).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list Content Reset targets"})
		return
	}
	hasMore := len(targets) > limit
	if hasMore {
		targets = targets[:limit]
	}
	for _, target := range targets {
		canonical, err := contentResetCanonicalJSON(target.Snapshot)
		if err != nil || hashContentResetValue(json.RawMessage(canonical)) != target.SnapshotHash {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Content Reset target snapshot integrity check failed"})
			return
		}
	}
	var nextCursor int64
	if len(targets) > 0 {
		nextCursor = targets[len(targets)-1].ItemOrdinal
	}
	c.JSON(http.StatusOK, gin.H{"data": targets, "next_cursor": nextCursor, "limit": limit, "has_more": hasMore})
}

func ListContentResetEvidence(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "campaign not found"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var campaign models.ContentResetCampaign
	if err := db.Where("tenant_id = ? AND public_id = ?", principal.TenantID, id).First(&campaign).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "campaign not found"})
		return
	}
	cursor, cursorErr := strconv.ParseInt(c.DefaultQuery("cursor", "0"), 10, 64)
	limit, limitErr := strconv.Atoi(c.DefaultQuery("limit", "100"))
	if cursorErr != nil || cursor < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cursor must be a non-negative integer"})
		return
	}
	if limitErr != nil || limit < 1 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	var rows []models.ContentResetEvidence
	if err := db.Where("tenant_id = ? AND campaign_id = ? AND id > ? AND evidence_type IN ?", principal.TenantID, campaign.ID, cursor, contentResetPreviewEvidenceTypes).
		Order("id ASC").Limit(limit + 1).Find(&rows).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list Content Reset evidence"})
		return
	}
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}
	data := make([]contentResetPublicEvidence, 0, len(rows))
	var nextCursor int64
	for _, row := range rows {
		canonical, err := contentResetCanonicalJSON(row.Payload)
		if err != nil || hashContentResetValue(json.RawMessage(canonical)) != row.PayloadHash {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Content Reset evidence integrity check failed"})
			return
		}
		data = append(data, contentResetPublicEvidence{
			ID: row.PublicID, EvidenceKey: row.EvidenceKey, EvidenceType: row.EvidenceType,
			Owner: row.Owner, Payload: row.Payload, PayloadHash: row.PayloadHash,
			ObservedAt: row.ObservedAt,
		})
		nextCursor = int64(row.ID)
	}
	c.JSON(http.StatusOK, gin.H{"data": data, "next_cursor": nextCursor, "limit": limit, "has_more": hasMore})
}

func CancelContentResetCampaign(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "campaign not found"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	err = db.Transaction(func(tx *gorm.DB) error {
		var campaign models.ContentResetCampaign
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id = ? AND public_id = ?", principal.TenantID, id).First(&campaign).Error; err != nil {
			return err
		}
		if campaign.State == "cancelled" {
			return nil
		}
		if campaign.State != "planning" && campaign.State != "blocked" && campaign.State != "previewed" {
			return errors.New("only an unexecuted campaign can be cancelled")
		}
		var outstandingSteps int64
		if err := tx.Model(&models.ContentResetStep{}).
			Where("tenant_id = ? AND campaign_id = ?", principal.TenantID, campaign.ID).
			Count(&outstandingSteps).Error; err != nil {
			return err
		}
		if outstandingSteps != 0 {
			return errors.New("campaign with owner steps cannot be cancelled as an unexecuted preview")
		}
		var revision models.ContentResetRevision
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id = ? AND campaign_id = ? AND revision = ?", principal.TenantID, campaign.ID, campaign.CurrentRevision).First(&revision).Error; err != nil {
			return err
		}
		if revision.State != "planning" && revision.State != "blocked" && revision.State != "previewed" && revision.State != "superseded" {
			return errors.New("campaign revision is not an unexecuted preview")
		}
		now := time.Now().UTC()
		if err := tx.Model(&campaign).Updates(map[string]any{"state": "cancelled", "cancelled_at": now, "updated_at": now}).Error; err != nil {
			return err
		}
		if revision.State != "superseded" {
			if err := tx.Model(&revision).Update("state", "superseded").Error; err != nil {
				return err
			}
		}
		if err := appendContentResetEvidence(tx, campaign, &revision, "preview-cancelled", "preview_cancelled", "cms/content-reset", map[string]any{
			"cancelled_at": now.UTC(), "state": "cancelled", "revision_state": "superseded",
		}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "campaign cannot be cancelled"})
		return
	}
	c.JSON(http.StatusOK, loadContentResetDetail(db, principal.TenantID, id))
}
