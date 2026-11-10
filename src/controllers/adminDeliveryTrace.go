package controllers

import (
	"content-management-system/src/feedcontract"
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type deliveryRow struct {
	Status     string     `json:"status"`
	Detail     string     `json:"detail"`
	Owner      string     `json:"owner,omitempty"`
	ObservedAt *time.Time `json:"observed_at,omitempty"`
}

type deliveryLink struct {
	Label string `json:"label"`
	Href  string `json:"href"`
}

type deliveryObservation struct {
	Boundary   string     `json:"boundary"`
	Scope      string     `json:"scope"`
	ObservedAt time.Time  `json:"observed_at"`
	Generation *uuid.UUID `json:"generation,omitempty"`
	Historical bool       `json:"historical"`
}

type deliverySummary struct {
	Version              string                `json:"version"`
	ID                   uuid.UUID             `json:"id"`
	Title                string                `json:"title"`
	ItemType             string                `json:"item_type"`
	AsOf                 time.Time             `json:"as_of"`
	ProcessingGeneration int64                 `json:"processing_generation"`
	ActiveFeedGeneration *uuid.UUID            `json:"active_feed_generation,omitempty"`
	Source               deliveryRow           `json:"source"`
	Processing           deliveryRow           `json:"processing"`
	Eligibility          deliveryRow           `json:"eligibility"`
	Publication          deliveryRow           `json:"publication"`
	Delivery             deliveryRow           `json:"delivery"`
	Observations         []deliveryObservation `json:"observations"`
	Links                []deliveryLink        `json:"links"`
}

var errDeliveryRankingScope = errors.New("ranking evidence is available for Pods items")

func withDeliveryItem(c *gin.Context, snapshot bool, read func(*gorm.DB, utils.AdminPrincipal, models.ContentItem) (any, error)) {
	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil || id == uuid.Nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Enter a valid content ID"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	db := c.MustGet("db").(*gorm.DB).WithContext(ctx)
	var payload any
	load := func(tx *gorm.DB) error {
		var item models.ContentItem
		if err := tx.Select("public_id", "tenant_id", "title", "type", "status", "processing_generation", "duration_sec",
			"parent_content_item_id", "content_source_id", "source_run_request_id", "story_id", "feed_visibility").
			Where("tenant_id = ? AND public_id = ?", p.TenantID, id).First(&item).Error; err != nil {
			return err
		}
		var err error
		payload, err = read(tx, p, item)
		return err
	}
	if snapshot {
		err = db.Transaction(load, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	} else {
		err = load(db)
	}
	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		c.JSON(http.StatusNotFound, gin.H{"message": "This content item is unavailable"})
	case errors.Is(err, errDeliveryRankingScope):
		c.JSON(http.StatusBadRequest, gin.H{"message": "Ranking evidence is available for Pods items"})
	case err != nil:
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Delivery evidence could not be read"})
	default:
		c.JSON(http.StatusOK, gin.H{"data": payload})
	}
}

func AdminGetDeliverySummary(c *gin.Context) {
	withDeliveryItem(c, true, func(db *gorm.DB, p utils.AdminPrincipal, item models.ContentItem) (any, error) {
		return readDeliverySummary(db, p, item), db.Statement.Context.Err()
	})
}

func readDeliverySummary(db *gorm.DB, p utils.AdminPrincipal, item models.ContentItem) deliverySummary {
	media := item.Type == models.ContentTypeVideo || item.Type == models.ContentTypePodcast
	title := "Untitled content"
	if item.Title != nil && *item.Title != "" {
		title = *item.Title
	}
	summary := deliverySummary{
		Version: "delivery-summary/v1", ID: item.PublicID, Title: title, ItemType: string(item.Type), AsOf: time.Now().UTC(),
		ProcessingGeneration: item.ProcessingGeneration, Links: []deliveryLink{}, Observations: []deliveryObservation{},
	}
	if media && item.ParentContentItemID != nil {
		summary.ItemType = "PODS_CHAPTER"
	} else if media && item.DurationSec != nil && *item.DurationSec > feedcontract.PodsHardMaxDuration {
		summary.ItemType = "PODS_PARENT"
	}
	summary.Source = deliveryRow{Status: "No source recorded", Detail: "CMS has no source identity for this item."}
	if item.ContentSourceID != nil {
		var source models.ContentSource
		err := db.Select("public_id", "name").Where("tenant_id = ? AND public_id = ?", p.TenantID, *item.ContentSourceID).First(&source).Error
		switch {
		case err == nil:
			summary.Source = deliveryRow{Status: "Source recorded", Detail: source.Name}
			if p.HasPermission("source:read") {
				summary.Links = append(summary.Links, deliveryLink{"View source", "/platform/sources/" + source.PublicID.String()})
			}
		case errors.Is(err, gorm.ErrRecordNotFound):
			summary.Source = deliveryRow{Status: "Source unavailable", Detail: "The item retains a source reference."}
		default:
			summary.Source = unavailableDeliveryRow()
		}
	}
	if item.SourceRunRequestID != nil {
		var run models.SourceRunRequest
		err := db.Select("public_id", "state", "finished_at").Where("tenant_id = ? AND id = ?", p.TenantID, *item.SourceRunRequestID).First(&run).Error
		if err == nil {
			if summary.Source.Status == "Source recorded" {
				summary.Source.Owner = summary.Source.Detail
			}
			summary.Source.Status = "Source run recorded"
			summary.Source.Detail = "Source run is " + run.State + "."
			summary.Source.ObservedAt = run.FinishedAt
			if media {
				summary.Links = append(summary.Links, deliveryLink{"View source-run trace", "/platform/media/circulation?trace=" + run.PublicID.String()})
			}
		}
	}
	if media && p.HasPermission("content:read") {
		params := url.Values{"tab": {"diagnostics"}, "item": {item.PublicID.String()}}
		if item.ParentContentItemID != nil {
			params.Set("item", item.ParentContentItemID.String())
			params.Set("chapter", item.PublicID.String())
		}
		summary.Links = append([]deliveryLink{{"Open Media Studio", "/platform/media/atomization?" + params.Encode()}}, summary.Links...)
	} else if item.Type == models.ContentTypeNews && item.StoryID != nil && p.HasPermission("content:read") {
		summary.Links = append([]deliveryLink{{"View News story", "/platform/news?view=topics&story=" + item.StoryID.String()}}, summary.Links...)
	}
	summary.Processing = readDeliveryProcessing(db, item)
	summary.Eligibility = readDeliveryEligibility(db, item, media)
	summary.Publication, summary.ActiveFeedGeneration = readDeliveryPublication(db, item, media)
	summary.Delivery, summary.Observations = readDeliveryObservations(db, item, media, summary.ActiveFeedGeneration)
	return summary
}

func unavailableDeliveryRow() deliveryRow {
	return deliveryRow{Status: "Evidence unavailable", Detail: "CMS could not verify this step."}
}

func readDeliveryProcessing(db *gorm.DB, item models.ContentItem) deliveryRow {
	if !db.Migrator().HasTable(&models.ContentStageRequest{}) {
		return unavailableDeliveryRow()
	}
	var requests []models.ContentStageRequest
	err := db.Select("stage", "owner", "blocking_scope", "state", "processing_generation", "updated_at").
		Where("tenant_id = ? AND content_item_id = ? AND processing_generation = ?", item.TenantID, item.PublicID, item.ProcessingGeneration).
		Order("stage, updated_at DESC, id DESC").Limit(101).Find(&requests).Error
	if err != nil || len(requests) > 100 {
		return unavailableDeliveryRow()
	}
	return summarizeDeliveryProcessing(item.ProcessingGeneration, requests)
}

func summarizeDeliveryProcessing(generation int64, requests []models.ContentStageRequest) deliveryRow {
	result := deliveryRow{Status: "No stage evidence", Detail: "CMS has no durable stage records for this processing generation."}
	priority := 0
	seen := map[string]bool{}
	for _, request := range requests {
		if request.ProcessingGeneration != generation || request.State == models.ContentStageSuperseded || seen[request.Stage] {
			continue
		}
		seen[request.Stage] = true
		status, weight := deliveryStageStatus(request)
		if weight > priority {
			priority = weight
			result = deliveryRow{Status: status, Detail: request.Stage, Owner: request.Owner, ObservedAt: &request.UpdatedAt}
		}
	}
	return result
}

func deliveryStageStatus(request models.ContentStageRequest) (string, int) {
	if request.BlockingScope == models.ContentStageBlockingOptional {
		switch request.State {
		case models.ContentStageFailed, models.ContentStageUncertain, models.ContentStageCancelled:
			return "Optional stage needs review", 2
		}
		return "Optional stage recorded", 1
	}
	switch request.State {
	case models.ContentStageFailed, models.ContentStageBlocked, models.ContentStageCancelled:
		return "Required stage blocked", 6
	case models.ContentStageUncertain, models.ContentStageReconciling:
		return "Stage outcome uncertain", 5
	case models.ContentStageVerified:
		return "Recorded stages verified", 1
	default:
		return "Waiting on required stage", 4
	}
}

func readDeliveryEligibility(db *gorm.DB, item models.ContentItem, media bool) deliveryRow {
	var count int64
	if media {
		atomized := feedcontract.SupportsAtomizedPodsSchema(db)
		err := feedcontract.PodsEligibleMediaQuery(db, item.TenantID, atomized).
			Where("content_items.public_id = ?", item.PublicID).Count(&count).Error
		if err != nil {
			return unavailableDeliveryRow()
		}
		if count == 1 {
			return deliveryRow{Status: "Eligible for Pods", Detail: "The item meets the current Pods eligibility rules."}
		}
		if item.DurationSec != nil && *item.DurationSec > feedcontract.PodsHardMaxDuration && item.ParentContentItemID == nil {
			return deliveryRow{Status: "Excluded by policy", Detail: "This long parent is reviewed through its feed-ready chapters."}
		}
		if item.Status == models.ContentStatusReady && atomized && item.FeedVisibility != feedcontract.FeedVisibilityShown {
			return deliveryRow{Status: "Excluded from Pods", Detail: "The item’s current feed visibility is restricted."}
		}
		return deliveryRow{Status: "Requirements not met", Detail: "The item does not currently meet the Pods serving rules."}
	}
	if item.Type != models.ContentTypeNews {
		return deliveryRow{Status: "Not applicable", Detail: "The item has no supported News or Pods lane."}
	}
	if item.Status != models.ContentStatusReady || item.StoryID == nil {
		return deliveryRow{Status: "Requirements not met", Detail: "News requires a ready item with an assigned story."}
	}
	if !db.Migrator().HasColumn(&models.ContentItem{}, "news_retention_state") {
		return unavailableDeliveryRow()
	}
	if err := db.Model(&models.ContentItem{}).Where("tenant_id = ? AND public_id = ?", item.TenantID, item.PublicID).
		Where(newsRetentionFeedPredicate).Count(&count).Error; err != nil {
		return unavailableDeliveryRow()
	}
	if count == 0 {
		return deliveryRow{Status: "Excluded by policy", Detail: "Current News retention rules exclude this item."}
	}
	return deliveryRow{Status: "Ready for publication check", Detail: "The item is ready, assigned to a story, and passes the News retention filter."}
}

func readDeliveryPublication(db *gorm.DB, item models.ContentItem, media bool) (deliveryRow, *uuid.UUID) {
	if !media && item.Type != models.ContentTypeNews {
		return deliveryRow{Status: "Not applicable", Detail: "The item has no supported publication lane."}, nil
	}
	lane, kind, member := "media", "feed_unit", item.PublicID
	if !media {
		lane, kind = "news", "story"
		if item.StoryID == nil {
			return deliveryRow{Status: "No story assigned", Detail: "CMS cannot check News publication without a story."}, nil
		}
		member = *item.StoryID
	}
	if !db.Migrator().HasTable(&models.FeedGenerationHead{}) || !db.Migrator().HasTable(&models.FeedGenerationMembership{}) {
		return unavailableDeliveryRow(), nil
	}
	var head models.FeedGenerationHead
	err := db.Where("tenant_id = ? AND lane = ?", item.TenantID, lane).First(&head).Error
	if errors.Is(err, gorm.ErrRecordNotFound) || (err == nil && head.ActiveGenerationID == nil) {
		return deliveryRow{Status: "No active feed generation", Detail: "CMS has no active generation for this feed."}, nil
	}
	if err != nil {
		return unavailableDeliveryRow(), nil
	}
	var active models.FeedGeneration
	if err := db.Select("public_id").Where("public_id = ? AND tenant_id = ? AND lane = ? AND state = 'active'", *head.ActiveGenerationID, item.TenantID, lane).First(&active).Error; err != nil {
		return unavailableDeliveryRow(), nil
	}
	if media && item.ParentContentItemID == nil && item.DurationSec != nil && *item.DurationSec > feedcontract.PodsHardMaxDuration {
		return readParentPublication(db, item, active.PublicID), head.ActiveGenerationID
	}
	var row models.FeedGenerationMembership
	query := db.Where("generation_id = ?", *head.ActiveGenerationID)
	err = query.Session(&gorm.Session{}).Where("member_type = ? AND member_id = ?", kind, member).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return deliveryRow{Status: "Outside active generation", Detail: "The active feed generation has no matching membership."}, head.ActiveGenerationID
	}
	if err != nil {
		return unavailableDeliveryRow(), head.ActiveGenerationID
	}
	if media {
		return deliveryRow{Status: "In active Pods generation", Detail: "CMS recorded membership for this exact feed unit.", ObservedAt: &row.AttachedAt}, head.ActiveGenerationID
	}
	exact, known := feedcontract.NewsExactMembershipSchema(db)
	if !known {
		return deliveryRow{Status: "Story included", Detail: "Exact item membership evidence is unavailable.", ObservedAt: &row.AttachedAt}, head.ActiveGenerationID
	}
	if !exact {
		return deliveryRow{Status: "Story included", Detail: "This generation schema records membership at story level.", ObservedAt: &row.AttachedAt}, head.ActiveGenerationID
	}
	var exactRow models.FeedGenerationMembership
	err = query.Where("member_type = 'news_item' AND member_id = ?", item.PublicID).First(&exactRow).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return deliveryRow{Status: "Story included", Detail: "This exact item is outside the active News generation."}, head.ActiveGenerationID
	}
	if err != nil {
		return deliveryRow{Status: "Story included", Detail: "Exact item membership evidence is unavailable."}, head.ActiveGenerationID
	}
	return deliveryRow{Status: "In active News generation", Detail: "CMS recorded membership for both the story and this exact item.", ObservedAt: &exactRow.AttachedAt}, head.ActiveGenerationID
}

func readParentPublication(db *gorm.DB, item models.ContentItem, active uuid.UUID) deliveryRow {
	if !db.Migrator().HasTable(&models.AtomizationGeneration{}) {
		return unavailableDeliveryRow()
	}
	var published models.AtomizationGeneration
	err := db.Select("public_id", "generation_number", "processing_generation", "activation_at").
		Where("tenant_id = ? AND parent_content_item_id = ? AND state = 'active'", item.TenantID, item.PublicID).
		Order("generation_number DESC").First(&published).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return deliveryRow{Status: "No published chapter generation", Detail: "This parent publishes through its chapters."}
	}
	if err != nil {
		return unavailableDeliveryRow()
	}
	var chapters int64
	err = db.Table("feed_generation_memberships AS fgm").
		Joins("JOIN content_items AS child ON child.public_id = fgm.member_id").
		Where("fgm.generation_id = ? AND fgm.member_type = 'feed_unit' AND child.tenant_id = ? AND child.parent_content_item_id = ? AND child.metadata->>'atomization_generation_id' = ?", active, item.TenantID, item.PublicID, published.PublicID.String()).
		Count(&chapters).Error
	if err != nil {
		return unavailableDeliveryRow()
	}
	if chapters == 0 {
		return deliveryRow{Status: "Chapters outside active feed generation", Detail: "No chapter from the published episode generation is recorded in the active feed."}
	}
	detail := strconv.FormatInt(chapters, 10) + " in active Pods generation · episode generation " + strconv.Itoa(published.GenerationNumber) + "."
	if published.ProcessingGeneration < item.ProcessingGeneration {
		detail += " The earlier processing generation remains published."
	}
	return deliveryRow{Status: "Published chapters", Detail: detail, ObservedAt: published.ActivationAt}
}

func readDeliveryObservations(db *gorm.DB, item models.ContentItem, media bool, active *uuid.UUID) (deliveryRow, []deliveryObservation) {
	result := []deliveryObservation{}
	if !media {
		return deliveryRow{Status: "Evidence unavailable", Detail: "This trace has no exact News consumer-boundary observation source."}, result
	}
	if item.ParentContentItemID == nil && item.DurationSec != nil && *item.DurationSec > feedcontract.PodsHardMaxDuration {
		return deliveryRow{Status: "Review chapter delivery", Detail: "Consumer delivery evidence is recorded for individual feed units."}, result
	}
	if !db.Migrator().HasTable(&models.PodsBoundaryObservation{}) {
		return unavailableDeliveryRow(), result
	}
	current := false
	for _, boundary := range []string{"feed_return", "page_render"} {
		base := db.Select("boundary", "probe_kind", "observed_at", "generation_id").
			Where("tenant_id = ? AND content_item_id = ? AND boundary = ? AND verdict = 'present'", item.TenantID, item.PublicID, boundary)
		for _, historical := range []bool{false, true} {
			if active == nil && !historical {
				continue
			}
			query := base.Session(&gorm.Session{})
			if active != nil && historical {
				query = query.Where("generation_id IS DISTINCT FROM ?", *active)
			} else if active != nil {
				query = query.Where("generation_id = ?", *active)
			}
			var row models.PodsBoundaryObservation
			err := query.Order("observed_at DESC, public_id DESC").First(&row).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				continue
			}
			if err != nil {
				return unavailableDeliveryRow(), result
			}
			result = append(result, deliveryObservation{row.Boundary, row.ProbeKind, row.ObservedAt, row.GenerationID, historical})
			current = current || !historical
		}
	}
	if current {
		return deliveryRow{Status: "Delivery observed", Detail: "CMS recorded a consumer-boundary observation for the active generation."}, result
	}
	if len(result) > 0 {
		return deliveryRow{Status: "Historical evidence only", Detail: "Recorded observations do not confirm delivery for the active generation."}, result
	}
	return deliveryRow{Status: "No observation recorded", Detail: "CMS found no confirmed feed return or client render for this item."}, result
}

type deliveryHistoryRow struct {
	RowID      uint64    `json:"-"`
	ID         uuid.UUID `json:"id"`
	Generation int64     `json:"processing_generation"`
	Stage      string    `json:"stage"`
	Owner      string    `json:"owner"`
	State      string    `json:"state"`
	At         time.Time `json:"at"`
}

func AdminGetDeliveryEvidence(c *gin.Context) {
	section := c.DefaultQuery("section", "attempts")
	before, err := strconv.ParseUint(c.DefaultQuery("before", "0"), 10, 63)
	if err != nil || (section != "attempts" && section != "receipts" && section != "ranking") {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid delivery evidence selection"})
		return
	}
	withDeliveryItem(c, false, func(db *gorm.DB, p utils.AdminPrincipal, item models.ContentItem) (any, error) {
		if section == "ranking" {
			if item.Type != models.ContentTypeVideo && item.Type != models.ContentTypePodcast {
				return nil, errDeliveryRankingScope
			}
			now := time.Now().UTC()
			raw := podsRankingTrace(db, p.TenantID, item)
			if raw["error"] != nil {
				return nil, errors.New("ranking unavailable")
			}
			return gin.H{"as_of": now, "raw_rank": raw["raw_rank"], "final_rank": raw["final_rank"], "assembly_mode": raw["assembly_mode"]}, nil
		}
		const limit = 25
		table, state, stamp := "content_stage_attempts", "state", "created_at"
		if section == "receipts" {
			table, state, stamp = "content_stage_receipts", "outcome", "observed_at"
		}
		records := []deliveryHistoryRow{}
		query := db.Table(table+" AS evidence").
			Select("evidence.id AS row_id, evidence.public_id AS id, r.processing_generation AS generation, evidence.stage, evidence.owner, evidence."+state+" AS state, evidence."+stamp+" AS at").
			Joins("JOIN content_stage_requests r ON r.public_id = evidence.request_id AND r.tenant_id = evidence.tenant_id").
			Where("r.tenant_id = ? AND r.content_item_id = ?", p.TenantID, item.PublicID)
		if before > 0 {
			query = query.Where("evidence.id < ?", before)
		}
		if err := query.Order("evidence.id DESC").Limit(limit + 1).Scan(&records).Error; err != nil {
			return nil, err
		}
		next := ""
		if len(records) > limit {
			records = records[:limit]
			next = strconv.FormatUint(records[limit-1].RowID, 10)
		}
		return gin.H{"items": records, "next": next}, nil
	})
}
