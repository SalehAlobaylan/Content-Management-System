package controllers

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"content-management-system/src/contentstage"
	"content-management-system/src/models"
	"content-management-system/src/podsflow"
	"content-management-system/src/utils"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// SQL projection allows counts over the filtered inventory without transferring
// every episode to Go. The page and counts are from one statement/snapshot.
func JourneyMutationVersion(c *gin.Context) {
	if raw := c.Query("expected_generation"); raw != "" {
		version, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || version < 0 {
			c.AbortWithStatusJSON(400, utils.HTTPError{Code: 400, Message: "Invalid expected episode generation"})
			return
		}
		c.Set("db", c.MustGet("db").(*gorm.DB).Set(contentstage.JourneyExpectedGeneration, version))
	}
	c.Next()
}

const mediaJourneyLaneSQL = `CASE
 WHEN media_stage_state IN ('awaiting_approval','blocked') THEN 'awaiting_download'
 WHEN media_stage_state IN ('queued','deferred','claimed','running','verifying') THEN 'media'
 WHEN media_stage_state IN ('failed','uncertain','reconciling') THEN 'failed'
 WHEN duration_sec BETWEEN 270 AND 2400 AND status='READY' AND is_feed_unit AND feed_visibility='visible' THEN 'published'
 WHEN transcript_id IS NULL AND NULLIF(current_failure,'') IS NOT NULL THEN 'failed'
 WHEN transcript_id IS NULL AND transcript_stage_state IN ('awaiting_approval','blocked','queued','deferred','claimed','running','verifying') THEN 'transcript'
 WHEN transcript_id IS NULL AND transcript_stage_state IN ('failed','uncertain','reconciling') THEN 'failed'
 WHEN (atomization_stage_state IS NOT NULL AND current_failure IS NOT NULL) OR (atomization_stage_state IS NULL AND failed_or_stuck) THEN 'failed'
 WHEN atomization_stage_state IN ('failed','uncertain','reconciling') THEN 'failed'
 WHEN atomization_stage_state IN ('queued','claimed','running','deferred','verifying') THEN 'planning'
 WHEN atomization_stage_state='verified' AND review_count>0 THEN 'review'
 WHEN atomization_stage_state='verified' AND embedding_pending_count>0 THEN 'embedding'
 WHEN atomization_stage_state='verified' AND child_count>0 AND published_count=child_count THEN 'published'
 WHEN atomization_stage_state='verified' THEN 'embedding'
 WHEN atomization_override='disabled' THEN 'disabled'
 WHEN chaptering_status='failed' THEN 'failed'
 WHEN chaptering_status='needs_review' THEN 'review'
 WHEN chaptering_status IN ('completed','published') THEN 'published'
 WHEN chaptering_status IN ('embedding','embedding_pending') THEN 'embedding'
 WHEN chaptering_status IN ('cutting','renditions','children','planning') THEN 'planning'
 WHEN chaptering_status IN ('waiting_transcript','transcript_ready') THEN 'transcript'
 WHEN chaptering_status='waiting_media' THEN 'media'
 WHEN COALESCE(chaptering_status,'unstarted') IN ('queued','media_ready','unstarted','') THEN 'ready'
 WHEN transcript_id IS NULL THEN 'transcript' ELSE 'ready' END`

func adminMediaPipelineList(c *gin.Context, db *gorm.DB, tenant string) {
	lane := c.Query("lane")
	if lane == "all" {
		lane = ""
	}
	valid := lane == ""
	for _, col := range defaultPipelineColumns() {
		if col.Key == lane {
			valid = true
		}
	}
	if !valid {
		c.JSON(400, utils.HTTPError{Code: 400, Message: "Unknown workflow lane"})
		return
	}
	query, args := mediaPipelineQuery(tenant, pipelineQueryFilters(c))
	limit := boundedLimit(c.Query("limit"), 25, 100)
	var result struct {
		Items  datatypes.JSON
		Counts datatypes.JSON
		Total  int64
	}
	args = append(args, lane, lane, c.Query("cursor"), limit+1)
	if err := db.Raw(`WITH inventory AS (`+query+`), classified AS MATERIALIZED (SELECT inventory.*,`+mediaJourneyLaneSQL+` AS projected_lane FROM inventory), selected AS (SELECT * FROM classified WHERE (?='' OR projected_lane=?)), page AS (SELECT * FROM selected WHERE id>? ORDER BY id LIMIT ?) SELECT (SELECT COALESCE(jsonb_agg(to_jsonb(page)),'[]'::jsonb) FROM page) AS items,(SELECT COALESCE(jsonb_agg(to_jsonb(counts)),'[]'::jsonb) FROM (SELECT projected_lane AS key,COUNT(*) AS count FROM classified GROUP BY projected_lane) counts) AS counts,(SELECT COUNT(*) FROM selected) AS total`, args...).Scan(&result).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	rows := []mediaAtomizationPipelineItem{}
	counts := []struct {
		Key   string
		Count int
	}{}
	if err := decodeMediaProjectionJSON(result.Items, &rows); err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	if err := json.Unmarshal(result.Counts, &counts); err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	next := ""
	if len(rows) > limit {
		rows = rows[:limit]
		next = rows[len(rows)-1].ID
	}
	// Enrich only this page with the existing CMS-owned action projection.
	byID := map[string]mediaAtomizationPipelineItem{}
	for _, col := range projectMediaPipelineRows(rows) {
		for _, item := range col.Items {
			byID[item.ID] = item
		}
	}
	for i := range rows {
		rows[i] = byID[rows[i].ID]
		principal, _ := utils.GetAdminPrincipal(c)
		rows[i].Actions = journeyActions(rows[i], principal)
	}
	progressUnavailable := false
	if len(rows) > 0 {
		ids := make([]string, 0, len(rows))
		for _, row := range rows {
			ids = append(ids, row.ID)
		}
		var progress []struct {
			ID string
			At time.Time
		}
		if err := db.Raw(`SELECT r.content_item_id::text AS id,MAX(e.occurred_at) AS at FROM content_stage_events e JOIN content_stage_requests r ON r.tenant_id=e.tenant_id AND r.public_id=e.request_id JOIN content_items i ON i.tenant_id=r.tenant_id AND i.public_id=r.content_item_id AND i.processing_generation=r.processing_generation WHERE r.tenant_id=? AND r.content_item_id IN ? AND e.event_type LIKE 'checkpoint:%' GROUP BY r.content_item_id`, tenant, ids).Scan(&progress).Error; err != nil {
			progressUnavailable = true
		}
		for i := range rows {
			for _, cp := range progress {
				if cp.ID == rows[i].ID {
					at := cp.At
					rows[i].LastProgressAt = &at
				}
			}
		}
	}
	summaries := []gin.H{}
	for _, col := range defaultPipelineColumns() {
		n := 0
		for _, count := range counts {
			if count.Key == col.Key {
				n = count.Count
			}
		}
		summaries = append(summaries, gin.H{"key": col.Key, "label": col.Label, "count": n})
	}
	c.JSON(200, utils.ResponseMessage{Code: 200, Data: gin.H{"items": rows, "counts": summaries, "total": result.Total, "count_unit": "episodes", "next_cursor": next, "progress_unavailable": progressUnavailable, "updated_at": time.Now().UTC()}})
}

// Journey reads never call recovery/admission or providers. Unknown evidence is
// left unknown; updated_at and lease heartbeats are not progress measurements.
type mediaJourneyStep struct {
	DependsOn       []string             `json:"depends_on"`
	Actions         []mediaJourneyAction `json:"actions"`
	Key             string               `json:"key"`
	State           string               `json:"state"`
	Required        bool                 `json:"required"`
	Reason          string               `json:"reason_code"`
	WaitingCategory string               `json:"waiting_category,omitempty"`
	CompletedAt     *time.Time           `json:"completed_at,omitempty"`
	StartedAt       *time.Time           `json:"started_at,omitempty"`
	LastProgressAt  *time.Time           `json:"last_progress_at,omitempty"`
	Completed       *int                 `json:"completed,omitempty"`
	Total           *int                 `json:"total,omitempty"`
}

// Dependencies describe requirements, not a new scheduling sequence. Review
// and readiness may overlap; published joins both branches for long episodes.
func journeyDependencies(steps []mediaJourneyStep, direct bool) {
	deps := map[string][]string{"discovery": {}, "download": {"discovery"}, "preparation": {"download"}, "transcript": {"preparation"}, "planning": {"transcript"}, "cutting": {"planning"}, "review": {"cutting"}, "readiness": {"cutting"}, "published": {"review", "readiness"}}
	if direct {
		deps["readiness"], deps["published"] = []string{"preparation"}, []string{"readiness"}
	}
	for i := range steps {
		steps[i].DependsOn = deps[steps[i].Key]
		if !steps[i].Required {
			steps[i].DependsOn = []string{}
		}
	}
}

func journeyGeneration(g *models.AtomizationGeneration) any {
	if g == nil {
		return nil
	}
	return gin.H{"id": g.PublicID, "state": g.State, "processing_generation": g.ProcessingGeneration, "plan_origin": g.PlanOrigin, "plan": g.Plan, "expected_units": g.ExpectedUnits, "completed_units": g.CompletedUnits, "created_at": g.CreatedAt, "activation_at": g.ActivationAt}
}

func journeyCapacity(db *gorm.DB, parent models.ContentItem, slot podsflow.Slot, steps []mediaJourneyStep) gin.H {
	capacity := gin.H{"waiting": false}
	if slot.RootContentItemID == nil || (slot.TenantID != nil && *slot.TenantID == parent.TenantID && *slot.RootContentItemID == parent.PublicID) {
		return capacity
	}
	for i := range steps {
		if steps[i].State == "queued" {
			steps[i].WaitingCategory, steps[i].Reason = "execution_capacity", "shared_capacity"
			capacity["waiting"] = true
		}
	}
	// Do not even query the holder's identity outside the requesting tenant.
	if capacity["waiting"] == true && slot.TenantID != nil && *slot.TenantID == parent.TenantID {
		var holder models.ContentItem
		if db.Select("public_id", "title").Where("tenant_id=? AND public_id=?", parent.TenantID, slot.RootContentItemID).First(&holder).Error == nil {
			capacity["episode_id"], capacity["title"] = holder.PublicID, holder.Title
		}
	}
	return capacity
}

func journeyStage(key string, request *models.ContentStageRequest) mediaJourneyStep {
	s := mediaJourneyStep{Key: key, State: "pending", Required: true, Reason: "evidence_unavailable"}
	if request == nil {
		return s
	}
	s.Reason = key
	switch request.State {
	case models.ContentStageVerified:
		s.State, s.CompletedAt = "completed", request.VerifiedAt
	case models.ContentStageAwaitingApproval:
		s.State, s.WaitingCategory, s.Reason = "waiting", "operator", key+"_approval"
	case models.ContentStageBlocked:
		s.State, s.WaitingCategory, s.Reason = "waiting", "predecessor", "predecessor_required"
	case models.ContentStageDeferred:
		s.State, s.WaitingCategory, s.Reason = "waiting", "scheduled_retry", "scheduled_retry"
	case models.ContentStageQueued, models.ContentStageClaimed:
		s.State, s.Reason = "queued", "awaiting_execution"
	case models.ContentStageRunning, models.ContentStageVerifying:
		s.State = "running"
	case models.ContentStageUncertain, models.ContentStageReconciling:
		s.State, s.WaitingCategory, s.Reason = "reconciling", "reconciliation", "effects_unresolved"
	case models.ContentStageFailed:
		s.State, s.Reason = "failed", request.FailureClass
	case models.ContentStageCancelled, models.ContentStageSuperseded:
		s.State, s.Reason = "waiting", "request_cancelled"
	}
	return s
}

func journeyParent(c *gin.Context) (*gorm.DB, *models.ContentItem, bool) {
	p, ok := requireAdminPrincipal(c)
	if !ok {
		return nil, nil, false
	}
	db := c.MustGet("db").(*gorm.DB)
	parent, _, _, found, err := resolveMediaAtomizationContextParent(db, p.TenantID, c.Param("id"))
	if err != nil {
		mediaAtomizationQueryError(c, err)
		return nil, nil, false
	}
	if !found {
		c.JSON(404, utils.HTTPError{Code: 404, Message: "Media episode not found"})
		return nil, nil, false
	}
	return db, parent, true
}

func AdminGetMediaJourney(c *gin.Context) {
	db, parent, ok := journeyParent(c)
	if !ok {
		return
	}
	var requests []models.ContentStageRequest
	if err := db.Where("tenant_id=? AND content_item_id=? AND processing_generation=?", parent.TenantID, parent.PublicID, parent.ProcessingGeneration).Find(&requests).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	byStage := map[string]*models.ContentStageRequest{}
	for i := range requests {
		byStage[requests[i].Stage] = &requests[i]
	}
	steps := []mediaJourneyStep{{Key: "discovery", State: "completed", Required: true, Reason: "metadata_saved"}, journeyStage("download", byStage[models.ContentStagePodsMediaArtifacts]), journeyStage("preparation", byStage[models.ContentStagePodsMediaArtifacts]), journeyStage("transcript", byStage[models.ContentStagePodsTranscript]), journeyStage("planning", byStage[models.ContentStagePodsAtomization]), {Key: "cutting", State: "pending", Required: true, Reason: "plan_required"}, {Key: "review", State: "pending", Required: true, Reason: "cuts_required"}, {Key: "readiness", State: "pending", Required: true, Reason: "checks_required"}, {Key: "published", State: "pending", Required: true, Reason: "checks_required"}}
	var generations []models.AtomizationGeneration
	// Resolve each identity explicitly: a run of failed candidates must never
	// push the still-published generation outside an arbitrary history limit.
	if err := db.Raw(`SELECT * FROM atomization_generations WHERE public_id IN (
		(SELECT public_id FROM atomization_generations WHERE tenant_id=? AND parent_content_item_id=? AND state='active' ORDER BY generation_number DESC LIMIT 1)
		UNION
		(SELECT public_id FROM atomization_generations WHERE tenant_id=? AND parent_content_item_id=? AND processing_generation=? AND state<>'superseded' ORDER BY generation_number DESC LIMIT 1)
	) ORDER BY generation_number DESC`, parent.TenantID, parent.PublicID, parent.TenantID, parent.PublicID, parent.ProcessingGeneration).Scan(&generations).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	var published, candidate *models.AtomizationGeneration
	for i := range generations {
		g := &generations[i]
		if g.State == "active" && published == nil {
			published = g
		}
		if g.ProcessingGeneration == parent.ProcessingGeneration && candidate == nil {
			candidate = g
		}
	}
	if candidate != nil {
		steps[4].State, steps[4].Reason = "completed", "plan_frozen"
		steps[4].CompletedAt = &candidate.CreatedAt
		var unitStates []struct {
			State      string
			Count      int64
			StartedAt  *time.Time
			ProgressAt *time.Time
		}
		if err := db.Model(&models.AtomizationChapterUnit{}).Select("state,COUNT(*) AS count,MIN(effect_started_at) AS started_at,MAX(CASE WHEN state='verified' THEN updated_at END) AS progress_at").Where("tenant_id=? AND generation_id=?", parent.TenantID, candidate.PublicID).Group("state").Scan(&unitStates).Error; err != nil {
			mediaAtomizationQueryError(c, err)
			return
		}
		states := map[string]int64{}
		for _, state := range unitStates {
			states[state.State] = state.Count
		}
		steps[5] = journeyCuttingState(states)
		for _, unit := range unitStates {
			if unit.StartedAt != nil && (steps[5].StartedAt == nil || unit.StartedAt.Before(*steps[5].StartedAt)) {
				steps[5].StartedAt = unit.StartedAt
			}
			if unit.ProgressAt != nil {
				steps[5].LastProgressAt = unit.ProgressAt
			}
		}
		verified := int(states["verified"])
		steps[5].Completed, steps[5].Total = &verified, &candidate.ExpectedUnits
		if verified == candidate.ExpectedUnits && candidate.ExpectedUnits > 0 {
			steps[5].State, steps[5].Reason = "completed", "cuts_verified"
		}
		if candidate.State == "failed" && steps[5].State != "reconciling" && steps[5].State != "completed" {
			steps[5].State, steps[5].Reason = "failed", "cutting_failed"
		}
	}
	rows, err := loadMediaPipelineRows(db, parent.TenantID, map[string]string{"id": parent.PublicID.String()})
	if err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	var item *mediaAtomizationPipelineItem
	for _, col := range projectMediaPipelineRows(rows) {
		if len(col.Items) > 0 {
			value := col.Items[0]
			item = &value
		}
	}
	if item != nil {
		principal, _ := utils.GetAdminPrincipal(c)
		item.Actions = journeyActions(*item, principal)
		if steps[1].State != "completed" {
			steps[2] = mediaJourneyStep{Key: "preparation", Required: true, State: "waiting", Reason: "predecessor_required", WaitingCategory: "predecessor"}
			if item.MediaStagePhase != nil {
				switch *item.MediaStagePhase {
				case "probe", "upload", "renditions", "thumbnail", "verify", "complete":
					steps[1].State, steps[1].Reason = "completed", "download_completed"
					steps[2] = journeyStage("preparation", byStage[models.ContentStagePodsMediaArtifacts])
				}
			}
		}
		if item.ReviewCount > 0 {
			steps[6].State, steps[6].Reason, steps[6].WaitingCategory = "waiting", "editorial_review_required", "operator"
		} else if steps[5].State == "completed" {
			steps[6].State, steps[6].Reason = "completed", "review_clear"
		}
		if item.EmbeddingPendingCount > 0 {
			steps[7].State, steps[7].Reason = "waiting", "embedding_or_family_required"
		}
		if item.Lane == "published" {
			steps[7].State, steps[8].State = "completed", "completed"
			steps[7].Reason, steps[8].Reason = "publication_verified", "publication_verified"
			if published != nil {
				steps[7].CompletedAt, steps[8].CompletedAt = published.ActivationAt, published.ActivationAt
			}
		}
		if candidate != nil && candidate.State == "failed" && steps[5].State == "completed" {
			steps[7].State, steps[7].Reason = "failed", "generation_completion_failed"
		}
	}
	if parent.DurationSec != nil && *parent.DurationSec <= 2400 {
		for _, i := range []int{4, 5, 6} {
			steps[i].Required = false
			steps[i].State = "not_required"
			steps[i].Reason = "direct_media"
		}
		steps[3].Required = false
		if byStage[models.ContentStagePodsTranscript] == nil {
			steps[3].State, steps[3].Reason = "not_required", "direct_media"
		}
	}
	journeyDependencies(steps, parent.DurationSec != nil && *parent.DurationSec <= 2400)
	// Report checkpoint time, never request.updated_at (which may be a heartbeat).
	progressUnavailable := false
	type checkpoint struct {
		Stage string
		At    time.Time
	}
	var checkpoints []checkpoint
	if err := db.Raw(`SELECT r.stage, MAX(e.occurred_at) AS at FROM content_stage_events e JOIN content_stage_requests r ON r.tenant_id=e.tenant_id AND r.public_id=e.request_id WHERE r.tenant_id=? AND r.content_item_id=? AND r.processing_generation=? AND e.event_type LIKE 'checkpoint:%' GROUP BY r.stage`, parent.TenantID, parent.PublicID, parent.ProcessingGeneration).Scan(&checkpoints).Error; err != nil {
		progressUnavailable = true
	}
	for _, cp := range checkpoints {
		for i := range steps {
			if (cp.Stage == models.ContentStagePodsMediaArtifacts && (i == 1 || i == 2)) || (cp.Stage == models.ContentStagePodsTranscript && i == 3) || (cp.Stage == models.ContentStagePodsAtomization && (i == 4 || i == 5)) {
				at := cp.At
				steps[i].LastProgressAt = &at
			}
		}
	}
	var starts []struct {
		Stage string
		At    *time.Time
	}
	if err := db.Raw(`SELECT r.stage,MIN(a.effect_started_at) AS at FROM content_stage_requests r JOIN content_stage_attempts a ON a.tenant_id=r.tenant_id AND a.request_id=r.public_id WHERE r.tenant_id=? AND r.content_item_id=? AND r.processing_generation=? GROUP BY r.stage`, parent.TenantID, parent.PublicID, parent.ProcessingGeneration).Scan(&starts).Error; err != nil {
		progressUnavailable = true
	}
	for _, start := range starts {
		for i := range steps {
			if (start.Stage == models.ContentStagePodsMediaArtifacts && i == 1) || (start.Stage == models.ContentStagePodsTranscript && i == 3) || (start.Stage == models.ContentStagePodsAtomization && i == 4) {
				steps[i].StartedAt = start.At
			}
		}
	}
	var control models.ContentStageControl
	if err := db.Where("tenant_id=? AND lane=?", parent.TenantID, models.ContentStageLanePods).First(&control).Error; err == nil {
		for i := range steps {
			if (steps[i].State == "queued" || steps[i].WaitingCategory == "scheduled_retry") && (!control.ExecutionEnabled || !control.SchedulingEnabled || (i == 3 && !control.TranscriptExecutionEnabled)) {
				steps[i].State, steps[i].Reason, steps[i].WaitingCategory = "waiting", "operational_pause", "operational_pause"
			}
		}
	} else if err != gorm.ErrRecordNotFound {
		mediaAtomizationQueryError(c, err)
		return
	}
	var slot podsflow.Slot
	if err := db.Where("singleton=true").First(&slot).Error; err != nil && err != gorm.ErrRecordNotFound {
		mediaAtomizationQueryError(c, err)
		return
	}
	capacity := journeyCapacity(db, *parent, slot, steps)
	var transcript *models.Transcript
	if parent.TranscriptID != nil {
		var t models.Transcript
		if err := episodeTranscriptQuery(db, *parent).Select("public_id", "source", "provider", "approved_at", "approved_by").First(&t).Error; err == nil {
			transcript = &t
		} else if err != gorm.ErrRecordNotFound {
			mediaAtomizationQueryError(c, err)
			return
		}
	}
	var config models.TranscriptionConfig
	if err := db.Where("tenant_id=?", parent.TenantID).First(&config).Error; err != nil && err != gorm.ErrRecordNotFound {
		mediaAtomizationQueryError(c, err)
		return
	}
	effective, policyErr := readOnlyEffectiveMediaPolicy(db, parent)
	if policyErr != nil {
		mediaAtomizationQueryError(c, policyErr)
		return
	}
	var transcriptSummary any
	if transcript != nil {
		transcriptSummary = gin.H{"id": transcript.PublicID, "source": transcript.Source, "provider": transcript.Provider, "approved_at": transcript.ApprovedAt, "verified": steps[3].State == "completed"}
		if item != nil {
			principal, _ := utils.GetAdminPrincipal(c)
			a := mediaJourneyAction{Code: "approve_transcript_for_use", Step: "transcript", Enabled: transcript.ApprovedAt == nil && principal.HasPermission("content:publish"), Reason: "already_approved_or_permission_required"}
			if a.Enabled {
				a.Reason = ""
			}
			item.Actions = append(item.Actions, a)
		}
	}
	for i := range steps {
		steps[i].Actions = []mediaJourneyAction{}
		if item != nil {
			for _, a := range item.Actions {
				if a.Step == steps[i].Key {
					steps[i].Actions = append(steps[i].Actions, a)
				}
			}
		}
	}
	captionOutcome := "not_checked_or_unknown"
	var metadata map[string]any
	if json.Unmarshal(parent.Metadata, &metadata) == nil {
		if value, ok := metadata["caption_acquisition_outcome"].(string); ok && (value == "available" || value == "unavailable") {
			captionOutcome = value
		}
	}
	if parent.CaptionState != nil && (*parent.CaptionState == models.CaptionStateYouTubeAuto || *parent.CaptionState == models.CaptionStateYouTubeHuman) {
		captionOutcome = "available"
	}
	if item != nil && item.CurrentFailureSummary != nil && strings.Contains(strings.ToLower(*item.CurrentFailureSummary), "caption") && (steps[1].State == "failed" || steps[2].State == "failed") {
		captionOutcome = "retrieval_failed"
	}
	acquisitionMode, acquisitionSource, acquisitionErr := contentstage.ReadMediaAcquisitionPolicy(db, *parent)
	if acquisitionErr != nil {
		mediaAtomizationQueryError(c, acquisitionErr)
		return
	}
	activeExecution := []gin.H{}
	for _, r := range requests {
		if r.State == "claimed" || r.State == "running" || r.State == "verifying" {
			activeExecution = append(activeExecution, gin.H{"request_id": r.PublicID, "stage": r.Stage, "state": r.State, "worker": r.ClaimOwner, "lease_expires_at": r.ClaimExpiresAt})
		}
	}
	c.JSON(http.StatusOK, utils.ResponseMessage{Code: 200, Message: "Media journey fetched", Data: gin.H{"parent": mapMediaAtomizationContextParent(parent), "item": item, "steps": steps, "capacity": capacity, "active_execution": activeExecution, "published_generation": journeyGeneration(published), "candidate_generation": journeyGeneration(candidate), "transcript": transcriptSummary, "caption_state": parent.CaptionState, "caption_outcome": captionOutcome, "acquisition_mode": acquisitionMode, "acquisition_policy_source": acquisitionSource, "auto_stt_enabled": config.AutoSttEnabled, "effective_policy": effective.Policy, "policy_source": effective.PolicySource, "progress_unavailable": progressUnavailable, "updated_at": time.Now().UTC()}})
}

type mediaJourneyAction struct {
	Code    string `json:"code"`
	Step    string `json:"step"`
	Enabled bool   `json:"enabled"`
	Reason  string `json:"reason_code,omitempty"`
}

func journeyActions(item mediaAtomizationPipelineItem, principal utils.AdminPrincipal) []mediaJourneyAction {
	actions := []mediaJourneyAction{}
	for _, entry := range []struct{ code, step, permission string }{{"download", "download", "content:write"}, {"approve_transcript", "transcript", "content:write"}, {"retry_atomization", "planning", "content:write"}, {"review", "review", "content:read"}, {"inspect", "readiness", "content:read"}} {
		a := mediaJourneyAction{Code: entry.code, Step: entry.step, Reason: "prerequisites_required"}
		for _, allowed := range item.AllowedActions {
			if allowed == entry.code {
				a.Enabled = true
				a.Reason = ""
			}
		}
		if !principal.HasPermission(entry.permission) {
			a.Enabled = false
			a.Reason = "permission_required"
		}
		if entry.code == "retry_atomization" && item.UnresolvedEffects > 0 {
			a.Enabled = false
			a.Reason = "effects_unresolved"
		}
		actions = append(actions, a)
	}
	return actions
}

func journeyCuttingState(states map[string]int64) mediaJourneyStep {
	s := mediaJourneyStep{Key: "cutting", Required: true, State: "queued", Reason: "awaiting_execution"}
	switch {
	case states["uncertain"]+states["reconciling"] > 0:
		s.State, s.Reason, s.WaitingCategory = "reconciling", "effects_unresolved", "reconciliation"
	case states["failed"] > 0:
		s.State, s.Reason = "failed", "cutting_failed"
	case states["running"]+states["verifying"] > 0:
		s.State, s.Reason = "running", "cutting"
	}
	return s
}

func AdminGetMediaJourneyChapters(c *gin.Context) {
	db, parent, ok := journeyParent(c)
	if !ok {
		return
	}
	limit := boundedLimit(c.Query("limit"), 25, 100)
	var generation models.AtomizationGeneration
	err := db.Select("public_id").Where("tenant_id=? AND parent_content_item_id=? AND processing_generation=? AND state<>'superseded'", parent.TenantID, parent.PublicID, parent.ProcessingGeneration).Order("generation_number DESC").First(&generation).Error
	if err == gorm.ErrRecordNotFound {
		c.JSON(200, utils.ResponseMessage{Code: 200, Data: gin.H{"items": []any{}, "next_cursor": ""}})
		return
	}
	if err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	after := -1
	if cursor := c.Query("cursor"); cursor != "" {
		parts := strings.Split(cursor, ":")
		if len(parts) != 2 {
			c.JSON(400, utils.HTTPError{Code: 400, Message: "Invalid chapter cursor"})
			return
		}
		id, idErr := uuid.Parse(parts[0])
		index, indexErr := strconv.Atoi(parts[1])
		if idErr != nil || indexErr != nil || index < 0 {
			c.JSON(400, utils.HTTPError{Code: 400, Message: "Invalid chapter cursor"})
			return
		}
		if id != generation.PublicID {
			c.JSON(409, utils.HTTPError{Code: 409, Message: "Chapter generation changed; refresh chapter progress"})
			return
		}
		after = index
	}
	type requirement struct {
		ChildID      string `json:"-"`
		Stage        string `json:"stage"`
		State        string `json:"state"`
		Required     bool   `json:"required"`
		FailureClass string `json:"failure_class,omitempty"`
	}
	type row struct {
		Requirements       []requirement `json:"requirements" gorm:"-"`
		ReadyForActivation bool          `json:"ready_for_activation"`
		ReviewStatus       *string       `json:"review_status"`
		ID                 string        `json:"id"`
		GenerationID       string        `json:"generation_id"`
		UnitIndex          int           `json:"unit_index"`
		State              string        `json:"state"`
		Title              string        `json:"title"`
		StartMs            int64         `json:"start_ms"`
		EndMs              int64         `json:"end_ms"`
		ChildID            *string       `json:"child_id"`
		Visibility         *string       `json:"feed_visibility" gorm:"column:feed_visibility"`
		PlaybackURL        *string       `json:"playback_url"`
		PlaybackType       *string       `json:"playback_type"`
		FailureClass       string        `json:"failure_class"`
	}
	rows := []row{}
	q := db.Table("atomization_chapter_units u").Select("u.public_id::text AS id,u.generation_id,u.unit_index,u.state,COALESCE(c.title,g.plan->u.unit_index->>'title',u.result->>'title','') AS title,u.start_ms,u.end_ms,c.public_id::text AS child_id,c.feed_visibility,c.playback_url,c.playback_type,u.failure_class,(c.status='READY' AND c.feed_visibility IN ('embedding_pending','visible') AND c.is_feed_unit AND c.duration_sec BETWEEN 270 AND 2400) AS ready_for_activation,(SELECT ch.status FROM chapters ch WHERE ch.tenant_id=c.tenant_id AND ch.child_content_item_id=c.public_id LIMIT 1) AS review_status").Joins("JOIN atomization_generations g ON g.tenant_id=u.tenant_id AND g.public_id=u.generation_id").Joins("LEFT JOIN content_items c ON c.tenant_id=u.tenant_id AND c.public_id=u.candidate_content_item_id").Where("g.tenant_id=? AND g.parent_content_item_id=? AND g.processing_generation=? AND g.state<>'superseded'", parent.TenantID, parent.PublicID, parent.ProcessingGeneration)
	q = q.Where("g.public_id=? AND u.unit_index>?", generation.PublicID, after)
	if err := q.Order("u.unit_index ASC").Limit(limit + 1).Scan(&rows).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	next := ""
	if len(rows) > limit {
		rows = rows[:limit]
		next = generation.PublicID.String() + ":" + strconv.Itoa(rows[len(rows)-1].UnitIndex)
	}
	ids := []string{}
	for i := range rows {
		rows[i].Requirements = []requirement{}
		if rows[i].ChildID != nil {
			ids = append(ids, *rows[i].ChildID)
		}
	}
	if len(ids) > 0 {
		var requirements []requirement
		if err := db.Table("content_stage_requests r").Select("r.content_item_id::text AS child_id,r.stage,r.state,r.blocking_scope<>'optional' AS required,r.failure_class").Joins("JOIN content_items c ON c.tenant_id=r.tenant_id AND c.public_id=r.content_item_id AND c.processing_generation=r.processing_generation").Where("r.tenant_id=? AND r.content_item_id IN ?", parent.TenantID, ids).Order("r.stage ASC").Scan(&requirements).Error; err != nil {
			mediaAtomizationQueryError(c, err)
			return
		}
		byChild := map[string][]requirement{}
		for _, value := range requirements {
			byChild[value.ChildID] = append(byChild[value.ChildID], value)
		}
		for i := range rows {
			if rows[i].ChildID != nil && len(byChild[*rows[i].ChildID]) > 0 {
				rows[i].Requirements = byChild[*rows[i].ChildID]
			}
		}
	}
	c.JSON(200, utils.ResponseMessage{Code: 200, Data: gin.H{"items": rows, "next_cursor": next}})
}

func AdminGetMediaJourneyEvents(c *gin.Context) {
	db, parent, ok := journeyParent(c)
	if !ok {
		return
	}
	limit := boundedLimit(c.Query("limit"), 25, 100)
	type event struct {
		Sequence   int64     `json:"sequence"`
		Stage      string    `json:"stage"`
		EventType  string    `json:"event_type"`
		CreatedAt  time.Time `json:"created_at"`
		Generation int64     `json:"processing_generation" gorm:"column:processing_generation"`
	}
	rows := []event{}
	q := db.Table("content_stage_events e").Select("e.sequence,r.stage,e.event_type,e.occurred_at AS created_at,r.processing_generation").Joins("JOIN content_stage_requests r ON r.tenant_id=e.tenant_id AND r.public_id=e.request_id").Joins("JOIN content_items i ON i.tenant_id=r.tenant_id AND i.public_id=r.content_item_id").Where("r.tenant_id=? AND (i.public_id=? OR i.parent_content_item_id=?)", parent.TenantID, parent.PublicID, parent.PublicID)
	if raw := c.Query("cursor"); raw != "" {
		cursor, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || cursor < 1 {
			c.JSON(400, utils.HTTPError{Code: 400, Message: "Invalid event cursor"})
			return
		}
		q = q.Where("e.sequence<?", cursor)
	}
	if err := q.Order("e.sequence DESC").Limit(limit + 1).Scan(&rows).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	next := ""
	if len(rows) > limit {
		rows = rows[:limit]
		next = strconv.FormatInt(rows[len(rows)-1].Sequence, 10)
	}
	c.JSON(200, utils.ResponseMessage{Code: 200, Data: gin.H{"items": rows, "next_cursor": next}})
}
