package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/supply"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type mediaCirculationCockpitHealth struct {
	Headline    string   `json:"headline"`
	Score       int      `json:"score"`
	Summary     string   `json:"summary"`
	Reasons     []string `json:"reasons"`
	GeneratedAt string   `json:"generated_at"`
	Enabled     bool     `json:"enabled"`
}

type mediaCirculationCockpitBucket struct {
	Bucket        string  `json:"bucket"`
	VisibleUnits  int64   `json:"visible_units"`
	State         string  `json:"state"`
	ThinFloor     int64   `json:"thin_floor"`
	SaturatedCeil int64   `json:"saturated_ceil"`
	SharePct      float64 `json:"share_pct"`

	// Demand surface (stage 4): measured demand vs value-weighted coverage.
	DemandScore   float64 `json:"demand_score"`
	CoverageScore float64 `json:"coverage_score"`
	Gap           float64 `json:"gap"`
	Measured      bool    `json:"measured"`
}

type mediaCirculationCockpitSummary struct {
	Total          int64            `json:"total"`
	Pending        int64            `json:"pending"`
	Applied        int64            `json:"applied"`
	Dismissed      int64            `json:"dismissed"`
	Superseded     int64            `json:"superseded"`
	ByUnitType     map[string]int64 `json:"by_unit_type"`
	ByVerdict      map[string]int64 `json:"by_verdict"`
	ByStatus       map[string]int64 `json:"by_status"`
	ByActionLane   map[string]int64 `json:"by_action_lane"`
	NeedsAttention int64            `json:"needs_attention"`
}

type mediaCirculationCockpitRecommendation struct {
	models.MediaCirculationRecommendation
	DisplayTitle    string   `json:"display_title"`
	DisplaySubtitle string   `json:"display_subtitle"`
	ActionLane      string   `json:"action_lane"`
	PriorityLabel   string   `json:"priority_label"`
	PrimaryMetric   string   `json:"primary_metric"`
	ProofPoints     []string `json:"proof_points"`
}

type mediaCirculationCockpitResponse struct {
	Health               mediaCirculationCockpitHealth           `json:"health"`
	Storage              storageProofMetrics                     `json:"storage"`
	OpBudget             opBudgetStatus                          `json:"op_budget"`
	AtomizationBacklog   mediaCircAtomizationBacklog             `json:"atomization_backlog"`
	AppliedYieldByBucket map[string]mediaCircBucketYield         `json:"applied_yield_by_bucket"`
	Buckets              []mediaCirculationCockpitBucket         `json:"buckets"`
	Summary              mediaCirculationCockpitSummary          `json:"summary"`
	Policy               models.MediaCirculationPolicy           `json:"policy"`
	Recommendations      []mediaCirculationCockpitRecommendation `json:"recommendations"`
	Autopilot            mediaAutopilotStatusBlock               `json:"autopilot"`
	Delivery             mediaCirculationDeliveryProof           `json:"delivery"`
	Schedules            mediaCirculationSourceScheduleProof     `json:"schedules"`
}

const (
	mediaCirculationDeliveryRequestLimit = 24
	// One ingest event plus at most 24 bounded Pods observations per request.
	// Keep the ledger query bounded while preserving the newest event for every
	// displayed request, even when a recent run consumes its full retry budget.
	mediaCirculationDeliveryEventLimit  = mediaCirculationDeliveryRequestLimit * 25
	mediaCirculationSourceScheduleLimit = 24
)

// mediaCirculationDeliveryProof is an evidence-only view of the current media
// source-run delivery boundary. It never reads queues, retries work, or turns
// an absent observation into a retry permission.
type mediaCirculationDeliveryProof struct {
	GeneratedAt    time.Time                           `json:"generated_at"`
	Verified       int                                 `json:"verified"`
	Pending        int                                 `json:"pending"`
	Degraded       int                                 `json:"degraded"`
	NotObserved    int                                 `json:"not_observed"`
	LastVerifiedAt *time.Time                          `json:"last_verified_at,omitempty"`
	Items          []mediaCirculationDeliveryProofItem `json:"items"`
}

type mediaCirculationDeliveryProofItem struct {
	RequestID           uuid.UUID  `json:"request_id"`
	SourceID            uuid.UUID  `json:"source_id"`
	SourceName          string     `json:"source_name"`
	RequestState        string     `json:"request_state"`
	TerminalOutcome     string     `json:"terminal_outcome,omitempty"`
	EvidenceState       string     `json:"evidence_state"`
	DeliveryState       string     `json:"delivery_state"`
	Reason              string     `json:"reason"`
	IngestVerdict       string     `json:"ingest_verdict,omitempty"`
	PodsVerdict         string     `json:"pods_verdict,omitempty"`
	ObservationAttempts int        `json:"observation_attempts"`
	RequestedAt         time.Time  `json:"requested_at"`
	IngestObservedAt    *time.Time `json:"ingest_observed_at,omitempty"`
	PodsObservedAt      *time.Time `json:"pods_observed_at,omitempty"`
	NextObservationAt   *time.Time `json:"next_observation_at,omitempty"`
}

// mediaCirculationSourceScheduleProof shows source-scheduling facts before a
// source run exists. It deliberately keeps "due but unadmitted" distinct from
// failure: the scheduler may still create a request on its next bounded pass.
type mediaCirculationSourceScheduleProof struct {
	GeneratedAt       time.Time                                 `json:"generated_at"`
	Available         bool                                      `json:"available"`
	UnavailableReason string                                    `json:"unavailable_reason,omitempty"`
	DueUnadmitted     int                                       `json:"due_unadmitted"`
	InFlight          int                                       `json:"in_flight"`
	Scheduled         int                                       `json:"scheduled"`
	Paused            int                                       `json:"paused"`
	Unknown           int                                       `json:"unknown"`
	Items             []mediaCirculationSourceScheduleProofItem `json:"items"`
}

type mediaCirculationSourceScheduleProofItem struct {
	SourceID               uuid.UUID  `json:"source_id"`
	SourceName             string     `json:"source_name"`
	ScheduleState          string     `json:"schedule_state"`
	Reason                 string     `json:"reason"`
	NextDueAt              *time.Time `json:"next_due_at,omitempty"`
	LastClaimedAt          *time.Time `json:"last_claimed_at,omitempty"`
	LastAttemptedAt        *time.Time `json:"last_attempted_at,omitempty"`
	LastProviderSuccessAt  *time.Time `json:"last_provider_success_at,omitempty"`
	LastUpstreamObservedAt *time.Time `json:"last_upstream_observed_at,omitempty"`
	LastNoChangeAt         *time.Time `json:"last_no_change_at,omitempty"`
	LastNewItemAt          *time.Time `json:"last_new_item_at,omitempty"`
	LastDeliveryVerifiedAt *time.Time `json:"last_delivery_verified_at,omitempty"`
	IntakeCircuitUntil     *time.Time `json:"intake_circuit_until,omitempty"`
	LatestRequestID        *uuid.UUID `json:"latest_request_id,omitempty"`
	LatestRequestState     string     `json:"latest_request_state,omitempty"`
}

func GetMediaCirculationCockpit(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	health := buildMediaCirculationHealth(db, principal.TenantID, c.GetHeader("Authorization"))
	rows := loadMediaCirculationCockpitRows(db, principal.TenantID)
	resp := mediaCirculationCockpitResponse{
		Health: mediaCirculationCockpitHealth{
			Headline:    health.Headline,
			Score:       health.Score,
			Summary:     health.Summary,
			Reasons:     health.Reasons,
			GeneratedAt: health.GeneratedAt,
			Enabled:     health.Policy.Enabled,
		},
		Storage:              health.Proof.Storage,
		OpBudget:             health.Proof.OpBudget,
		AtomizationBacklog:   health.Proof.AtomizationBacklog,
		AppliedYieldByBucket: health.Proof.AppliedYieldByBucket,
		Buckets:              cockpitBuckets(health.Proof.Buckets),
		Policy:               health.Policy,
		Recommendations:      rows,
		Autopilot:            buildMediaAutopilotStatus(db, principal.TenantID, health.Policy),
		Delivery:             loadMediaCirculationDeliveryProof(db, principal.TenantID),
		Schedules:            loadMediaCirculationSourceScheduleProof(db, principal.TenantID),
	}
	resp.Summary = summarizeCockpitRecommendations(rows)
	c.JSON(http.StatusOK, resp)
}

func loadMediaCirculationSourceScheduleProof(db *gorm.DB, tenantID string) mediaCirculationSourceScheduleProof {
	now := time.Now().UTC()
	proof := mediaCirculationSourceScheduleProof{GeneratedAt: now, Available: true, Items: []mediaCirculationSourceScheduleProofItem{}}
	var sources []models.ContentSource
	if err := db.Where("tenant_id = ? AND category = ?", tenantID, models.SourceCategoryMedia).
		Order("CASE WHEN next_due_at IS NULL THEN 1 ELSE 0 END, next_due_at ASC, public_id ASC").Limit(mediaCirculationSourceScheduleLimit).Find(&sources).Error; err != nil {
		proof.Available = false
		proof.UnavailableReason = "CMS could not read media source schedule facts."
		return proof
	}

	for _, source := range sources {
		var latest models.SourceRunRequest
		err := db.Where("tenant_id = ? AND content_source_id = ? AND lane = ?", tenantID, source.PublicID, models.SourceCategoryMedia).
			Order("requested_at DESC, public_id DESC").First(&latest).Error
		if err != nil && err != gorm.ErrRecordNotFound {
			proof.Available = false
			proof.UnavailableReason = "CMS could not read the latest source-run state."
			proof.Items = []mediaCirculationSourceScheduleProofItem{}
			return proof
		}
		hasLatest := err == nil
		state, reason := mediaSourceScheduleState(source, hasLatest, latest, now)
		item := mediaCirculationSourceScheduleProofItem{
			SourceID: source.PublicID, SourceName: source.Name, ScheduleState: state, Reason: reason,
			NextDueAt: source.NextDueAt, LastClaimedAt: source.LastClaimedAt, LastAttemptedAt: source.LastAttemptedAt,
			LastProviderSuccessAt: source.LastProviderSuccessAt, LastNewItemAt: source.LastNewItemAt,
			LastUpstreamObservedAt: source.LastUpstreamObservedAt, LastNoChangeAt: source.LastNoChangeAt,
			LastDeliveryVerifiedAt: source.LastDeliveryVerifiedAt, IntakeCircuitUntil: source.IntakeCircuitUntil,
		}
		if hasLatest {
			requestID := latest.PublicID
			item.LatestRequestID, item.LatestRequestState = &requestID, latest.State
		}
		switch state {
		case "due_unadmitted":
			proof.DueUnadmitted++
		case "in_flight":
			proof.InFlight++
		case "scheduled":
			proof.Scheduled++
		case "paused":
			proof.Paused++
		default:
			proof.Unknown++
		}
		proof.Items = append(proof.Items, item)
	}
	return proof
}

func mediaSourceScheduleState(source models.ContentSource, hasLatest bool, latest models.SourceRunRequest, now time.Time) (string, string) {
	if !source.IsActive {
		return "paused", "This source is inactive; no scheduling admission is expected."
	}
	if source.NextDueAt == nil {
		return "unknown", "CMS has no explicit next-due checkpoint for this active source."
	}
	if hasLatest && sourceRunRequestActive(latest.State) {
		return "in_flight", "A durable source run is active; CMS will verify it before treating delivery as complete."
	}
	if now.Before(*source.NextDueAt) {
		return "scheduled", "The source is not due yet under its current CMS schedule."
	}
	return "due_unadmitted", "The source is due and has no active CMS source run; this is an admission fact, not a provider failure."
}

func sourceRunRequestActive(state string) bool {
	switch state {
	case models.SourceRunRequested, models.SourceRunAccepted, models.SourceRunRunning, models.SourceRunVerificationRequired:
		return true
	default:
		return false
	}
}

func loadMediaCirculationDeliveryProof(db *gorm.DB, tenantID string) mediaCirculationDeliveryProof {
	proof := mediaCirculationDeliveryProof{GeneratedAt: time.Now().UTC(), Items: []mediaCirculationDeliveryProofItem{}}
	var requests []models.SourceRunRequest
	if err := db.Where("tenant_id = ? AND lane = ?", tenantID, models.SourceCategoryMedia).
		Order("requested_at DESC, public_id DESC").Limit(mediaCirculationDeliveryRequestLimit).Find(&requests).Error; err != nil || len(requests) == 0 {
		return proof
	}
	requestIDs := make([]uuid.UUID, 0, len(requests))
	sourceIDs := make([]uuid.UUID, 0, len(requests))
	for _, request := range requests {
		requestIDs = append(requestIDs, request.PublicID)
		sourceIDs = append(sourceIDs, request.ContentSourceID)
	}

	sources := map[uuid.UUID]models.ContentSource{}
	var sourceRows []models.ContentSource
	if err := db.Where("tenant_id = ? AND public_id IN ?", tenantID, sourceIDs).Find(&sourceRows).Error; err == nil {
		for _, source := range sourceRows {
			sources[source.PublicID] = source
		}
	}

	var tasks []models.SourceRunVerificationTask
	_ = db.Where("tenant_id = ? AND source_run_request_id IN ? AND causation_id LIKE ?", tenantID, requestIDs, "consumer_pods_delivery:%").
		Order("created_at DESC, public_id DESC").Find(&tasks).Error
	podsTasks := map[uuid.UUID]models.SourceRunVerificationTask{}
	for _, task := range tasks {
		if _, exists := podsTasks[task.SourceRunRequestID]; !exists {
			podsTasks[task.SourceRunRequestID] = task
		}
	}
	var coordinatorUnits []models.SourceRunExecutionUnit
	_ = db.Where("tenant_id=? AND source_run_request_id IN ? AND unit_type=?", tenantID, requestIDs, "coordinator").Find(&coordinatorUnits).Error
	terminalOutcomes := map[uuid.UUID]string{}
	for _, unit := range coordinatorUnits {
		terminalOutcomes[unit.SourceRunRequestID] = unit.TerminalOutcome
	}

	var events []models.SourceRunReconciliationEvent
	_ = db.Where("tenant_id = ? AND source_run_request_id IN ? AND (causation_id = ? OR causation_id LIKE ?)", tenantID, requestIDs, "consumer_delivery", "consumer_pods_delivery:%").
		Order("observed_at DESC, public_id DESC").Limit(mediaCirculationDeliveryEventLimit).Find(&events).Error
	ingestEvents := map[uuid.UUID]models.SourceRunReconciliationEvent{}
	podsEvents := map[uuid.UUID]models.SourceRunReconciliationEvent{}
	for _, event := range events {
		if event.CausationID == "consumer_delivery" {
			if _, exists := ingestEvents[event.SourceRunRequestID]; !exists {
				ingestEvents[event.SourceRunRequestID] = event
			}
			continue
		}
		if strings.HasPrefix(event.CausationID, "consumer_pods_delivery:") {
			if _, exists := podsEvents[event.SourceRunRequestID]; !exists {
				podsEvents[event.SourceRunRequestID] = event
			}
		}
	}

	for _, request := range requests {
		source := sources[request.ContentSourceID]
		ingest, hasIngest := ingestEvents[request.PublicID]
		pods, hasPods := podsEvents[request.PublicID]
		task, hasTask := podsTasks[request.PublicID]
		item := mediaCirculationDeliveryProofItem{
			RequestID: request.PublicID, SourceID: request.ContentSourceID, SourceName: source.Name,
			RequestState: request.State, TerminalOutcome: terminalOutcomes[request.PublicID], EvidenceState: request.EvidenceState, RequestedAt: request.RequestedAt,
		}
		if hasIngest {
			item.IngestVerdict, item.IngestObservedAt = ingest.Verdict, &ingest.ObservedAt
		}
		if hasPods {
			item.PodsVerdict, item.PodsObservedAt = pods.Verdict, &pods.ObservedAt
		}
		if hasTask {
			item.ObservationAttempts, item.NextObservationAt = task.AttemptCount, task.NotBeforeAt
		}
		item.DeliveryState, item.Reason = mediaDeliveryState(request, item.TerminalOutcome, hasIngest, ingest, hasTask, task, hasPods, pods)
		switch item.DeliveryState {
		case "verified":
			proof.Verified++
			if item.PodsObservedAt != nil && (proof.LastVerifiedAt == nil || item.PodsObservedAt.After(*proof.LastVerifiedAt)) {
				observed := *item.PodsObservedAt
				proof.LastVerifiedAt = &observed
			}
		case "pending":
			proof.Pending++
		case "degraded":
			proof.Degraded++
		default:
			proof.NotObserved++
		}
		proof.Items = append(proof.Items, item)
	}
	return proof
}

func mediaDeliveryState(request models.SourceRunRequest, terminalOutcome string, hasIngest bool, ingest models.SourceRunReconciliationEvent, hasTask bool, task models.SourceRunVerificationTask, hasPods bool, pods models.SourceRunReconciliationEvent) (string, string) {
	if terminalOutcome == string(supply.OutcomeNoChange) {
		return "not_applicable", "The provider reported a verified no-change outcome; no downstream Pods item was expected."
	}
	if terminalOutcome == string(supply.OutcomeUpstreamChangeDeferred) {
		return "deferred", "Upstream identities were preserved for bounded later materialization; Pods delivery is not yet expected."
	}
	if terminalOutcome == string(supply.OutcomeObservationBlockedByIntake) || terminalOutcome == string(supply.OutcomeConfigurationBlocked) {
		return "blocked", "The source observation reached an explicit intake or configuration block and did not advance the provider cursor."
	}
	if hasPods && pods.Verdict == string(supply.VerdictPresent) {
		return "verified", "CMS confirmed every source-run media item is currently eligible for Pods, directly or through an atomized child."
	}
	if hasPods && pods.Verdict == string(supply.VerdictUnknown) && hasTask && task.State == models.SourceRunVerificationTaskTerminal {
		return "degraded", "Pods delivery evidence remained unknown after the bounded observation budget."
	}
	if hasTask && task.State != models.SourceRunVerificationTaskTerminal {
		return "pending", "CMS is waiting for the next bounded Pods delivery observation; source execution will not be replayed."
	}
	if request.State == models.SourceRunVerificationRequired || (hasIngest && ingest.Verdict == string(supply.VerdictUnknown)) {
		return "pending", "CMS ingest proof is still verification-required."
	}
	if request.State == models.SourceRunFailed || request.State == models.SourceRunPartial || request.State == models.SourceRunCancelled || request.State == models.SourceRunExpired {
		return "not_observed", "The source run did not reach a successful ingest proof."
	}
	if request.State == models.SourceRunSucceeded && hasIngest && ingest.Verdict == string(supply.VerdictPresent) {
		return "pending", "CMS ingest is verified; the read-only Pods delivery task is awaiting its first observation."
	}
	return "not_observed", "No authoritative Pods delivery observation exists yet."
}

func loadMediaCirculationCockpitRows(db *gorm.DB, tenantID string) []mediaCirculationCockpitRecommendation {
	var rows []models.MediaCirculationRecommendation
	db.Where("tenant_id = ?", tenantID).
		Order("status = 'pending' DESC, applied ASC, score DESC, updated_at DESC").
		Limit(300).
		Find(&rows)
	if len(rows) == 0 {
		return []mediaCirculationCockpitRecommendation{}
	}

	sourceIDs := []uuid.UUID{}
	itemIDs := []uuid.UUID{}
	for _, r := range rows {
		switch r.UnitType {
		case models.MediaCirculationUnitSource:
			sourceIDs = append(sourceIDs, r.SubjectID)
		case models.MediaCirculationUnitItemFamily:
			itemIDs = append(itemIDs, r.SubjectID)
		}
	}

	sources := map[uuid.UUID]models.ContentSource{}
	if len(sourceIDs) > 0 {
		var sourceRows []models.ContentSource
		db.Where("tenant_id = ? AND public_id IN ?", tenantID, sourceIDs).Find(&sourceRows)
		for _, s := range sourceRows {
			sources[s.PublicID] = s
		}
	}

	items := map[uuid.UUID]models.ContentItem{}
	if len(itemIDs) > 0 {
		var itemRows []models.ContentItem
		db.Where("tenant_id = ? AND public_id IN ?", tenantID, itemIDs).Find(&itemRows)
		for _, it := range itemRows {
			items[it.PublicID] = it
		}
	}

	out := make([]mediaCirculationCockpitRecommendation, 0, len(rows))
	for _, r := range rows {
		metrics := mediaCircMetricsMap(r)
		displayTitle, displaySubtitle := cockpitDisplayText(r, metrics, sources, items)
		proof := cockpitProofPoints(r, metrics)
		out = append(out, mediaCirculationCockpitRecommendation{
			MediaCirculationRecommendation: r,
			DisplayTitle:                   displayTitle,
			DisplaySubtitle:                displaySubtitle,
			ActionLane:                     mediaCircActionLane(r.Verdict),
			PriorityLabel:                  mediaCircPriorityLabel(r),
			PrimaryMetric:                  cockpitPrimaryMetric(r, metrics),
			ProofPoints:                    proof,
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Status == models.MediaCirculationRecStatusPending && out[j].Status != models.MediaCirculationRecStatusPending {
			return true
		}
		if out[i].Status != models.MediaCirculationRecStatusPending && out[j].Status == models.MediaCirculationRecStatusPending {
			return false
		}
		if out[i].ActionLane != out[j].ActionLane {
			return mediaCircLaneOrder(out[i].ActionLane) < mediaCircLaneOrder(out[j].ActionLane)
		}
		return out[i].Score > out[j].Score
	})
	return out
}

func cockpitBuckets(buckets []libraryBucketHealth) []mediaCirculationCockpitBucket {
	total := int64(0)
	for _, b := range buckets {
		total += b.VisibleUnits
	}
	out := make([]mediaCirculationCockpitBucket, 0, len(buckets))
	for _, b := range buckets {
		share := 0.0
		if total > 0 {
			share = float64(b.VisibleUnits) / float64(total) * 100
		}
		out = append(out, mediaCirculationCockpitBucket{
			Bucket:        b.Bucket,
			VisibleUnits:  b.VisibleUnits,
			State:         b.State,
			ThinFloor:     mediaCirculationBucketThinFloor,
			SaturatedCeil: mediaCirculationBucketSaturatedCeil,
			SharePct:      share,
			DemandScore:   b.DemandScore,
			CoverageScore: b.CoverageScore,
			Gap:           b.Gap,
			Measured:      b.Measured,
		})
	}
	return out
}

func summarizeCockpitRecommendations(rows []mediaCirculationCockpitRecommendation) mediaCirculationCockpitSummary {
	s := mediaCirculationCockpitSummary{
		Total:        int64(len(rows)),
		ByUnitType:   map[string]int64{},
		ByVerdict:    map[string]int64{},
		ByStatus:     map[string]int64{},
		ByActionLane: map[string]int64{},
	}
	for _, r := range rows {
		s.ByUnitType[r.UnitType]++
		s.ByVerdict[r.Verdict]++
		s.ByStatus[r.Status]++
		s.ByActionLane[r.ActionLane]++
		switch r.Status {
		case models.MediaCirculationRecStatusPending:
			s.Pending++
			if cockpitNeedsAttention(r) {
				s.NeedsAttention++
			}
		case models.MediaCirculationRecStatusApplied:
			s.Applied++
		case models.MediaCirculationRecStatusDismissed:
			s.Dismissed++
		case models.MediaCirculationRecStatusSuperseded:
			s.Superseded++
		}
	}
	return s
}

func mediaCircActionLane(verdict string) string {
	switch verdict {
	case mediaCircVerdictPullNow, mediaCircVerdictDeepPull:
		return "pull"
	case mediaCircVerdictAtomizeNow:
		return "atomize"
	case mediaCircVerdictPullLimited, mediaCircVerdictSkipSource, mediaCircVerdictPauseSource:
		return "limit_skip"
	case mediaCircVerdictProtect:
		return "protect"
	case mediaCircVerdictReEncode, mediaCircVerdictMoveToCold, mediaCircVerdictRecoverableDelete:
		return "cool"
	case mediaCircVerdictRankDown:
		return "downrank"
	case mediaCircVerdictNeedsAdminReview:
		return "review"
	case mediaCircVerdictBlockedTranscript, mediaCircVerdictAtomizationLeak:
		return "review"
	default:
		return "review"
	}
}

func mediaCircLaneOrder(lane string) int {
	switch lane {
	case "pull":
		return 0
	case "downrank":
		return 1
	case "atomize":
		return 2
	case "cool":
		return 3
	case "review":
		return 4
	case "limit_skip":
		return 5
	case "protect":
		return 6
	default:
		return 9
	}
}

func mediaCircPriorityLabel(rec models.MediaCirculationRecommendation) string {
	if rec.Status != models.MediaCirculationRecStatusPending {
		return strings.ReplaceAll(rec.Status, "_", " ")
	}
	switch rec.Verdict {
	case mediaCircVerdictDeepPull:
		return "High-yield pull"
	case mediaCircVerdictPullNow:
		return "Pull now"
	case mediaCircVerdictRankDown:
		return "Reduce exposure"
	case mediaCircVerdictAtomizeNow:
		return "Atomize now"
	case mediaCircVerdictBlockedTranscript:
		return "Transcript blocked"
	case mediaCircVerdictAtomizationLeak:
		return "Atomization leak"
	case mediaCircVerdictRecoverableDelete:
		return "Cost reclaim"
	case mediaCircVerdictMoveToCold:
		return "Move cold"
	case mediaCircVerdictReEncode:
		return "Optimize bytes"
	case mediaCircVerdictNeedsAdminReview:
		return "Needs review"
	case mediaCircVerdictPauseSource:
		return "Pause source"
	case mediaCircVerdictPullLimited:
		return "Limited pull"
	case mediaCircVerdictProtect:
		return "Protected"
	default:
		return strings.ReplaceAll(rec.Verdict, "_", " ")
	}
}

func cockpitNeedsAttention(r mediaCirculationCockpitRecommendation) bool {
	switch r.ActionLane {
	case "pull", "downrank", "cool", "review":
		return true
	default:
		return false
	}
}

func mediaCircMetricsMap(rec models.MediaCirculationRecommendation) map[string]interface{} {
	if len(rec.Metrics) == 0 {
		return map[string]interface{}{}
	}
	var metrics map[string]interface{}
	if err := json.Unmarshal(rec.Metrics, &metrics); err != nil || metrics == nil {
		return map[string]interface{}{}
	}
	return metrics
}

func cockpitDisplayText(rec models.MediaCirculationRecommendation, metrics map[string]interface{}, sources map[uuid.UUID]models.ContentSource, items map[uuid.UUID]models.ContentItem) (string, string) {
	if rec.UnitType == models.MediaCirculationUnitSource {
		if source, ok := sources[rec.SubjectID]; ok {
			return source.Name, fmt.Sprintf("%s source · interval %dm", source.Type, source.FetchIntervalMinutes)
		}
		if name := stringMetric(metrics, "source_name"); name != "" {
			return name, "Media source"
		}
		return "Unknown source", rec.SubjectID.String()
	}

	if item, ok := items[rec.SubjectID]; ok {
		title := "Untitled media"
		if item.Title != nil && strings.TrimSpace(*item.Title) != "" {
			title = strings.TrimSpace(*item.Title)
		}
		source := "unknown source"
		if item.SourceName != nil && strings.TrimSpace(*item.SourceName) != "" {
			source = strings.TrimSpace(*item.SourceName)
		}
		role := stringMetric(metrics, "role")
		if role != "" {
			return title, fmt.Sprintf("%s · %s", source, strings.ReplaceAll(role, "_", " "))
		}
		return title, source
	}
	return strings.ReplaceAll(rec.SubjectKind, "_", " "), rec.SubjectID.String()
}

func cockpitPrimaryMetric(rec models.MediaCirculationRecommendation, metrics map[string]interface{}) string {
	switch rec.UnitType {
	case models.MediaCirculationUnitSource:
		if allowed := intMetric(metrics, "allowed_intake"); allowed > 0 {
			return fmt.Sprintf("%d allowed", allowed)
		}
		if failure := floatMetric(metrics, "failure_rate"); failure > 0 {
			return fmt.Sprintf("%.0f%% failed", failure*100)
		}
	case models.MediaCirculationUnitItemFamily:
		if bytes := int64Metric(metrics, "file_size_bytes"); bytes > 0 {
			return humanBytes(bytes)
		}
		if value := floatMetric(metrics, "value"); value > 0 {
			return fmt.Sprintf("value %.2f", value)
		}
		if views := int64Metric(metrics, "view_count"); views > 0 {
			return fmt.Sprintf("%d views", views)
		}
	}
	return fmt.Sprintf("score %.3f", rec.Score)
}

func cockpitProofPoints(rec models.MediaCirculationRecommendation, metrics map[string]interface{}) []string {
	points := []string{}
	if rec.Reasons != nil {
		var reasons []string
		if err := json.Unmarshal(rec.Reasons, &reasons); err == nil {
			for _, r := range reasons {
				if strings.TrimSpace(r) != "" {
					points = append(points, r)
				}
			}
		}
	}
	if qp := floatMetric(metrics, "quality_prior"); qp > 0 {
		points = append(points, fmt.Sprintf("Quality prior %.2f", qp))
	}
	if bm := floatMetric(metrics, "bucket_demand_match"); bm > 0 {
		points = append(points, fmt.Sprintf("Bucket demand %.2f", bm))
	}
	if thin := stringSliceMetric(metrics, "matched_thin_buckets"); len(thin) > 0 {
		points = append(points, "Fills thin buckets: "+strings.Join(thin, ", "))
	}
	if role := stringMetric(metrics, "role"); role != "" {
		points = append(points, "Storage role: "+strings.ReplaceAll(role, "_", " "))
	}
	if len(points) > 5 {
		return points[:5]
	}
	return points
}

func stringMetric(metrics map[string]interface{}, key string) string {
	if raw, ok := metrics[key]; ok {
		if value, ok := raw.(string); ok {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func stringSliceMetric(metrics map[string]interface{}, key string) []string {
	raw, ok := metrics[key]
	if !ok {
		return nil
	}
	values, ok := raw.([]interface{})
	if !ok {
		return nil
	}
	out := []string{}
	for _, v := range values {
		if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
			out = append(out, strings.TrimSpace(s))
		}
	}
	return out
}

func floatMetric(metrics map[string]interface{}, key string) float64 {
	if raw, ok := metrics[key]; ok {
		switch v := raw.(type) {
		case float64:
			return v
		case int:
			return float64(v)
		case int64:
			return float64(v)
		}
	}
	return 0
}

func intMetric(metrics map[string]interface{}, key string) int {
	return int(int64Metric(metrics, key))
}

func int64Metric(metrics map[string]interface{}, key string) int64 {
	if raw, ok := metrics[key]; ok {
		switch v := raw.(type) {
		case float64:
			return int64(v)
		case int:
			return int64(v)
		case int64:
			return v
		}
	}
	return 0
}

func humanBytes(bytes int64) string {
	if bytes <= 0 {
		return "0 B"
	}
	units := []string{"B", "KB", "MB", "GB", "TB"}
	value := float64(bytes)
	unit := 0
	for value >= 1024 && unit < len(units)-1 {
		value /= 1024
		unit++
	}
	if unit == 0 {
		return fmt.Sprintf("%d %s", bytes, units[unit])
	}
	return fmt.Sprintf("%.1f %s", value, units[unit])
}
