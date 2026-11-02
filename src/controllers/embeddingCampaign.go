package controllers

import (
	"encoding/json"
	"fmt"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/spaceid"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Embedding & Model Lifecycle System (stage 10) — campaign engine (Slice 3).
//
// The campaign migrates one vector space to a frozen target identity by
// re-embedding stale/unstamped ITEM rows in bounded batches through the existing
// owner inference paths (which stamp the new identity on write-back), then
// verifying by database readback. It NEVER writes a vector itself.
//
// Derived surface rebuilds and the completion handshake live in embeddingOwnerAdapters.go.

// activeCampaignForSpace returns the non-terminal campaign for a space, if any.
func activeCampaignForSpace(db *gorm.DB, space string) (*models.EmbeddingCampaign, bool) {
	var c models.EmbeddingCampaign
	err := db.Where("space = ? AND state IN ?", space,
		[]string{models.EmbeddingCampaignDraft, models.EmbeddingCampaignRunning,
			models.EmbeddingCampaignPaused, models.EmbeddingCampaignBlocked,
			models.EmbeddingCampaignVerifying}).
		Order("created_at DESC").First(&c).Error
	if err != nil {
		return nil, false
	}
	return &c, true
}

// runningCampaignTargetProducer returns the target producer_id a write to a
// content-item surface must carry while a text/image campaign is RUNNING, plus
// whether fencing is active. Used by the write-back fence (§7).
func runningCampaignTargetProducer(db *gorm.DB, space, recipe string) (string, bool, error) {
	var c models.EmbeddingCampaign
	err := db.Where("space = ? AND state IN ?", space,
		[]string{models.EmbeddingCampaignRunning, models.EmbeddingCampaignVerifying, models.EmbeddingCampaignPaused, models.EmbeddingCampaignBlocked}).
		First(&c).Error
	if err == gorm.ErrRecordNotFound {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	for _, s := range EmbeddingSurfaces() {
		if s.Space == space && s.Recipe == recipe {
			return campaignExpectedProducer(&c, s), true, nil
		}
	}
	return "", true, fmt.Errorf("unregistered campaign recipe")
}

func campaignExpectedProducer(c *models.EmbeddingCampaign, s EmbeddingSurface) string {
	var snapshot struct {
		Producers map[string]string `json:"producers"`
	}
	if len(c.DescriptorSnapshot) > 0 && json.Unmarshal(c.DescriptorSnapshot, &snapshot) == nil {
		if producer := snapshot.Producers[s.Key]; producer != "" {
			return producer
		}
	}
	return spaceid.ProducerID(c.TargetSpaceID, s.Recipe)
}

// fenceEmbeddingWrite returns (reason, blocked). While a campaign is running or
// verifying for a space, an item write must carry the target producer; anything
// else (old producer, or missing stamp) is rejected as writer_regression.
// Outside a campaign the freshly reported service identity remains mandatory.
func fenceEmbeddingWrite(db *gorm.DB, space, recipe, writeSpaceID, writeProducer string) (string, bool) {
	if (writeSpaceID == "") != (writeProducer == "") {
		return "space_id and producer_id must be supplied together", true
	}
	if writeSpaceID != "" && spaceid.ProducerID(writeSpaceID, recipe) != writeProducer {
		return "producer_id does not match space_id and registered recipe", true
	}
	es := refreshExpectedSpace(space)
	if es.SpaceID == "" || writeSpaceID != es.SpaceID || writeProducer != es.ProducerFor(recipe) {
		return "write identity does not match a freshly resolved service identity", true
	}
	target, active, err := runningCampaignTargetProducer(db, space, recipe)
	if err != nil {
		return "cannot verify campaign write fence", true
	}
	if !active {
		return "", false
	}
	if writeProducer == "" {
		return "campaign active for space " + space + ": write must carry a vector-space producer_id", true
	}
	if writeProducer != target {
		return "campaign active: write producer_id does not match the migration target (stale writer)", true
	}
	var c models.EmbeddingCampaign
	if err := db.Where("space = ? AND state IN ?", space,
		[]string{models.EmbeddingCampaignRunning, models.EmbeddingCampaignVerifying, models.EmbeddingCampaignPaused, models.EmbeddingCampaignBlocked}).First(&c).Error; err == nil && writeSpaceID != c.TargetSpaceID {
		return "campaign active: write space_id does not match the migration target", true
	}
	return "", false
}

// campaignPreview computes the dry-run impact for a proposed space campaign:
// how many item rows are stale/unstamped per in-scope surface, and readiness.
type campaignPreviewResult struct {
	Space                   string                   `json:"space"`
	TargetModel             string                   `json:"target_model"`
	TargetRevision          string                   `json:"target_revision"`
	ExpectedID              string                   `json:"expected_space_id"`
	Resolved                bool                     `json:"resolved"`
	DimOK                   bool                     `json:"dim_ok"`
	Surfaces                []campaignPreviewSurface `json:"surfaces"`
	TotalTargets            int64                    `json:"total_targets"`
	EstimatedInferenceCalls int64                    `json:"estimated_inference_calls"`
	Blockers                []string                 `json:"blockers"`
}

type campaignPreviewSurface struct {
	Key     string `json:"key"`
	Kind    string `json:"kind"`
	Targets int64  `json:"targets"`
}

func computeCampaignPreview(db *gorm.DB, space string) campaignPreviewResult {
	es := refreshExpectedSpace(space) // fresh — this gates a mutating decision
	res := campaignPreviewResult{Space: space, ExpectedID: es.SpaceID, TargetModel: es.Model, TargetRevision: es.Revision, Resolved: es.SpaceID != "", Surfaces: []campaignPreviewSurface{}, Blockers: []string{}}
	if es.SpaceID == "" {
		res.Blockers = append(res.Blockers, "expected space unresolved: "+es.Err)
		return res
	}
	res.DimOK = true
	for _, s := range EmbeddingSurfaces() {
		if s.Space != space {
			continue
		}
		if es.Dimensions > 0 && es.Dimensions != s.Dim {
			res.DimOK = false
			res.Blockers = append(res.Blockers, fmt.Sprintf("%s dim change: service %d vs column %d", s.Key, es.Dimensions, s.Dim))
			continue
		}
		expectedProducer := es.ProducerFor(s.Recipe)
		var n int64
		err := db.Raw(fmt.Sprintf(
			`SELECT COUNT(*) FROM %[1]s WHERE %[2]s IS NOT NULL AND (%[3]s IS DISTINCT FROM ? OR %[4]s IS DISTINCT FROM ?)`,
			s.Table, s.VecCol, s.ProducerIDCol, s.SpaceIDCol), expectedProducer, es.SpaceID).Scan(&n).Error
		if err != nil {
			res.Blockers = append(res.Blockers, s.Key+": inventory unavailable")
			continue
		}
		res.Surfaces = append(res.Surfaces, campaignPreviewSurface{Key: s.Key, Kind: s.Kind, Targets: n})
		res.TotalTargets += n
		if s.Key != "story_centroid" {
			res.EstimatedInferenceCalls += n
		}
	}
	return res
}

// createCampaignDraft validates and creates a draft campaign for a space. Fails
// if a non-terminal campaign already exists or the target is unresolved / dim
// mismatched (dim-change campaigns are refused, §7).
func createCampaignDraft(db *gorm.DB, space string, caps campaignCaps) (*models.EmbeddingCampaign, error) {
	if space != EmbeddingSpaceText && space != EmbeddingSpaceImage {
		return nil, fmt.Errorf("unknown space %q", space)
	}
	if _, exists := activeCampaignForSpace(db, space); exists {
		return nil, fmt.Errorf("a non-terminal campaign already exists for space %q", space)
	}
	es := refreshExpectedSpace(space)
	if es.SpaceID == "" {
		return nil, fmt.Errorf("expected space unresolved: %s", es.Err)
	}
	// Dim-change refusal.
	for _, s := range EmbeddingSurfaces() {
		if s.Space == space && es.Dimensions > 0 && es.Dimensions != s.Dim {
			return nil, fmt.Errorf("dim change required (service %d vs column %d) — use the SQL-migration playbook, not an in-place campaign", es.Dimensions, s.Dim)
		}
	}
	scope := []string{}
	producers := map[string]string{}
	for _, s := range EmbeddingSurfaces() {
		if s.Space == space {
			scope = append(scope, s.Key)
			producers[s.Key] = es.ProducerFor(s.Recipe)
		}
	}
	c := &models.EmbeddingCampaign{
		TenantID: embeddingLifecycleTenant, Space: space, State: models.EmbeddingCampaignDraft,
		TargetSpaceID: es.SpaceID, TargetModel: es.Model, TargetRevision: es.Revision,
		SurfaceScope: jsonEvidence(map[string]any{"surfaces": scope}),
		DescriptorSnapshot: jsonEvidence(map[string]any{
			"space_id": es.SpaceID, "model": es.Model, "revision": es.Revision, "producers": producers,
		}),
		ItemsPerBatch: caps.ItemsPerBatch, BatchesPerRun: caps.BatchesPerRun,
		DailyItemCap: caps.DailyItemCap, RetryCeiling: caps.RetryCeiling,
	}
	if err := db.Create(c).Error; err != nil {
		return nil, err
	}
	return c, nil
}

type campaignCaps struct {
	ItemsPerBatch, BatchesPerRun, DailyItemCap, RetryCeiling int
}

// startCampaign freezes the target and transitions draft/paused → running after
// a fresh preflight (target still resolved + matches the frozen snapshot).
func startCampaign(db *gorm.DB, id uint, startedBy, reason string, confirmedTargets ...string) (*models.EmbeddingCampaign, error) {
	var c models.EmbeddingCampaign
	if err := db.First(&c, id).Error; err != nil {
		return nil, fmt.Errorf("campaign not found")
	}
	if c.State != models.EmbeddingCampaignDraft && c.State != models.EmbeddingCampaignPaused {
		return nil, fmt.Errorf("campaign is %s, cannot start", c.State)
	}
	if len(confirmedTargets) > 0 && confirmedTargets[0] != c.TargetSpaceID {
		return nil, fmt.Errorf("confirmed target does not match campaign")
	}
	if err := db.Exec("SELECT pg_advisory_xact_lock(hashtext(?))", "embedding-write:"+c.Space).Error; err != nil {
		return nil, err
	}
	preview := computeCampaignPreview(db, c.Space)
	if len(preview.Blockers) > 0 {
		return nil, fmt.Errorf("campaign preflight blocked: %v", preview.Blockers)
	}
	// Preflight: the service's current identity must still equal the frozen target.
	es := refreshExpectedSpace(c.Space)
	if es.SpaceID == "" {
		return nil, fmt.Errorf("preflight failed: expected space unresolved (%s)", es.Err)
	}
	if es.SpaceID != c.TargetSpaceID {
		return nil, fmt.Errorf("preflight failed: service now reports space %s but campaign target is %s — recreate the campaign against the current target", es.SpaceID, c.TargetSpaceID)
	}
	for _, s := range EmbeddingSurfaces() {
		if s.Space == c.Space && campaignExpectedProducer(&c, s) != es.ProducerFor(s.Recipe) {
			return nil, fmt.Errorf("preflight failed: producer recipe changed for %s — recreate the campaign", s.Key)
		}
	}
	now := time.Now()
	c.State = models.EmbeddingCampaignRunning
	c.StartedBy = startedBy
	c.ApprovalReason = reason
	c.StartedAt = &now
	c.BlockedReason = ""
	result := db.Model(&models.EmbeddingCampaign{}).Where("id=? AND state IN ?", c.ID, []string{models.EmbeddingCampaignDraft, models.EmbeddingCampaignPaused}).Updates(map[string]any{"state": c.State, "started_by": c.StartedBy, "approval_reason": c.ApprovalReason, "started_at": c.StartedAt, "blocked_reason": ""})
	if result.Error != nil {
		return nil, result.Error
	}
	if result.RowsAffected != 1 {
		return nil, fmt.Errorf("campaign state changed; reload before starting")
	}
	return &c, nil
}

// transitionCampaign moves a campaign to pause/resume/abort with guards.
func transitionCampaign(db *gorm.DB, id uint, action string) (*models.EmbeddingCampaign, error) {
	var c models.EmbeddingCampaign
	if err := db.First(&c, id).Error; err != nil {
		return nil, fmt.Errorf("campaign not found")
	}
	previousState := c.State
	switch action {
	case "pause":
		if c.State != models.EmbeddingCampaignRunning && c.State != models.EmbeddingCampaignVerifying {
			return nil, fmt.Errorf("only an active campaign can be paused")
		}
		c.State = models.EmbeddingCampaignPaused
	case "resume":
		if c.State != models.EmbeddingCampaignPaused && c.State != models.EmbeddingCampaignBlocked {
			return nil, fmt.Errorf("only a paused or blocked campaign can be resumed")
		}
		es := refreshExpectedSpace(c.Space)
		if es.SpaceID == "" || es.SpaceID != c.TargetSpaceID {
			return nil, fmt.Errorf("resume preflight failed: expected space unavailable or changed")
		}
		for _, s := range EmbeddingSurfaces() {
			if s.Space == c.Space && campaignExpectedProducer(&c, s) != es.ProducerFor(s.Recipe) {
				return nil, fmt.Errorf("resume preflight failed: producer recipe changed for %s", s.Key)
			}
		}
		c.State = models.EmbeddingCampaignRunning
		c.BlockedReason = ""
	case "abort":
		if c.IsTerminal() {
			return nil, fmt.Errorf("campaign already terminal")
		}
		now := time.Now()
		c.State = models.EmbeddingCampaignAborted
		c.CompletedAt = &now
	default:
		return nil, fmt.Errorf("unknown action %q", action)
	}
	r := db.Model(&models.EmbeddingCampaign{}).Where("id=? AND state=?", c.ID, previousState).Updates(map[string]any{"state": c.State, "blocked_reason": c.BlockedReason, "completed_at": c.CompletedAt})
	if r.Error != nil {
		return nil, r.Error
	}
	if r.RowsAffected != 1 {
		return nil, fmt.Errorf("campaign state changed; reload before retrying")
	}
	return &c, nil
}

// executeCampaignBatch runs one bounded batch for a running campaign over its
// item surfaces. Claims targets by explicit IDs, calls the owner adapter, and
// verifies by readback. Returns (processed, error). Precondition-abort then
// per-action isolation (family G11).
func executeCampaignBatch(db *gorm.DB, c *models.EmbeddingCampaign) (int, error) {
	if !campaignCanContinue(db, c) || c.State != models.EmbeddingCampaignRunning {
		return 0, nil
	}
	// Fresh identity required for mutating work.
	es := refreshExpectedSpace(c.Space)
	if es.SpaceID == "" || es.SpaceID != c.TargetSpaceID {
		campaignSetState(db, c, models.EmbeddingCampaignBlocked, "expected space unavailable or diverged from target")
		return 0, fmt.Errorf("blocked: %s", c.BlockedReason)
	}
	batchLimit, err := campaignBudgetRemaining(db, c)
	if err != nil {
		return 0, err
	}
	if batchLimit <= 0 {
		return 0, nil
	}

	batchID := uuid.NewString()
	processed := 0
	attempted := 0
	for _, s := range EmbeddingSurfaces() {
		if s.Space != c.Space || s.Kind != SurfaceKindItem {
			continue
		}
		expectedProducer := campaignExpectedProducer(c, s)
		targets := campaignBatchTargets(db, c, s, expectedProducer, batchLimit-attempted)
		for _, tid := range targets {
			if !campaignCanContinue(db, c) {
				return processed, nil
			}
			processed += runCampaignTarget(db, c, s, tid, expectedProducer, batchID)
			attempted++
			if attempted >= batchLimit || c.State == models.EmbeddingCampaignBlocked {
				break
			}
		}
		if attempted >= batchLimit || c.State == models.EmbeddingCampaignBlocked {
			break
		}
	}

	// If no item targets remain across scope, move to verifying (Slice 4 drives
	// the centroid handshake before completed).
	remaining := remainingItemTargets(db, c, es)
	if remaining < 0 {
		return processed, fmt.Errorf("item inventory unavailable")
	}
	if remaining == 0 {
		campaignSetState(db, c, models.EmbeddingCampaignVerifying, "")
	}
	return processed, nil
}

// claimBatchTargets selects up to n stale/unstamped target IDs for a surface,
// visible/active first. Public IDs for content surfaces.
func claimBatchTargets(db *gorm.DB, s EmbeddingSurface, expectedProducer string, n int) []string {
	var ids []string
	// Priority: READY/visible content first. For content surfaces order by
	// status so feed-visible rows migrate first.
	order := "created_at DESC"
	if s.Table == "content_items" {
		order = "(CASE WHEN status = 'READY' THEN 0 ELSE 1 END), created_at DESC"
	}
	db.Raw(fmt.Sprintf(
		`SELECT %[1]s::text FROM %[2]s
		 WHERE %[3]s IS NOT NULL AND (%[4]s IS DISTINCT FROM ?)
		 ORDER BY %[5]s LIMIT ?`,
		s.IDCol, s.Table, s.VecCol, s.ProducerIDCol, order), expectedProducer, n).Scan(&ids)
	return ids
}

func campaignBatchTargets(db *gorm.DB, c *models.EmbeddingCampaign, s EmbeddingSurface, producer string, limit int) []string {
	var ids []string
	q := fmt.Sprintf(`SELECT x.%s::text FROM %s x WHERE x.%s IS NOT NULL AND (x.%s IS DISTINCT FROM ? OR x.%s IS DISTINCT FROM ?)
      AND NOT EXISTS (SELECT 1 FROM embedding_campaign_exceptions e WHERE e.campaign_id=? AND e.surface_key=? AND e.target_id=x.%s::text AND e.status='waived' AND e.waiver_expires>now())
      ORDER BY CASE WHEN status='READY' THEN 0 ELSE 1 END, created_at DESC LIMIT ?`, s.IDCol, s.Table, s.VecCol, s.ProducerIDCol, s.SpaceIDCol, s.IDCol)
	db.Raw(q, producer, c.TargetSpaceID, c.ID, s.Key, limit).Scan(&ids)
	return ids
}

// runCampaignTarget claims one target (unique ownership), calls the adapter, and
// verifies by readback. Returns 1 if the target reached completed, else 0.
func runCampaignTarget(db *gorm.DB, c *models.EmbeddingCampaign, s EmbeddingSurface, targetID, expectedProducer, batchID string) int {
	start := time.Now()
	retryNumber := 0
	var existing models.EmbeddingCampaignException
	if err := db.Where("campaign_id = ? AND surface_key = ? AND target_id = ?", c.ID, s.Key, targetID).First(&existing).Error; err == nil {
		if existing.Status == models.EmbeddingExceptionWaived {
			if existing.WaiverExpires != nil && existing.WaiverExpires.After(time.Now()) {
				return 0
			}
			existing.Status = models.EmbeddingExceptionOpen
			_ = db.Save(&existing).Error
		}
		retryNumber = existing.Attempts
		if existing.Attempts >= c.RetryCeiling && existing.Status != models.EmbeddingExceptionRetrying {
			campaignSetState(db, c, models.EmbeddingCampaignBlocked, fmt.Sprintf("retry ceiling reached for %s/%s", s.Key, targetID))
			return 0
		}
	}
	var latest models.EmbeddingCampaignAction
	if err := db.Where("campaign_id=? AND surface_key=? AND target_id=? AND expected_producer_id=?", c.ID, s.Key, targetID, expectedProducer).Order("retry_number DESC").First(&latest).Error; err == nil {
		if latest.Status == models.EmbeddingActionAttempted {
			return 0
		}
		retryNumber = latest.RetryNumber + 1
	} else if err != gorm.ErrRecordNotFound {
		return 0
	}
	var target models.ContentItem
	if db.Where("public_id=?", targetID).First(&target).Error != nil {
		return 0
	}
	fingerprint, inputErr := campaignInputFingerprint(db, &target, s.Key)
	class := "text_embedding"
	if s.Space == EmbeddingSpaceImage {
		class = "image_embedding"
	}
	action := models.EmbeddingCampaignAction{
		CampaignID: c.ID, TenantID: target.TenantID, BatchID: batchID,
		SurfaceKey: s.Key, TargetID: targetID, ExpectedProducerID: expectedProducer,
		Status: models.EmbeddingActionAttempted, RetryNumber: retryNumber, Tool: toolForSurface(s),
		Evidence: jsonEvidence(map[string]any{"input_fingerprint": fingerprint}),
	}
	// Claim ownership: unique (campaign, surface, target, expected_producer). A
	// concurrent runner that already claimed this target hits the unique index
	// and we skip — no double call.
	if err := claimCampaignAction(db, c, &action); err != nil {
		return 0 // already claimed by another runner/attempt
	}
	if inputErr != nil {
		action.Status, action.Reason = models.EmbeddingActionFailed, inputErr.Error()
		recordAdapterFailure(db, c, s, targetID, inputErr.Error())
		_ = db.Save(&action).Error
		return 0
	}
	lease, capacityErr := acquireControllerWorkload(db, target.TenantID, "embedding_campaign", class, batchID, "campaign")
	if capacityErr != nil {
		action.Status, action.Guardrail, action.Reason = models.EmbeddingActionSkipped, "capacity", capacityErr.Error()
		_ = db.Save(&action).Error
		return 0
	}
	defer lease.cancelBeforeEffect()
	callContext := withWorkloadContext(db, spendContext(target.TenantID, "embedding_campaign", fmt.Sprint(c.ID)), "campaign")
	callContext.workloadLease = lease
	if observed := readbackProducer(db, s, targetID); ownerReadback(db, s, targetID, c.TargetSpaceID, expectedProducer) == nil {
		action.Status = models.EmbeddingActionSkipped
		action.Guardrail = "staleness"
		action.ObservedProducerID = observed
		_ = db.Save(&action).Error
		db.Model(c).UpdateColumn("skipped_count", gorm.Expr("skipped_count + 1"))
		return 0
	}

	tool := toolForSurface(s)
	callContext.TenantID = target.TenantID
	callContext.embeddingCampaign = &embeddingCampaignCorrelation{CampaignID: c.ID, ActionID: action.ID, RequestID: action.RequestID}
	err := invokeItemAdapter(db, s, targetID, callContext)
	action.LatencyMS = time.Since(start).Milliseconds()
	action.Tool = tool
	if err != nil {
		if isWorkloadDeferred(err) {
			// No model effect began; keep the reasoned skip and retry next tick.
			action.Status, action.Guardrail, action.Reason = models.EmbeddingActionSkipped, "capacity", err.Error()
			_ = db.Save(&action).Error
			return 0
		}
		recordAdapterFailure(db, c, s, targetID, err.Error())
		action.Status = models.EmbeddingActionFailed
		action.Reason = err.Error()
		db.Save(&action)
		return 0
	}

	// Readback: HTTP 2xx from the service is not success — the row must now carry
	// the target producer.
	observed := readbackProducer(db, s, targetID)
	action.ObservedProducerID = observed
	if ownerReadback(db, s, targetID, c.TargetSpaceID, expectedProducer) != nil {
		recordAdapterFailure(db, c, s, targetID, "readback mismatch: "+observed)
		action.Status = models.EmbeddingActionFailed
		action.Reason = "readback producer mismatch"
		db.Save(&action)
		return 0
	}
	action.Status = models.EmbeddingActionCompleted
	db.Save(&action)
	db.Model(&models.EmbeddingCampaignException{}).
		Where("campaign_id = ? AND surface_key = ? AND target_id = ?", c.ID, s.Key, targetID).
		Updates(map[string]interface{}{"status": models.EmbeddingExceptionResolved})
	db.Model(c).UpdateColumn("completed_count", gorm.Expr("completed_count + 1"))
	return 1
}

func toolForSurface(s EmbeddingSurface) string {
	if s.Space == EmbeddingSpaceImage {
		return models.EmbeddingToolImageEmbedding
	}
	return models.EmbeddingToolTextEmbedding
}

// invokeItemAdapter re-embeds one item through the owner inference path, which
// stamps the CURRENT service identity on write-back. This is the narrow
// force-recompute adapter — it does NOT reuse the skip-on-present trigger path.
func invokeItemAdapter(db *gorm.DB, s EmbeddingSurface, targetID string, contexts ...AISpendContext) error {
	var item models.ContentItem
	if err := db.Where("public_id = ?", targetID).First(&item).Error; err != nil {
		return fmt.Errorf("target not found: %w", err)
	}
	switch s.Key {
	case "content_text":
		text, err := buildLifecycleEmbeddingText(db, &item)
		if err != nil {
			return err
		}
		if text == "" {
			return fmt.Errorf("no text content to embed")
		}
		return triggerEmbedding(text, targetID, contexts...)
	case "content_image":
		if item.ThumbnailURL == nil || *item.ThumbnailURL == "" {
			return fmt.Errorf("no thumbnail_url to embed")
		}
		return triggerImageEmbedding(*item.ThumbnailURL, targetID, contexts...)
	default:
		return fmt.Errorf("no item adapter for surface %s", s.Key)
	}
}

// readbackProducer reads the freshly-written producer_id for a target.
func readbackProducer(db *gorm.DB, s EmbeddingSurface, targetID string) string {
	var producer *string
	db.Raw(fmt.Sprintf(`SELECT %s FROM %s WHERE %s = ? AND %s IS NOT NULL AND %s IS NOT NULL`, s.ProducerIDCol, s.Table, s.IDCol, s.VecCol, s.SpaceIDCol), targetID).Scan(&producer)
	if producer == nil {
		return ""
	}
	return *producer
}

// recordAdapterFailure upserts an exception, incrementing attempts; crossing the
// retry ceiling keeps the target as a durable blocking exception (never silently
// skipped).
func recordAdapterFailure(db *gorm.DB, c *models.EmbeddingCampaign, s EmbeddingSurface, targetID, msg string) {
	var ex models.EmbeddingCampaignException
	err := db.Where("campaign_id = ? AND surface_key = ? AND target_id = ?", c.ID, s.Key, targetID).First(&ex).Error
	if err == gorm.ErrRecordNotFound {
		tenant := c.TenantID
		var action models.EmbeddingCampaignAction
		if db.Where("campaign_id=? AND surface_key=? AND target_id=?", c.ID, s.Key, targetID).Order("id DESC").First(&action).Error == nil {
			tenant = action.TenantID
		}
		ex = models.EmbeddingCampaignException{
			CampaignID: c.ID, TenantID: tenant, SurfaceKey: s.Key, TargetID: targetID,
			Attempts: 1, Status: models.EmbeddingExceptionOpen, FailureClass: "adapter_error",
			LatestEvidence: jsonEvidence(map[string]any{"error": msg}),
		}
		db.Create(&ex)
		db.Model(c).UpdateColumn("failed_count", gorm.Expr("failed_count + 1"))
		return
	}
	if err != nil {
		return
	}
	ex.Attempts++
	ex.Status = models.EmbeddingExceptionOpen
	ex.LatestEvidence = jsonEvidence(map[string]any{"error": msg})
	db.Save(&ex)
}

// remainingItemTargets counts stale/unstamped item rows still in scope.
func remainingItemTargets(db *gorm.DB, c *models.EmbeddingCampaign, es expectedSpace) int64 {
	var total int64
	for _, s := range EmbeddingSurfaces() {
		if s.Space != c.Space || s.Kind != SurfaceKindItem {
			continue
		}
		var n int64
		err := db.Raw(fmt.Sprintf(`SELECT COUNT(*) FROM %s x WHERE x.%s IS NOT NULL AND (x.%s IS DISTINCT FROM ? OR x.%s IS DISTINCT FROM ?) AND NOT EXISTS
          (SELECT 1 FROM embedding_campaign_exceptions e WHERE e.campaign_id=? AND e.surface_key=? AND e.target_id=x.%s::text AND e.status='waived' AND e.waiver_expires>now())`, s.Table, s.VecCol, s.ProducerIDCol, s.SpaceIDCol, s.IDCol), campaignExpectedProducer(c, s), c.TargetSpaceID, c.ID, s.Key).Scan(&n).Error
		if err != nil {
			return -1
		}
		total += n
	}
	return total
}
