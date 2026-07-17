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
// Not built here (Slice 4): centroid/cache owner rebuild adapters + the held
// derived-work handshake. Slice 3 covers item surfaces (content_text,
// content_image) end-to-end plus write fencing that protects a running campaign.

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
		[]string{models.EmbeddingCampaignRunning, models.EmbeddingCampaignVerifying}).
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
// else (old producer, or missing stamp) is rejected as writer_regression. No
// active campaign ⇒ never blocked (normal operation is unaffected).
func fenceEmbeddingWrite(db *gorm.DB, space, recipe, writeSpaceID, writeProducer string) (string, bool) {
	if (writeSpaceID == "") != (writeProducer == "") {
		return "space_id and producer_id must be supplied together", true
	}
	if writeSpaceID != "" && spaceid.ProducerID(writeSpaceID, recipe) != writeProducer {
		return "producer_id does not match space_id and registered recipe", true
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
		[]string{models.EmbeddingCampaignRunning, models.EmbeddingCampaignVerifying}).First(&c).Error; err == nil && writeSpaceID != c.TargetSpaceID {
		return "campaign active: write space_id does not match the migration target", true
	}
	return "", false
}

// campaignPreview computes the dry-run impact for a proposed space campaign:
// how many item rows are stale/unstamped per in-scope surface, and readiness.
type campaignPreviewResult struct {
	Space        string                   `json:"space"`
	ExpectedID   string                   `json:"expected_space_id"`
	Resolved     bool                     `json:"resolved"`
	DimOK        bool                     `json:"dim_ok"`
	Surfaces     []campaignPreviewSurface `json:"surfaces"`
	TotalTargets int64                    `json:"total_targets"`
	Blockers     []string                 `json:"blockers"`
}

type campaignPreviewSurface struct {
	Key     string `json:"key"`
	Kind    string `json:"kind"`
	Targets int64  `json:"targets"`
}

func computeCampaignPreview(db *gorm.DB, space string) campaignPreviewResult {
	es := refreshExpectedSpace(space) // fresh — this gates a mutating decision
	res := campaignPreviewResult{Space: space, ExpectedID: es.SpaceID, Resolved: es.SpaceID != ""}
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
		db.Raw(fmt.Sprintf(
			`SELECT COUNT(*) FROM %[1]s WHERE %[2]s IS NOT NULL AND (%[3]s IS DISTINCT FROM ?)`,
			s.Table, s.VecCol, s.ProducerIDCol), expectedProducer).Scan(&n)
		res.Surfaces = append(res.Surfaces, campaignPreviewSurface{Key: s.Key, Kind: s.Kind, Targets: n})
		res.TotalTargets += n
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
func startCampaign(db *gorm.DB, id uint, startedBy, reason string) (*models.EmbeddingCampaign, error) {
	var c models.EmbeddingCampaign
	if err := db.First(&c, id).Error; err != nil {
		return nil, fmt.Errorf("campaign not found")
	}
	if c.State != models.EmbeddingCampaignDraft && c.State != models.EmbeddingCampaignPaused {
		return nil, fmt.Errorf("campaign is %s, cannot start", c.State)
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
	if err := db.Save(&c).Error; err != nil {
		return nil, err
	}
	return &c, nil
}

// transitionCampaign moves a campaign to pause/resume/abort with guards.
func transitionCampaign(db *gorm.DB, id uint, action string) (*models.EmbeddingCampaign, error) {
	var c models.EmbeddingCampaign
	if err := db.First(&c, id).Error; err != nil {
		return nil, fmt.Errorf("campaign not found")
	}
	switch action {
	case "pause":
		if c.State != models.EmbeddingCampaignRunning {
			return nil, fmt.Errorf("only a running campaign can be paused")
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
	if err := db.Save(&c).Error; err != nil {
		return nil, err
	}
	return &c, nil
}

// executeCampaignBatch runs one bounded batch for a running campaign over its
// item surfaces. Claims targets by explicit IDs, calls the owner adapter, and
// verifies by readback. Returns (processed, error). Precondition-abort then
// per-action isolation (family G11).
func executeCampaignBatch(db *gorm.DB, c *models.EmbeddingCampaign) (int, error) {
	if c.State != models.EmbeddingCampaignRunning {
		return 0, nil
	}
	// Fresh identity required for mutating work.
	es := refreshExpectedSpace(c.Space)
	if es.SpaceID == "" || es.SpaceID != c.TargetSpaceID {
		c.State = models.EmbeddingCampaignBlocked
		c.BlockedReason = "expected space unavailable or diverged from target"
		db.Save(c)
		return 0, fmt.Errorf("blocked: %s", c.BlockedReason)
	}
	// Daily cap.
	batchLimit := c.ItemsPerBatch
	if c.DailyItemCap > 0 {
		var todayCount int64
		db.Model(&models.EmbeddingCampaignAction{}).
			Where("campaign_id = ? AND status = ? AND created_at > ?", c.ID, models.EmbeddingActionCompleted, time.Now().Add(-24*time.Hour)).
			Count(&todayCount)
		if todayCount >= int64(c.DailyItemCap) {
			return 0, nil // cap reached; not an error
		}
		remaining := c.DailyItemCap - int(todayCount)
		if remaining < batchLimit {
			batchLimit = remaining
		}
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
		targets := claimBatchTargets(db, s, expectedProducer, batchLimit-attempted)
		for _, tid := range targets {
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
	if remainingItemTargets(db, c, es) == 0 {
		c.State = models.EmbeddingCampaignVerifying
		db.Save(c)
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
			c.State = models.EmbeddingCampaignBlocked
			c.BlockedReason = fmt.Sprintf("retry ceiling reached for %s/%s", s.Key, targetID)
			_ = db.Save(c).Error
			return 0
		}
	}
	action := models.EmbeddingCampaignAction{
		CampaignID: c.ID, TenantID: c.TenantID, BatchID: batchID,
		SurfaceKey: s.Key, TargetID: targetID, ExpectedProducerID: expectedProducer,
		Status: models.EmbeddingActionAttempted, RetryNumber: retryNumber,
	}
	// Claim ownership: unique (campaign, surface, target, expected_producer). A
	// concurrent runner that already claimed this target hits the unique index
	// and we skip — no double call.
	if err := db.Create(&action).Error; err != nil {
		return 0 // already claimed by another runner/attempt
	}
	if observed := readbackProducer(db, s, targetID); observed == expectedProducer {
		action.Status = models.EmbeddingActionSkipped
		action.Guardrail = "staleness"
		action.ObservedProducerID = observed
		_ = db.Save(&action).Error
		db.Model(c).UpdateColumn("skipped_count", gorm.Expr("skipped_count + 1"))
		return 0
	}

	tool := toolForSurface(s)
	err := invokeItemAdapter(db, s, targetID)
	action.LatencyMS = time.Since(start).Milliseconds()
	action.Tool = tool
	if err != nil {
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
	if observed != expectedProducer {
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
func invokeItemAdapter(db *gorm.DB, s EmbeddingSurface, targetID string) error {
	var item models.ContentItem
	if err := db.Where("public_id = ?", targetID).First(&item).Error; err != nil {
		return fmt.Errorf("target not found: %w", err)
	}
	switch s.Key {
	case "content_text":
		text := buildEmbeddingText(&item)
		if text == "" {
			return fmt.Errorf("no text content to embed")
		}
		return triggerEmbedding(text, targetID)
	case "content_image":
		if item.ThumbnailURL == nil || *item.ThumbnailURL == "" {
			return fmt.Errorf("no thumbnail_url to embed")
		}
		return triggerImageEmbedding(*item.ThumbnailURL, targetID)
	default:
		return fmt.Errorf("no item adapter for surface %s", s.Key)
	}
}

// readbackProducer reads the freshly-written producer_id for a target.
func readbackProducer(db *gorm.DB, s EmbeddingSurface, targetID string) string {
	var producer *string
	db.Raw(fmt.Sprintf(`SELECT %s FROM %s WHERE %s = ?`, s.ProducerIDCol, s.Table, s.IDCol), targetID).Scan(&producer)
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
		ex = models.EmbeddingCampaignException{
			CampaignID: c.ID, TenantID: c.TenantID, SurfaceKey: s.Key, TargetID: targetID,
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
		expectedProducer := campaignExpectedProducer(c, s)
		var n int64
		db.Raw(fmt.Sprintf(
			`SELECT COUNT(*) FROM %[1]s WHERE %[2]s IS NOT NULL AND (%[3]s IS DISTINCT FROM ?)`,
			s.Table, s.VecCol, s.ProducerIDCol), expectedProducer).Scan(&n)
		total += n
	}
	return total
}
