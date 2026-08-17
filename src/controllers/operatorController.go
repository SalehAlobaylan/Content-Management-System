package controllers

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"content-management-system/src/models"
	operatorpkg "content-management-system/src/operator"
	"content-management-system/src/supply"
	"content-management-system/src/utils"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// GetOperatorStatus is deliberately a small, gated capability probe. It does
// not accept plans or expose investigations; the full Operator API is mounted
// only after the durable context/action packages are ready.
func GetOperatorStatus(c *gin.Context) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"message": "Authentication required"})
		return
	}
	tenantID, err := operatorpkg.RequireExplicitTenant(principal)
	if err != nil {
		c.JSON(http.StatusForbidden, gin.H{"message": "Explicit tenant claim required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	_, runtimePolicy, err := operatorExecutionPolicy(db, tenantID)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Operator policy is invalid"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"name":        "Wahb Operator",
		"tenant_id":   tenantID,
		"operational": true,
		"controls": gin.H{
			"read_enabled":      runtimePolicy.ReadEnabled,
			"llm_enabled":       runtimePolicy.LLMEnabled,
			"execution_enabled": runtimePolicy.ExecutionEnabled,
			"schedules_enabled": runtimePolicy.SchedulesEnabled,
		},
	})
}

type operatorInvestigationRequest struct {
	VisibleContext    operatorpkg.VisibleContext `json:"visible_context"`
	Intent            operatorpkg.Intent         `json:"intent"`
	Locale            string                     `json:"locale"`
	Message           string                     `json:"message"`
	Tier              string                     `json:"tier"`
	ThreadID          string                     `json:"thread_id,omitempty"`
	SpendAcknowledged bool                       `json:"spend_acknowledged,omitempty"`
}

type operatorPlanCreateRequest struct {
	InvestigationID string   `json:"investigation_id"`
	ToolKey         string   `json:"tool_key"`
	TargetIDs       []string `json:"target_ids"`
}

type operatorPlanApprovalRequest struct {
	Confirmation string `json:"confirmation"`
}

func isOperatorSupplyRecoveryTool(key string) bool {
	for _, registered := range operatorpkg.OperatorSupplyRecoveryToolKeys() {
		if key == registered {
			return true
		}
	}
	return false
}

// ListOperatorEligibleActions is an owner-bound CMS read model for action UI.
// It never grants authority: the subsequent signed-plan preview repeats all
// current IAM, policy, freshness, target, and control checks.
func ListOperatorEligibleActions(c *gin.Context) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"message": "Authentication required"})
		return
	}
	tenantID, err := operatorpkg.RequireExplicitTenant(principal)
	if err != nil {
		c.JSON(http.StatusForbidden, gin.H{"message": "Explicit tenant claim required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	_, policy, err := operatorExecutionPolicy(db, tenantID)
	if err != nil || !policy.ReadEnabled {
		c.JSON(http.StatusOK, gin.H{"items": []gin.H{}, "execution_enabled": false})
		return
	}
	publicID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator investigation"})
		return
	}
	var investigation models.OperatorInvestigation
	if err := db.Where("public_id=? AND tenant_id=? AND actor_id=?", publicID, tenantID, principal.UserID).First(&investigation).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "Operator investigation not found"})
		return
	}
	input, err := operatorpkg.DecodeStoredInvestigationInput(investigation)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"items": []gin.H{}, "execution_enabled": policy.ExecutionEnabled})
		return
	}
	accessClient, err := operatorpkg.NewIAMAccessClientFromEnv()
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Current Operator access cannot be verified"})
		return
	}
	access, err := accessClient.Snapshot(c.Request.Context(), principal.UserID, tenantID)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Current Operator access cannot be verified"})
		return
	}
	packet, err := operatorpkg.NewContextFabric(db, operatorpkg.DefaultAdapterRegistry()).BuildPacket(c.Request.Context(), input.VisibleContext, access)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"items": []gin.H{}, "execution_enabled": policy.ExecutionEnabled})
		return
	}
	catalog := operatorpkg.DefaultToolCatalog()
	items := make([]gin.H, 0)
	toolKeys := []string{}
	if policy.ExecutionEnabled && packet.Completeness == "complete" && len(packet.Conflicts) == 0 && input.VisibleContext.Selection != nil && input.VisibleContext.Selection.Mode == "explicit" {
		toolKeys = operatorpkg.ToolKeysForDomain(input.VisibleContext.Domain)
	}
	for _, toolKey := range toolKeys {
		descriptor, found := catalog.Lookup(toolKey)
		if !found {
			// The static manifest is validated at startup/tests, but fail closed
			// if a skewed build ever returns an unknown descriptor.
			continue
		}
		if !access.HasPermission(descriptor.RequiredPermission) || operatorpkg.EnsureToolCapabilityEnabled(db, tenantID, descriptor.Key, time.Now().UTC()) != nil {
			continue
		}
		targets := make([]string, 0, len(input.VisibleContext.Selection.IDs))
		for _, targetID := range input.VisibleContext.Selection.IDs {
			if _, deriveErr := catalog.DeriveArguments(descriptor.Key, []string{targetID}); deriveErr != nil {
				continue
			}
			if isOperatorSupplyRecoveryTool(descriptor.Key) {
				episodeID, parseErr := uuid.Parse(targetID)
				if parseErr != nil {
					continue
				}
				if _, _, eligibilityErr := operatorSupplyEligibleCandidate(db, tenantID, episodeID, descriptor.Key); eligibilityErr != nil {
					continue
				}
			}
			targets = append(targets, targetID)
		}
		if len(targets) == 0 {
			continue
		}
		items = append(items, gin.H{
			"kind": "plan", "key": descriptor.Key, "localized_action_key": descriptor.LocalizedActionKey,
			"risk_tier": descriptor.RiskTier, "target_type": descriptor.TargetType,
			"argument_schema": descriptor.ArgumentSchema, "target_ids": targets,
			"affected_domains": descriptor.AffectedDomains, "cancellation": descriptor.Cancellation,
			"rollback": descriptor.Rollback, "contingencies": descriptor.Contingencies, "manual_only": false,
		})
	}
	manualLink := ""
	for _, evidence := range packet.Evidence {
		if evidence.Domain == input.VisibleContext.Domain && operatorpkg.IsInternalDeepLink(evidence.DeepLink) {
			manualLink = evidence.DeepLink
			break
		}
	}
	for _, admission := range operatorpkg.DefaultDomainActionCatalog() {
		if admission.Domain != input.VisibleContext.Domain || manualLink == "" {
			continue
		}
		for _, manualKey := range admission.ManualOnly {
			items = append(items, gin.H{
				"kind": "manual", "localized_action_key": "operator.manual." + input.VisibleContext.Domain + "." + manualKey,
				"reason_key": "operator.manual_reason." + input.VisibleContext.Domain, "affected_domain": input.VisibleContext.Domain,
				"deep_link": manualLink, "manual_only": true,
			})
		}
		break
	}
	c.JSON(http.StatusOK, gin.H{"packet_fingerprint": packet.Fingerprint, "execution_enabled": policy.ExecutionEnabled, "items": items})
}

type operatorThreadRequest struct {
	Title    string `json:"title"`
	Locale   string `json:"locale"`
	Pinned   *bool  `json:"pinned,omitempty"`
	Archived *bool  `json:"archived,omitempty"`
}

type operatorThreadMessageRequest struct {
	Text string `json:"text"`
}

type operatorCursor struct {
	Time   time.Time `json:"time"`
	ID     string    `json:"id"`
	Pinned bool      `json:"pinned,omitempty"`
	Kind   string    `json:"kind,omitempty"`
}

func encodeOperatorCursor(cursor operatorCursor) string {
	raw, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeOperatorCursor(value string) (operatorCursor, error) {
	var cursor operatorCursor
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(value))
	if err != nil || json.Unmarshal(raw, &cursor) != nil || cursor.Time.IsZero() || strings.TrimSpace(cursor.ID) == "" {
		return operatorCursor{}, fmt.Errorf("invalid Operator cursor")
	}
	return cursor, nil
}

// CreateOperatorInvestigation is the only interactive CMS entrypoint. The
// browser submits typed navigation intent, not evidence or a plan; CMS obtains
// current IAM authority and returns a durable backgrounded investigation.
// The worker obtains another snapshot before it reads context or calls a model.
func CreateOperatorInvestigation(c *gin.Context) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"message": "Authentication required"})
		return
	}
	tenantID, err := operatorpkg.RequireExplicitTenant(principal)
	if err != nil {
		c.JSON(http.StatusForbidden, gin.H{"message": "Explicit tenant claim required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var persisted models.OperatorPolicy
	_ = db.Where("tenant_id=?", tenantID).First(&persisted).Error
	_, runtimePolicy, err := operatorExecutionPolicy(db, tenantID)
	if err != nil || !runtimePolicy.ReadEnabled {
		c.Status(http.StatusNotFound)
		return
	}
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	var body operatorInvestigationRequest
	if err := decoder.Decode(&body); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator investigation request"})
		return
	}
	softSpend, hardSpend := operatorSpendStatus(db, tenantID, persisted.InteractiveSoftSpendLimit, persisted.DeepHardSpendLimit)
	if body.Tier == "reasoning" && hardSpend {
		c.JSON(http.StatusTooManyRequests, gin.H{"message": "Deep Operator reasoning is hard-stopped by the current spend policy", "code": "deep_hard_spend_stop"})
		return
	}
	if body.Tier != "reasoning" && softSpend && !body.SpendAcknowledged {
		c.JSON(http.StatusConflict, gin.H{"message": "Interactive Operator spend warning requires acknowledgement", "code": "interactive_soft_spend_warning"})
		return
	}
	body.Locale = strings.TrimSpace(body.Locale)
	if body.Locale == "" {
		body.Locale = "en"
	}
	var threadID *uint
	if strings.TrimSpace(body.ThreadID) != "" {
		publicID, err := uuid.Parse(body.ThreadID)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator thread"})
			return
		}
		var thread models.OperatorThread
		if err := db.Where("public_id=? AND tenant_id=? AND creator_id=?", publicID, tenantID, principal.UserID).First(&thread).Error; err != nil {
			c.JSON(http.StatusNotFound, gin.H{"message": "Operator thread not found"})
			return
		}
		threadID = &thread.ID
	}
	accessClient, err := operatorpkg.NewIAMAccessClientFromEnv()
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Current Operator access cannot be verified"})
		return
	}
	access, err := accessClient.Snapshot(c.Request.Context(), principal.UserID, tenantID)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Current Operator access cannot be verified"})
		return
	}
	var reasoner operatorpkg.Reasoner
	if runtimePolicy.LLMEnabled {
		reasoner, _ = operatorpkg.NewHTTPReasonerFromEnv() // nil produces a safe deterministic degradation.
	}
	store := operatorpkg.NewInvestigationStore(db)
	fabric := operatorpkg.NewContextFabric(db, operatorpkg.DefaultAdapterRegistry())
	memoryToken := os.Getenv("ENRICHMENT_SERVICE_TOKEN")
	if memoryToken == "" {
		memoryToken = os.Getenv("SERVICE_AUTH_TOKEN")
	}
	if memoryToken == "" {
		memoryToken = os.Getenv("CMS_SERVICE_TOKEN")
	}
	if memoryEmbedder, err := operatorpkg.NewHTTPMemoryEmbedder(os.Getenv("ENRICHMENT_BASE_URL"), memoryToken, nil); err == nil {
		fabric.WithMemoryRetrieval(operatorpkg.NewMemoryStore(db), memoryEmbedder)
	}
	coordinator := operatorpkg.NewInvestigationCoordinator(fabric, store, reasoner)
	input := operatorpkg.InvestigationInput{VisibleContext: body.VisibleContext, Intent: body.Intent, Locale: body.Locale, Message: body.Message, Tier: body.Tier, ThreadID: threadID}
	investigation, err := coordinator.Start(c.Request.Context(), access, runtimePolicy, input)
	if err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"message": "Operator investigation could not be started"})
		return
	}
	if err := store.QueueBackground(c.Request.Context(), investigation.ID, tenantID); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Operator investigation could not be queued"})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"investigation_id": investigation.PublicID, "state": "backgrounded"})
	go runBackgroundOperatorInvestigation(db, accessClient, coordinator, store, investigation, input)
}

// CreateOperatorBriefing is deliberately deterministic and read-only. It
// provides a useful first view without creating a conversation, plan, or LLM
// request; the first administrator message remains the conversation boundary.
func CreateOperatorBriefing(c *gin.Context) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"message": "Authentication required"})
		return
	}
	tenantID, err := operatorpkg.RequireExplicitTenant(principal)
	if err != nil {
		c.JSON(http.StatusForbidden, gin.H{"message": "Explicit tenant claim required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	_, policy, err := operatorExecutionPolicy(db, tenantID)
	if err != nil || !policy.ReadEnabled {
		c.JSON(http.StatusOK, gin.H{"headline": "Operator is read-only unavailable", "items": []gin.H{}, "suggested_questions": []gin.H{}})
		return
	}
	var body struct {
		VisibleContext operatorpkg.VisibleContext `json:"visible_context"`
		Locale         string                     `json:"locale"`
	}
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil || decoder.Decode(&struct{}{}) != io.EOF || body.VisibleContext.Validate() != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator briefing context"})
		return
	}
	if body.Locale != "ar" {
		body.Locale = "en"
	}
	accessClient, err := operatorpkg.NewIAMAccessClientFromEnv()
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Current Operator access cannot be verified"})
		return
	}
	access, err := accessClient.Snapshot(c.Request.Context(), principal.UserID, tenantID)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Current Operator access cannot be verified"})
		return
	}
	packet, err := operatorpkg.NewContextFabric(db, operatorpkg.DefaultAdapterRegistry()).BuildPacket(c.Request.Context(), body.VisibleContext, access)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Current Operator evidence cannot be collected"})
		return
	}
	headline := "Ready to investigate this operational context"
	questions := []gin.H{{"intent": "investigate", "text": "What is blocked, and what evidence supports it?"}, {"intent": "recommend", "text": "What is the safest next step?"}}
	if body.Locale == "ar" {
		headline = "جاهز للتحقيق في هذا السياق التشغيلي"
		questions = []gin.H{{"intent": "investigate", "text": "ما الذي يعيق العمل وما الأدلة التي تدعمه؟"}, {"intent": "recommend", "text": "ما الخطوة التالية الأكثر أماناً؟"}}
	}
	items := []gin.H{{"id": "context:" + body.VisibleContext.Domain + ":" + body.VisibleContext.View, "domain": body.VisibleContext.Domain, "kind": "context", "severity": "info", "title": body.VisibleContext.Domain, "summary": body.VisibleContext.View}}
	for _, evidence := range packet.Evidence {
		if operatorpkg.IsInternalDeepLink(evidence.DeepLink) {
			items = append(items, gin.H{"id": evidence.EvidenceID, "domain": evidence.Domain, "kind": "evidence", "severity": string(evidence.Availability), "title": evidence.Domain, "summary": evidence.AdapterKey, "deep_link": evidence.DeepLink})
		}
		if len(items) == 5 {
			break
		}
	}
	c.JSON(http.StatusOK, gin.H{"generated_at": time.Now().UTC(), "context": body.VisibleContext, "headline": headline, "items": items, "suggested_questions": questions})
}

func runBackgroundOperatorInvestigation(db *gorm.DB, accessClient *operatorpkg.IAMAccessClient, coordinator *operatorpkg.InvestigationCoordinator, store *operatorpkg.InvestigationStore, investigation models.OperatorInvestigation, input operatorpkg.InvestigationInput) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	access, err := accessClient.Snapshot(ctx, investigation.ActorID, investigation.TenantID)
	if err != nil {
		_ = store.Fail(ctx, investigation.ID, investigation.TenantID, "access_unavailable")
		return
	}
	var persisted models.OperatorPolicy
	_ = db.WithContext(ctx).Where("tenant_id=?", investigation.TenantID).First(&persisted).Error
	_, policy, err := operatorExecutionPolicy(db, investigation.TenantID)
	if err != nil || !policy.ReadEnabled {
		_ = store.Fail(ctx, investigation.ID, investigation.TenantID, "policy_unavailable")
		return
	}
	if input.Tier == "reasoning" && operatorScheduledHardSpendStop(db, investigation.TenantID, persisted.DeepHardSpendLimit) {
		_ = store.Fail(ctx, investigation.ID, investigation.TenantID, "deep_hard_spend_stop")
		return
	}
	_, _ = coordinator.Process(ctx, investigation, access, policy, input)
}

// ListOperatorInvestigationEvents is the reconnect-safe read path used by the
// Console BFF. It accepts only an opaque CMS investigation UUID and a numeric
// event cursor; it never accepts an upstream URL or arbitrary event source.
func ListOperatorInvestigationEvents(c *gin.Context) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"message": "Authentication required"})
		return
	}
	tenantID, err := operatorpkg.RequireExplicitTenant(principal)
	if err != nil {
		c.JSON(http.StatusForbidden, gin.H{"message": "Explicit tenant claim required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	_, runtimePolicy, err := operatorExecutionPolicy(db, tenantID)
	if err != nil || !runtimePolicy.ReadEnabled {
		c.Status(http.StatusNotFound)
		return
	}
	publicID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator investigation"})
		return
	}
	after := int64(0)
	cursor := strings.TrimSpace(c.GetHeader("Last-Event-ID"))
	if cursor == "" {
		cursor = strings.TrimSpace(c.Query("after"))
	}
	if cursor != "" {
		after, err = strconv.ParseInt(cursor, 10, 64)
		if err != nil || after < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator event cursor"})
			return
		}
	}
	store := operatorpkg.NewInvestigationStore(db)
	investigation, events, err := store.LoadEvents(c.Request.Context(), publicID.String(), tenantID, principal.UserID, after, 200)
	if err == gorm.ErrRecordNotFound {
		c.JSON(http.StatusNotFound, gin.H{"message": "Operator investigation not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Operator events are unavailable"})
		return
	}
	next := after
	if len(events) > 0 {
		next = events[len(events)-1].Sequence
	}
	c.JSON(http.StatusOK, gin.H{"investigation_id": investigation.PublicID, "state": investigation.State, "events": events, "next_sequence": next})
}

func CancelOperatorInvestigation(c *gin.Context) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"message": "Authentication required"})
		return
	}
	tenantID, err := operatorpkg.RequireExplicitTenant(principal)
	if err != nil {
		c.JSON(http.StatusForbidden, gin.H{"message": "Explicit tenant claim required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	_, policy, err := operatorExecutionPolicy(db, tenantID)
	if err != nil || !policy.ReadEnabled {
		c.Status(http.StatusNotFound)
		return
	}
	publicID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator investigation"})
		return
	}
	investigation, err := operatorpkg.NewInvestigationStore(db).Cancel(c.Request.Context(), publicID.String(), tenantID, principal.UserID)
	if err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"message": "Operator investigation cannot be cancelled after collection begins"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": investigation.PublicID, "state": investigation.State, "finished_at": investigation.FinishedAt})
}

func CreateOperatorThread(c *gin.Context) {
	principal, tenantID, db, ok := operatorGovernancePrincipal(c)
	if !ok {
		return
	}
	_, policy, err := operatorExecutionPolicy(db, tenantID)
	if err != nil || !policy.ReadEnabled {
		c.Status(http.StatusNotFound)
		return
	}
	var body operatorThreadRequest
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator thread"})
		return
	}
	if body.Locale == "" {
		body.Locale = "en"
	}
	thread, err := operatorpkg.NewConversationStore(db).CreateThread(c.Request.Context(), tenantID, principal.UserID, body.Title, body.Locale)
	if err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"message": "Operator thread could not be created"})
		return
	}
	c.JSON(http.StatusCreated, operatorThreadResponse(thread))
}

func ListOperatorThreads(c *gin.Context) {
	principal, tenantID, db, ok := operatorGovernancePrincipal(c)
	if !ok {
		return
	}
	_, policy, err := operatorExecutionPolicy(db, tenantID)
	if err != nil || !policy.ReadEnabled {
		c.Status(http.StatusNotFound)
		return
	}
	limit := 50
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		parsed, parseErr := strconv.Atoi(raw)
		if parseErr != nil || parsed < 1 || parsed > 100 {
			c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator thread limit"})
			return
		}
		limit = parsed
	}
	var threads []models.OperatorThread
	query := db.Where("tenant_id=? AND creator_id=? AND expires_at>?", tenantID, principal.UserID, time.Now().UTC())
	if c.Query("archived") == "true" {
		query = query.Where("archived_at IS NOT NULL")
	} else {
		query = query.Where("archived_at IS NULL")
	}
	if c.Query("pinned") == "true" {
		query = query.Where("pinned_at IS NOT NULL")
	}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		query = query.Where("title ILIKE ?", "%"+q+"%")
	}
	if raw := strings.TrimSpace(c.Query("cursor")); raw != "" {
		cursor, cursorErr := decodeOperatorCursor(raw)
		if cursorErr != nil {
			c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator thread cursor"})
			return
		}
		if cursor.Pinned {
			query = query.Where("(pinned_at IS NOT NULL AND (last_activity_at < ? OR (last_activity_at = ? AND public_id::text < ?))) OR pinned_at IS NULL", cursor.Time, cursor.Time, cursor.ID)
		} else {
			query = query.Where("pinned_at IS NULL AND (last_activity_at < ? OR (last_activity_at = ? AND public_id::text < ?))", cursor.Time, cursor.Time, cursor.ID)
		}
	}
	if err := query.Order("pinned_at IS NULL ASC, last_activity_at DESC, public_id DESC").Limit(limit + 1).Find(&threads).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Operator threads are unavailable"})
		return
	}
	hasMore := len(threads) > limit
	if hasMore {
		threads = threads[:limit]
	}
	items := make([]gin.H, 0, len(threads))
	for _, thread := range threads {
		item := operatorThreadResponse(thread)
		var latest models.OperatorInvestigation
		if err := db.Where("thread_id=? AND tenant_id=? AND actor_id=?", thread.ID, tenantID, principal.UserID).Order("updated_at DESC").First(&latest).Error; err == nil {
			item["last_state"] = latest.State
			item["last_domain"] = operatorTaskDomain(latest)
		}
		var unread int64
		_ = db.Model(&models.OperatorInvestigation{}).Where("thread_id=? AND tenant_id=? AND actor_id=? AND read_at IS NULL", thread.ID, tenantID, principal.UserID).Count(&unread).Error
		item["unread_count"] = unread
		items = append(items, item)
	}
	nextCursor := ""
	if hasMore && len(threads) > 0 {
		last := threads[len(threads)-1]
		nextCursor = encodeOperatorCursor(operatorCursor{Time: last.LastActivityAt, ID: last.PublicID.String(), Pinned: last.PinnedAt != nil})
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "next_cursor": nextCursor})
}

func GetOperatorThread(c *gin.Context) {
	principal, tenantID, db, ok := operatorGovernancePrincipal(c)
	if !ok {
		return
	}
	_, policy, err := operatorExecutionPolicy(db, tenantID)
	if err != nil || !policy.ReadEnabled {
		c.Status(http.StatusNotFound)
		return
	}
	publicID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator thread"})
		return
	}
	var thread models.OperatorThread
	if err := db.Where("public_id=? AND tenant_id=? AND creator_id=?", publicID, tenantID, principal.UserID).First(&thread).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "Operator thread not found"})
		return
	}
	var messages []models.OperatorMessage
	if err := db.Where("thread_id=? AND tenant_id=?", thread.ID, tenantID).Order("created_at ASC").Limit(200).Find(&messages).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Operator thread messages are unavailable"})
		return
	}
	response := make([]gin.H, 0, len(messages))
	for _, message := range messages {
		var content any
		_ = json.Unmarshal(message.Content, &content)
		item := gin.H{"id": message.PublicID, "kind": message.MessageKind, "actor_type": message.ActorType, "created_at": message.CreatedAt, "content": content}
		if message.InvestigationID != nil {
			var linked models.OperatorInvestigation
			if err := db.Select("public_id").Where("id=? AND tenant_id=? AND actor_id=?", *message.InvestigationID, tenantID, principal.UserID).First(&linked).Error; err == nil {
				item["investigation_id"] = linked.PublicID
			}
		}
		if message.PlanID != nil {
			var linked models.OperatorActionPlan
			if err := db.Select("public_id").Where("id=? AND tenant_id=? AND actor_id=?", *message.PlanID, tenantID, principal.UserID).First(&linked).Error; err == nil {
				item["plan_id"] = linked.PublicID
			}
		}
		response = append(response, item)
	}
	c.JSON(http.StatusOK, gin.H{"thread": operatorThreadResponse(thread), "messages": response})
}

func UpdateOperatorThread(c *gin.Context) {
	principal, tenantID, db, ok := operatorGovernancePrincipal(c)
	if !ok {
		return
	}
	_, policy, err := operatorExecutionPolicy(db, tenantID)
	if err != nil || !policy.ReadEnabled {
		c.Status(http.StatusNotFound)
		return
	}
	publicID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator thread"})
		return
	}
	var body operatorThreadRequest
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil || decoder.Decode(&struct{}{}) != io.EOF || (body.Locale != "" && body.Locale != "en" && body.Locale != "ar") || len(strings.TrimSpace(body.Title)) > 240 {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator thread update"})
		return
	}
	updates := map[string]any{"updated_at": time.Now().UTC()}
	if body.Title != "" {
		updates["title"] = strings.TrimSpace(body.Title)
	}
	if body.Locale != "" {
		updates["locale"] = body.Locale
	}
	if body.Pinned != nil {
		if *body.Pinned {
			updates["pinned_at"] = time.Now().UTC()
		} else {
			updates["pinned_at"] = nil
		}
	}
	if body.Archived != nil {
		if *body.Archived {
			updates["archived_at"] = time.Now().UTC()
		} else {
			updates["archived_at"] = nil
		}
	}
	var thread models.OperatorThread
	if err := db.Where("public_id=? AND tenant_id=? AND creator_id=?", publicID, tenantID, principal.UserID).First(&thread).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "Operator thread not found"})
		return
	}
	if err := db.Model(&thread).Updates(updates).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Operator thread could not be updated"})
		return
	}
	if err := db.First(&thread, thread.ID).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Operator thread is unavailable"})
		return
	}
	c.JSON(http.StatusOK, operatorThreadResponse(thread))
}

func DeleteOperatorThread(c *gin.Context) {
	principal, tenantID, db, ok := operatorGovernancePrincipal(c)
	if !ok {
		return
	}
	_, policy, err := operatorExecutionPolicy(db, tenantID)
	if err != nil || !policy.ReadEnabled {
		c.Status(http.StatusNotFound)
		return
	}
	publicID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator thread"})
		return
	}
	if err := operatorpkg.NewConversationStore(db).DeleteThreadContent(c.Request.Context(), publicID, tenantID, principal.UserID); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "Operator thread not found"})
		return
	}
	c.Status(http.StatusNoContent)
}

func AppendOperatorThreadMessage(c *gin.Context) {
	principal, tenantID, db, ok := operatorGovernancePrincipal(c)
	if !ok {
		return
	}
	_, policy, err := operatorExecutionPolicy(db, tenantID)
	if err != nil || !policy.ReadEnabled {
		c.Status(http.StatusNotFound)
		return
	}
	publicID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator thread"})
		return
	}
	var body operatorThreadMessageRequest
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator message"})
		return
	}
	// Reuse the canonical request redactor before conversation content reaches
	// storage; threads are never an exception to credential hygiene.
	request, err := operatorpkg.NewInvestigationRequest(operatorpkg.IntentExplain, body.Text, "fast")
	if err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"message": "Operator message is invalid"})
		return
	}
	var thread models.OperatorThread
	if err := db.Where("public_id=? AND tenant_id=? AND creator_id=?", publicID, tenantID, principal.UserID).First(&thread).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "Operator thread not found"})
		return
	}
	message, err := operatorpkg.NewConversationStore(db).AppendMessage(c.Request.Context(), thread.ID, tenantID, principal.UserID, "admin", principal.UserID, map[string]any{"text": request.Message, "credential_redactions": request.CredentialRedactionCount})
	if err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"message": "Operator message could not be saved"})
		return
	}
	c.JSON(http.StatusCreated, message)
}

func operatorThreadResponse(thread models.OperatorThread) gin.H {
	return gin.H{"id": thread.PublicID, "title": thread.Title, "locale": thread.Locale, "last_activity_at": thread.LastActivityAt, "expires_at": thread.ExpiresAt, "created_at": thread.CreatedAt, "pinned_at": thread.PinnedAt, "archived_at": thread.ArchivedAt}
}

// ListOperatorRecommendations exposes only the caller's completed, ranked
// recommendations. The immutable packet remains the evidence authority; this
// read model supplies the public UUID required by feedback plans.
func ListOperatorRecommendations(c *gin.Context) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"message": "Authentication required"})
		return
	}
	tenantID, err := operatorpkg.RequireExplicitTenant(principal)
	if err != nil {
		c.JSON(http.StatusForbidden, gin.H{"message": "Explicit tenant claim required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	_, policy, err := operatorExecutionPolicy(db, tenantID)
	if err != nil || !policy.ReadEnabled {
		c.Status(http.StatusNotFound)
		return
	}
	query := db.Joins("JOIN operator_investigations ON operator_investigations.id = operator_recommendations.investigation_id").Where("operator_recommendations.tenant_id=? AND operator_investigations.actor_id=? AND operator_recommendations.rank BETWEEN ? AND ?", tenantID, principal.UserID, 1, 4)
	if raw := strings.TrimSpace(c.Query("investigation_id")); raw != "" {
		publicID, parseErr := uuid.Parse(raw)
		if parseErr != nil {
			c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator investigation"})
			return
		}
		query = query.Where("operator_investigations.public_id=?", publicID)
	}
	var rows []models.OperatorRecommendation
	if err := query.Order("operator_recommendations.rank ASC").Limit(4).Find(&rows).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Operator recommendations are unavailable"})
		return
	}
	items := make([]gin.H, 0, len(rows))
	for _, row := range rows {
		var payload map[string]any
		if err := json.Unmarshal(row.Payload, &payload); err != nil {
			continue
		}
		items = append(items, gin.H{"id": row.PublicID, "rank": row.Rank, "state": row.State, "expires_at": row.ExpiresAt, "title": payload["title"], "summary": payload["summary"], "deep_link": payload["deep_link"], "manual_only": payload["manual_only"]})
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

// ListOperatorInbox exposes a creator-bound, durable task index for the
// Console header and workspace. It carries lifecycle metadata only; clients
// fetch the validated event trail for any actual response content.
func ListOperatorInbox(c *gin.Context) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"message": "Authentication required"})
		return
	}
	tenantID, err := operatorpkg.RequireExplicitTenant(principal)
	if err != nil {
		c.JSON(http.StatusForbidden, gin.H{"message": "Explicit tenant claim required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	_, policy, err := operatorExecutionPolicy(db, tenantID)
	if err != nil || !policy.ReadEnabled {
		c.Status(http.StatusNotFound)
		return
	}
	limit := 20
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > 100 {
			c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator inbox limit"})
			return
		}
	}
	items, unread, err := operatorpkg.NewInvestigationStore(db).ListInbox(c.Request.Context(), tenantID, principal.UserID, limit)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Operator inbox is unavailable"})
		return
	}
	response := make([]gin.H, 0, len(items))
	for _, item := range items {
		response = append(response, gin.H{"id": item.PublicID, "state": item.State, "locale": item.Locale, "started_at": item.StartedAt, "finished_at": item.FinishedAt, "read_at": item.ReadAt, "error_class": item.ErrorClass, "packet_fingerprint": item.PacketFingerprint})
	}
	c.JSON(http.StatusOK, gin.H{"items": response, "unread_count": unread})
}

func MarkOperatorInboxRead(c *gin.Context) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"message": "Authentication required"})
		return
	}
	tenantID, err := operatorpkg.RequireExplicitTenant(principal)
	if err != nil {
		c.JSON(http.StatusForbidden, gin.H{"message": "Explicit tenant claim required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	_, policy, err := operatorExecutionPolicy(db, tenantID)
	if err != nil || !policy.ReadEnabled {
		c.Status(http.StatusNotFound)
		return
	}
	publicID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator investigation"})
		return
	}
	item, err := operatorpkg.NewInvestigationStore(db).MarkInboxRead(c.Request.Context(), publicID.String(), tenantID, principal.UserID)
	if err == gorm.ErrRecordNotFound {
		c.JSON(http.StatusNotFound, gin.H{"message": "Operator investigation not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Operator inbox is unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": item.PublicID, "read_at": item.ReadAt})
}

// ListOperatorInvestigationEvidence exposes provenance metadata only after a
// separate creator/tenant check. Response text is still supplied by validated
// response blocks, so the inspector cannot turn arbitrary stored JSON into
// browser content.
func ListOperatorInvestigationEvidence(c *gin.Context) {
	principal, tenantID, db, ok := operatorGovernancePrincipal(c)
	if !ok {
		return
	}
	_, policy, err := operatorExecutionPolicy(db, tenantID)
	if err != nil || !policy.ReadEnabled {
		c.Status(http.StatusNotFound)
		return
	}
	publicID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator investigation"})
		return
	}
	var investigation models.OperatorInvestigation
	if err := db.Where("public_id=? AND tenant_id=? AND actor_id=?", publicID, tenantID, principal.UserID).First(&investigation).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "Operator investigation not found"})
		return
	}
	var evidence []models.OperatorEvidence
	if err := db.Where("investigation_id=? AND tenant_id=?", investigation.ID, tenantID).Order("observed_at DESC").Find(&evidence).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Operator evidence is unavailable"})
		return
	}
	items := make([]gin.H, 0, len(evidence))
	for _, row := range evidence {
		if !operatorpkg.IsInternalDeepLink(row.DeepLink) {
			continue
		}
		var refs any
		_ = json.Unmarshal(row.RecordRefs, &refs)
		items = append(items, gin.H{"id": row.PublicID, "evidence_id": row.EvidenceID, "authority": row.Authority, "domain": row.Domain, "adapter_key": row.AdapterKey, "adapter_version": row.AdapterVersion, "required_permission": row.RequiredPermission, "record_refs": refs, "deep_link": row.DeepLink, "observed_at": row.ObservedAt, "fetched_at": row.FetchedAt, "expires_at": row.ExpiresAt, "availability": row.Availability, "source_version": row.SourceVersion})
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

type operatorTaskItem struct {
	updated time.Time
	id      string
	kind    string
	value   gin.H
}

func operatorTaskInGroup(state, group string) bool {
	if group == "" {
		return true
	}
	active := map[string]bool{"accepted": true, "backgrounded": true, "running": true, "queued": true, "claimed": true, "verifying": true, "active": true}
	failed := map[string]bool{"failed": true, "blocked": true, "paused": true}
	completed := map[string]bool{"completed": true, "done": true, "succeeded": true, "cancelled": true}
	switch group {
	case "active":
		return active[state]
	case "needs_approval":
		return state == "awaiting_approval"
	case "failed":
		return failed[state]
	case "completed":
		return completed[state]
	default:
		return false
	}
}

// ListOperatorTasks is the owner-bound, cursor-based task center across read
// investigations, signed plans, schedules, and their runs.
func ListOperatorTasks(c *gin.Context) {
	principal, tenantID, db, ok := operatorGovernancePrincipal(c)
	if !ok {
		return
	}
	_, policy, err := operatorExecutionPolicy(db, tenantID)
	if err != nil || !policy.ReadEnabled {
		c.Status(http.StatusNotFound)
		return
	}
	limit := 50
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		if parsed, parseErr := strconv.Atoi(raw); parseErr != nil || parsed < 1 || parsed > 100 {
			c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator task limit"})
			return
		} else {
			limit = parsed
		}
	}
	state, kind, group := strings.TrimSpace(c.Query("state")), strings.TrimSpace(c.Query("kind")), strings.TrimSpace(c.Query("group"))
	if kind == "" {
		kind = "all"
	}
	validKinds := map[string]bool{"all": true, "investigation": true, "plan": true, "schedule": true, "schedule_run": true}
	validGroups := map[string]bool{"": true, "active": true, "needs_approval": true, "failed": true, "completed": true}
	if !validKinds[kind] || !validGroups[group] {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator task filter"})
		return
	}
	var after *operatorCursor
	if raw := strings.TrimSpace(c.Query("cursor")); raw != "" {
		parsed, cursorErr := decodeOperatorCursor(raw)
		if cursorErr != nil {
			c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator task cursor"})
			return
		}
		after = &parsed
	}
	tasks := make([]operatorTaskItem, 0)
	if kind == "all" || kind == "investigation" {
		query := db.Where("tenant_id=? AND actor_id=?", tenantID, principal.UserID)
		if after != nil {
			query = query.Where("updated_at <= ?", after.Time)
		}
		if state != "" {
			query = query.Where("state=?", state)
		}
		var rows []models.OperatorInvestigation
		if err := query.Order("updated_at DESC").Limit(101).Find(&rows).Error; err == nil {
			for _, row := range rows {
				if !operatorTaskInGroup(row.State, group) {
					continue
				}
				item := gin.H{"id": row.PublicID, "kind": "investigation", "title": operatorTaskTitle(row), "domain": operatorTaskDomain(row), "state": row.State, "started_at": row.StartedAt, "updated_at": row.UpdatedAt, "unread": row.ReadAt == nil, "can_cancel": row.State == "accepted" || row.State == "backgrounded", "investigation_id": row.PublicID, "deep_link": "/platform/operator?investigation=" + row.PublicID.String()}
				if row.ThreadID != nil {
					var thread models.OperatorThread
					if db.Select("public_id").Where("id=? AND tenant_id=? AND creator_id=?", *row.ThreadID, tenantID, principal.UserID).First(&thread).Error == nil {
						item["thread_id"] = thread.PublicID
						item["deep_link"] = "/platform/operator?thread=" + thread.PublicID.String() + "&investigation=" + row.PublicID.String()
					}
				}
				tasks = append(tasks, operatorTaskItem{updated: row.UpdatedAt, id: row.PublicID.String(), kind: "investigation", value: item})
			}
		}
	}
	if kind == "all" || kind == "plan" {
		query := db.Where("tenant_id=? AND actor_id=?", tenantID, principal.UserID)
		if after != nil {
			query = query.Where("updated_at <= ?", after.Time)
		}
		if state != "" {
			query = query.Where("state=?", state)
		}
		var rows []models.OperatorActionPlan
		if err := query.Order("updated_at DESC").Limit(101).Find(&rows).Error; err == nil {
			for _, row := range rows {
				if !operatorTaskInGroup(row.State, group) {
					continue
				}
				item := gin.H{"id": row.PublicID, "kind": "plan", "title": row.ToolKey, "state": row.State, "risk_tier": row.RiskTier, "started_at": row.CreatedAt, "updated_at": row.UpdatedAt, "can_cancel": row.State == "awaiting_approval" || row.State == "queued", "plan_id": row.PublicID, "deep_link": "/platform/operator?plan=" + row.PublicID.String()}
				if row.InvestigationID != nil {
					var investigation models.OperatorInvestigation
					if db.Select("public_id").Where("id=? AND tenant_id=? AND actor_id=?", *row.InvestigationID, tenantID, principal.UserID).First(&investigation).Error == nil {
						item["investigation_id"] = investigation.PublicID
						item["deep_link"] = "/platform/operator?investigation=" + investigation.PublicID.String() + "&plan=" + row.PublicID.String()
					}
				}
				tasks = append(tasks, operatorTaskItem{updated: row.UpdatedAt, id: row.PublicID.String(), kind: "plan", value: item})
			}
		}
	}
	if kind == "all" || kind == "schedule" {
		var rows []models.OperatorSchedule
		query := db.Where("tenant_id=? AND (creator_id=? OR owner_id=?)", tenantID, principal.UserID, principal.UserID)
		if after != nil {
			query = query.Where("updated_at <= ?", after.Time)
		}
		if err := query.Order("updated_at DESC").Limit(101).Find(&rows).Error; err == nil {
			for _, row := range rows {
				if state != "" && row.State != state || !operatorTaskInGroup(row.State, group) {
					continue
				}
				item := gin.H{"id": row.PublicID, "kind": "schedule", "title": row.Cadence, "domain": "operator", "state": row.State, "started_at": row.CreatedAt, "updated_at": row.UpdatedAt, "unread": false, "can_cancel": false, "schedule_id": row.PublicID, "deep_link": "/platform/operator?task_kind=schedule"}
				tasks = append(tasks, operatorTaskItem{updated: row.UpdatedAt, id: row.PublicID.String(), kind: "schedule", value: item})
			}
		}
	}
	if kind == "all" || kind == "schedule_run" {
		var rows []models.OperatorScheduleRun
		query := db.Joins("JOIN operator_schedules ON operator_schedules.id = operator_schedule_runs.schedule_id").Where("operator_schedule_runs.tenant_id=? AND (operator_schedules.creator_id=? OR operator_schedules.owner_id=?)", tenantID, principal.UserID, principal.UserID)
		if after != nil {
			query = query.Where("operator_schedule_runs.updated_at <= ?", after.Time)
		}
		if err := query.Order("operator_schedule_runs.updated_at DESC").Limit(101).Find(&rows).Error; err == nil {
			for _, row := range rows {
				if state != "" && row.State != state || !operatorTaskInGroup(row.State, group) {
					continue
				}
				item := gin.H{"id": row.PublicID, "kind": "schedule_run", "title": row.State, "domain": "operator", "state": row.State, "started_at": row.StartedAt, "updated_at": row.UpdatedAt, "unread": false, "can_cancel": false, "schedule_run_id": row.PublicID, "deep_link": "/platform/operator?task_kind=schedule_run"}
				var schedule models.OperatorSchedule
				if db.Select("public_id").Where("id=? AND tenant_id=?", row.ScheduleID, tenantID).First(&schedule).Error == nil {
					item["schedule_id"] = schedule.PublicID
				}
				if row.ResultInvestigationID != nil {
					var investigation models.OperatorInvestigation
					if db.Select("public_id").Where("id=? AND tenant_id=? AND actor_id=?", *row.ResultInvestigationID, tenantID, principal.UserID).First(&investigation).Error == nil {
						item["investigation_id"] = investigation.PublicID
						item["deep_link"] = "/platform/operator?investigation=" + investigation.PublicID.String() + "&task_kind=schedule_run"
					}
				}
				tasks = append(tasks, operatorTaskItem{updated: row.UpdatedAt, id: row.PublicID.String(), kind: "schedule_run", value: item})
			}
		}
	}
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].updated.Equal(tasks[j].updated) {
			if tasks[i].kind == tasks[j].kind {
				return tasks[i].id > tasks[j].id
			}
			return tasks[i].kind > tasks[j].kind
		}
		return tasks[i].updated.After(tasks[j].updated)
	})
	filtered := tasks[:0]
	for _, task := range tasks {
		if after != nil && (task.updated.After(after.Time) || task.updated.Equal(after.Time) && (task.kind > after.Kind || task.kind == after.Kind && task.id >= after.ID)) {
			continue
		}
		filtered = append(filtered, task)
	}
	hasMore := len(filtered) > limit
	if hasMore {
		filtered = filtered[:limit]
	}
	items := make([]gin.H, 0, len(filtered))
	for _, task := range filtered {
		items = append(items, task.value)
	}
	nextCursor := ""
	if hasMore && len(filtered) > 0 {
		last := filtered[len(filtered)-1]
		nextCursor = encodeOperatorCursor(operatorCursor{Time: last.updated, ID: last.id, Kind: last.kind})
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "next_cursor": nextCursor})
}

func operatorTaskDomain(investigation models.OperatorInvestigation) string {
	var visible operatorpkg.VisibleContext
	if json.Unmarshal(investigation.VisibleContext, &visible) == nil {
		return visible.Domain
	}
	return "operator"
}

func operatorTaskTitle(investigation models.OperatorInvestigation) string {
	input, err := operatorpkg.DecodeStoredInvestigationInput(investigation)
	if err != nil {
		return investigation.State
	}
	title := strings.Join(strings.Fields(input.Message), " ")
	if len(title) > 100 {
		title = title[:100]
	}
	if title == "" {
		return investigation.State
	}
	return title
}

// CreateOperatorPlan builds a new immutable preview from fresh CMS evidence.
// The request carries only a prior investigation identity, a code-registered
// tool key, and exact IDs; arguments and evidence are reconstructed by CMS.
func CreateOperatorPlan(c *gin.Context) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"message": "Authentication required"})
		return
	}
	tenantID, err := operatorpkg.RequireExplicitTenant(principal)
	if err != nil {
		c.JSON(http.StatusForbidden, gin.H{"message": "Explicit tenant claim required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	_, policy, err := operatorExecutionPolicy(db, tenantID)
	if err != nil || !policy.ExecutionEnabled {
		c.Status(http.StatusNotFound)
		return
	}
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	var body operatorPlanCreateRequest
	if err := decoder.Decode(&body); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator plan request"})
		return
	}
	publicID, err := uuid.Parse(strings.TrimSpace(body.InvestigationID))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator investigation"})
		return
	}
	var investigation models.OperatorInvestigation
	if err := db.Where("public_id=? AND tenant_id=? AND actor_id=?", publicID, tenantID, principal.UserID).First(&investigation).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "Operator investigation not found"})
		return
	}
	input, err := operatorpkg.DecodeStoredInvestigationInput(investigation)
	if err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"message": "Operator investigation is not executable"})
		return
	}
	accessClient, err := operatorpkg.NewIAMAccessClientFromEnv()
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Current Operator access cannot be verified"})
		return
	}
	access, err := accessClient.Snapshot(c.Request.Context(), principal.UserID, tenantID)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Current Operator access cannot be verified"})
		return
	}
	packet, err := operatorpkg.NewContextFabric(db, operatorpkg.DefaultAdapterRegistry()).BuildPacket(c.Request.Context(), input.VisibleContext, access)
	if err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"message": "Fresh context could not be collected for this plan"})
		return
	}
	catalog := operatorpkg.DefaultToolCatalog()
	if isOperatorSupplyRecoveryTool(body.ToolKey) {
		if len(body.TargetIDs) != 1 {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"message": "Requested Supply recovery is ambiguous"})
			return
		}
		episodeID, parseErr := uuid.Parse(body.TargetIDs[0])
		if parseErr != nil {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"message": "Requested Supply episode is invalid"})
			return
		}
		if _, _, eligibilityErr := operatorSupplyEligibleCandidate(db, tenantID, episodeID, body.ToolKey); eligibilityErr != nil {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"message": "Requested Supply recovery is no longer eligible"})
			return
		}
	}
	arguments, err := catalog.DeriveArguments(body.ToolKey, body.TargetIDs)
	if err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"message": "Requested Operator action is not admitted"})
		return
	}
	if err := operatorpkg.EnsureToolCapabilityEnabled(db, tenantID, body.ToolKey, time.Now().UTC()); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"message": "Requested Operator action is disabled"})
		return
	}
	canonical, err := catalog.BuildCanonicalPlan(packet, access, body.ToolKey, body.TargetIDs, arguments)
	if err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"message": "Fresh evidence does not permit this action plan"})
		return
	}
	signingKey, err := operatorpkg.PlanSigningKeyFromEnv()
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Operator plan signing is unavailable"})
		return
	}
	stored, err := operatorpkg.NewPlanStore(db, signingKey).CreatePlan(c.Request.Context(), canonical, time.Now().UTC().Add(15*time.Minute))
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Operator plan could not be persisted"})
		return
	}
	// The signed envelope remains immutable; this relational link is only the
	// case-navigation edge needed to restore plan cards in a conversation.
	if err := db.Model(&models.OperatorActionPlan{}).Where("id=?", stored.ID).Update("investigation_id", investigation.ID).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Operator plan case link could not be persisted"})
		return
	}
	stored.InvestigationID = &investigation.ID
	if investigation.ThreadID != nil {
		content, marshalErr := json.Marshal(map[string]any{"tool_key": stored.ToolKey, "state": stored.State, "digest": stored.Digest})
		if marshalErr == nil {
			_ = db.Create(&models.OperatorMessage{ThreadID: *investigation.ThreadID, TenantID: tenantID, ActorType: "system", MessageKind: "plan", InvestigationID: &investigation.ID, PlanID: &stored.ID, Content: content}).Error
		}
	}
	c.JSON(http.StatusCreated, operatorPlanResponse(db, stored, canonical))
}

// ApproveOperatorPlan binds an exact current IAM identity and localized
// confirmation phrase to the signed plan. The phrase is transformed to a
// one-way proof; it is never sent to Enrichment or stored as plaintext.
func ApproveOperatorPlan(c *gin.Context) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"message": "Authentication required"})
		return
	}
	tenantID, err := operatorpkg.RequireExplicitTenant(principal)
	if err != nil {
		c.JSON(http.StatusForbidden, gin.H{"message": "Explicit tenant claim required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	_, policy, err := operatorExecutionPolicy(db, tenantID)
	if err != nil || !policy.ExecutionEnabled {
		c.Status(http.StatusNotFound)
		return
	}
	publicID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator plan"})
		return
	}
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	var body operatorPlanApprovalRequest
	if err := decoder.Decode(&body); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator plan confirmation"})
		return
	}
	signingKey, err := operatorpkg.PlanSigningKeyFromEnv()
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Operator plan signing is unavailable"})
		return
	}
	store := operatorpkg.NewPlanStore(db, signingKey)
	stored, canonical, err := store.LoadPlanForActor(c.Request.Context(), publicID.String(), tenantID, principal.UserID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "Operator plan not found"})
		return
	}
	if !validOperatorConfirmationForPlan(stored, body.Confirmation) {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Confirmation does not match this signed plan"})
		return
	}
	accessClient, err := operatorpkg.NewIAMAccessClientFromEnv()
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Current Operator access cannot be verified"})
		return
	}
	access, err := accessClient.Snapshot(c.Request.Context(), principal.UserID, tenantID)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Current Operator access cannot be verified"})
		return
	}
	approved, err := store.ApprovePlan(c.Request.Context(), stored.ID, access, principal.UserID, operatorConfirmationProof(publicID.String(), stored.Digest, body.Confirmation))
	if err != nil {
		if isOperatorPlanQueueSchemaError(err) {
			c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Operator execution queue schema is not ready", "code": "operator_plan_queue_schema"})
			return
		}
		c.JSON(http.StatusUnprocessableEntity, gin.H{"message": "Operator plan approval preconditions changed"})
		return
	}
	c.JSON(http.StatusOK, operatorPlanResponse(db, approved, canonical))
}

// Approval is the first transition that writes the durable queued state. A
// stale database constraint must not be reported as an IAM/precondition race;
// expose a stable readiness error so the operator can apply the canonical
// migration instead of retrying a request that can never succeed.
func isOperatorPlanQueueSchemaError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "operator_action_plans_state_check") ||
		strings.Contains(message, "operator_action_plans") && strings.Contains(message, "check constraint")
}

// CancelOperatorPlan exposes only the registered before-start cancellation
// window. It is an audited state transition, never a generic process kill.
func CancelOperatorPlan(c *gin.Context) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"message": "Authentication required"})
		return
	}
	tenantID, err := operatorpkg.RequireExplicitTenant(principal)
	if err != nil {
		c.JSON(http.StatusForbidden, gin.H{"message": "Explicit tenant claim required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	_, policy, err := operatorExecutionPolicy(db, tenantID)
	if err != nil || !policy.ExecutionEnabled {
		c.Status(http.StatusNotFound)
		return
	}
	publicID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator plan"})
		return
	}
	accessClient, err := operatorpkg.NewIAMAccessClientFromEnv()
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Current Operator access cannot be verified"})
		return
	}
	access, err := accessClient.Snapshot(c.Request.Context(), principal.UserID, tenantID)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Current Operator access cannot be verified"})
		return
	}
	signingKey, err := operatorpkg.PlanSigningKeyFromEnv()
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Operator plan signing is unavailable"})
		return
	}
	store := operatorpkg.NewPlanStore(db, signingKey)
	stored, canonical, err := store.LoadPlanForActor(c.Request.Context(), publicID.String(), tenantID, principal.UserID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "Operator plan not found"})
		return
	}
	cancelled, err := store.CancelPlan(c.Request.Context(), stored.ID, access)
	if err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"message": "Operator plan is outside its cancellation window"})
		return
	}
	c.JSON(http.StatusOK, operatorPlanResponse(db, cancelled, canonical))
}

func operatorExecutionPolicy(db *gorm.DB, tenantID string) (operatorpkg.LaunchMode, operatorpkg.RuntimePolicy, error) {
	bootMode, err := operatorpkg.LaunchModeFromEnv()
	if err != nil {
		return "", operatorpkg.RuntimePolicy{}, err
	}
	var persisted models.OperatorPolicy
	var row *models.OperatorPolicy
	// A missing row is the normal state for a tenant that relies on the
	// operational-by-default policy. Use Find instead of First so GORM does
	// not emit a record-not-found error for every Operator request; the
	// capability-control overlay below remains the authoritative kill switch.
	if err := db.Where("tenant_id=?", tenantID).Limit(1).Find(&persisted).Error; err == nil && persisted.TenantID != "" {
		row = &persisted
	} else if err != nil {
		return "", operatorpkg.RuntimePolicy{}, err
	}
	policy, err := operatorpkg.ResolveRuntimePolicy(bootMode, row)
	if err != nil {
		return bootMode, policy, err
	}
	now := time.Now().UTC()
	for _, capability := range []struct {
		key string
		set func(bool)
	}{
		{"read", func(enabled bool) { policy.ReadEnabled = enabled }},
		{"llm", func(enabled bool) { policy.LLMEnabled = enabled }},
		{"execution", func(enabled bool) { policy.ExecutionEnabled = enabled }},
		{"schedules", func(enabled bool) { policy.SchedulesEnabled = enabled }},
	} {
		if checkErr := operatorpkg.EnsureSystemCapabilityEnabled(db, tenantID, capability.key, now); checkErr != nil {
			capability.set(false)
		}
	}
	return bootMode, policy, nil
}

func validOperatorConfirmationForPlan(plan models.OperatorActionPlan, value string) bool {
	value = strings.TrimSpace(value)
	if plan.RiskTier == string(operatorpkg.RiskRoutine) {
		return value == "APPROVE" || value == "أوافق"
	}
	if plan.RiskTier != string(operatorpkg.RiskHigh) || len(plan.Digest) < 8 {
		return false
	}
	prefix := strings.ToUpper(plan.Digest[:8])
	return value == "APPROVE "+prefix || value == "أوافق "+prefix
}

func operatorConfirmationProof(publicID string, digest string, confirmation string) string {
	sum := sha256.Sum256([]byte("wahb-operator-confirmation-v1:" + publicID + ":" + digest + ":" + strings.TrimSpace(confirmation)))
	return fmt.Sprintf("%x", sum[:])
}

func operatorPlanResponse(db *gorm.DB, plan models.OperatorActionPlan, canonical operatorpkg.CanonicalPlan) gin.H {
	response := gin.H{"id": plan.PublicID, "state": plan.State, "tool_key": plan.ToolKey, "risk_tier": plan.RiskTier, "expires_at": plan.ExpiresAt, "digest": plan.Digest, "canonical_plan": canonical}
	if plan.RiskTier == string(operatorpkg.RiskHigh) && len(plan.Digest) >= 8 {
		response["confirmation_phrases"] = []string{"APPROVE " + strings.ToUpper(plan.Digest[:8]), "أوافق " + strings.ToUpper(plan.Digest[:8])}
	} else {
		response["confirmation_phrases"] = []string{"APPROVE", "أوافق"}
	}
	descriptor, admitted := operatorpkg.DefaultToolCatalog().Lookup(plan.ToolKey)
	if admitted {
		response["affected_domains"] = descriptor.AffectedDomains
		response["localized_action_key"] = descriptor.LocalizedActionKey
		response["target_type"] = descriptor.TargetType
	}
	if plan.InvestigationID != nil {
		var investigation models.OperatorInvestigation
		if err := db.Select("public_id").Where("id=? AND tenant_id=? AND actor_id=?", *plan.InvestigationID, plan.TenantID, plan.ActorID).First(&investigation).Error; err == nil {
			response["investigation_id"] = investigation.PublicID
		}
	}
	return response
}

// operatorVerifiedPlanEffects is deliberately derived on the CMS side. The
// Console invalidates only these verified domains/subjects and can deep-link
// only to a registered native surface; it never maps tool keys itself.
func operatorVerifiedPlanEffects(canonical operatorpkg.CanonicalPlan, descriptor operatorpkg.ToolDescriptor, step *models.OperatorActionStep) gin.H {
	effects := gin.H{"affected_domains": descriptor.AffectedDomains, "affected_subjects": canonical.TargetIDs, "deep_links": operatorPlanDeepLinks(descriptor.Key, canonical.TargetIDs)}
	if step != nil {
		effects["before"] = json.RawMessage(step.BeforeState)
		effects["after"] = json.RawMessage(step.AfterState)
		effects["verified"] = json.RawMessage(step.VerifiedState)
	}
	return effects
}

func operatorPlanDeepLinks(toolKey string, targets []string) []string {
	if len(targets) != 1 {
		return nil
	}
	switch toolKey {
	case "operator.schedule.create.hourly", "operator.schedule.create.daily", "operator.schedule.create.weekly", "operator.schedule.pause", "operator.schedule.resume", "operator.schedule.takeover", "operator.share.create", "operator.share.revoke", "operator.recommendation.snooze.15m", "operator.recommendation.snooze.1h", "operator.recommendation.snooze.1d", "operator.recommendation.snooze.7d", "operator.recommendation.dismiss", "operator.recommendation.subject_override", "operator.control.disable.read", "operator.control.disable.llm", "operator.control.disable.execution", "operator.control.disable.schedules", "operator.control.disable.adapter", "operator.control.disable.tool":
		return []string{"/platform/operator"}
	case "feed_integrity.refresh_snapshot":
		return []string{"/platform/feed-integrity"}
	case "feed_integrity.pause.24h":
		return []string{"/platform/feed-integrity"}
	case "real_experience.pause.24h":
		return []string{"/platform/real-experience"}
	case "retention.pause.24h":
		return []string{"/platform/retention"}
	case "ai_economics.pause.24h":
		return []string{"/platform/economics"}
	case "news_circulation.pause.24h":
		return []string{"/platform/news/circulation"}
	case "media_circulation.pause.24h", "media_circulation.supply.disable_evaluator":
		return []string{"/platform/media/circulation"}
	case "redundancy.pause.24h":
		return []string{"/platform/media/redundancy"}
	case "pipeline.pause.24h":
		return []string{"/platform/pipeline"}
	case "enrichment.pause.24h":
		return []string{"/platform/enrichment"}
	case "embeddings.pause_campaigns.24h":
		return []string{"/platform/intelligence/embeddings"}
	case "topics_preferences.pause.24h":
		return []string{"/platform/topics"}
	case "media_library.pause.24h":
		return []string{"/platform/media"}
	case "sources.run_once":
		return []string{"/platform/sources/" + url.PathEscape(targets[0])}
	case "sources.pause", "sources.resume":
		return []string{"/platform/sources/" + url.PathEscape(targets[0])}
	case "media_sources.run_once":
		return []string{"/platform/media/sources?source_id=" + url.QueryEscape(targets[0])}
	case "media_sources.pause", "media_sources.resume":
		return []string{"/platform/media/sources?source_id=" + url.QueryEscape(targets[0])}
	default:
		return nil
	}
}

// GetOperatorPlan exposes an owner-bound durable state snapshot. The worker
// owns execution; the browser may only observe or cancel in the declared
// before-start window.
func GetOperatorPlan(c *gin.Context) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"message": "Authentication required"})
		return
	}
	tenantID, err := operatorpkg.RequireExplicitTenant(principal)
	if err != nil {
		c.JSON(http.StatusForbidden, gin.H{"message": "Explicit tenant claim required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	_, policy, err := operatorExecutionPolicy(db, tenantID)
	if err != nil || !policy.ReadEnabled {
		c.Status(http.StatusNotFound)
		return
	}
	publicID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator plan"})
		return
	}
	signingKey, err := operatorpkg.PlanSigningKeyFromEnv()
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Operator plan signing is unavailable"})
		return
	}
	plan, canonical, err := operatorpkg.NewPlanStore(db, signingKey).LoadPlanForActor(c.Request.Context(), publicID.String(), tenantID, principal.UserID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "Operator plan not found"})
		return
	}
	var step models.OperatorActionStep
	if err := db.Where("plan_id=? AND ordinal=?", plan.ID, 1).First(&step).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Operator plan effects are unavailable"})
		return
	}
	response := operatorPlanResponse(db, plan, canonical)
	if descriptor, admitted := operatorpkg.DefaultToolCatalog().Lookup(plan.ToolKey); admitted {
		response["verified_effects"] = operatorVerifiedPlanEffects(canonical, descriptor, &step)
	}
	c.JSON(http.StatusOK, response)
}

func ListOperatorPlanEvents(c *gin.Context) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"message": "Authentication required"})
		return
	}
	tenantID, err := operatorpkg.RequireExplicitTenant(principal)
	if err != nil {
		c.JSON(http.StatusForbidden, gin.H{"message": "Explicit tenant claim required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	_, policy, err := operatorExecutionPolicy(db, tenantID)
	if err != nil || !policy.ReadEnabled {
		c.Status(http.StatusNotFound)
		return
	}
	publicID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator plan"})
		return
	}
	var plan models.OperatorActionPlan
	if err := db.Where("public_id=? AND tenant_id=? AND actor_id=?", publicID, tenantID, principal.UserID).First(&plan).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message": "Operator plan not found"})
		return
	}
	after := uint64(0)
	if raw := strings.TrimSpace(c.Query("after")); raw != "" {
		parsed, parseErr := strconv.ParseUint(raw, 10, 64)
		if parseErr != nil {
			c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Operator plan event cursor"})
			return
		}
		after = parsed
	}
	var events []models.OperatorPlanEvent
	if err := db.Where("plan_id=? AND tenant_id=? AND sequence>?", plan.ID, tenantID, after).Order("sequence ASC").Limit(100).Find(&events).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Operator plan events are unavailable"})
		return
	}
	next := after
	if len(events) > 0 {
		next = uint64(events[len(events)-1].Sequence)
	}
	c.JSON(http.StatusOK, gin.H{"id": plan.PublicID, "state": plan.State, "events": events, "next_sequence": next})
}

func snapshotOperatorState(snapshot models.NewsSnapshot, found bool) map[string]any {
	if !found {
		return map[string]any{"exists": false}
	}
	return map[string]any{"exists": true, "window": snapshot.Window, "slide_count": snapshot.SlideCount, "dirty": snapshot.Dirty, "generation": snapshot.Generation, "built_at": snapshot.BuiltAt.UTC().Format(time.RFC3339Nano)}
}

func executeRegisteredOperatorPlan(ctx context.Context, db *gorm.DB, tenantID string, toolKey string, plan models.OperatorActionPlan, step models.OperatorActionStep, canonical operatorpkg.CanonicalPlan, access operatorpkg.AccessSnapshot) (bool, map[string]any, map[string]any, map[string]any) {
	switch toolKey {
	case "operator.schedule.create.hourly", "operator.schedule.create.daily", "operator.schedule.create.weekly", "operator.schedule.pause", "operator.schedule.resume", "operator.schedule.takeover", "operator.recommendation.snooze.15m", "operator.recommendation.snooze.1h", "operator.recommendation.snooze.1d", "operator.recommendation.snooze.7d", "operator.recommendation.dismiss", "operator.recommendation.subject_override", "operator.control.disable.read", "operator.control.disable.llm", "operator.control.disable.execution", "operator.control.disable.schedules", "operator.control.disable.adapter", "operator.control.disable.tool":
		return executeOperatorGovernancePlan(ctx, db, tenantID, toolKey, plan, canonical, access)
	case "feed_integrity.refresh_snapshot":
		if len(canonical.TargetIDs) != 1 {
			return false, nil, map[string]any{"error_class": "invalid_registered_arguments"}, nil
		}
		window, ok := canonical.NormalizedArguments["window"].(string)
		if !ok || window != canonical.TargetIDs[0] || (window != models.NewsWindowToday && window != models.NewsWindowWeek && window != models.NewsWindowMonth) {
			return false, nil, map[string]any{"error_class": "invalid_registered_arguments"}, nil
		}
		var beforeSnapshot models.NewsSnapshot
		beforeFound := db.Where("tenant_id=? AND \"window\"=?", tenantID, window).First(&beforeSnapshot).Error == nil
		before := snapshotOperatorState(beforeSnapshot, beforeFound)
		count, err := buildNewsSnapshot(db, tenantID, window)
		if err != nil {
			return false, before, map[string]any{"error_class": "snapshot_refresh_failed"}, nil
		}
		var afterSnapshot models.NewsSnapshot
		afterFound := db.Where("tenant_id=? AND \"window\"=?", tenantID, window).First(&afterSnapshot).Error == nil
		after := snapshotOperatorState(afterSnapshot, afterFound)
		verified := map[string]any{"exists": afterFound, "clean": afterFound && !afterSnapshot.Dirty, "slide_count_matches": afterFound && afterSnapshot.SlideCount == count}
		return afterFound && !afterSnapshot.Dirty && afterSnapshot.SlideCount == count, before, after, verified
	case "feed_integrity.suppress_episode.1h", "feed_integrity.revoke_suppression", "real_experience.suppress_incident.1h", "real_experience.revoke_suppression":
		return executeOperatorDomainSafetyPlan(ctx, db, tenantID, toolKey, plan, canonical)
	case "media_circulation.supply.disable_evaluator":
		return executeOperatorMediaSupplyEvaluatorDisablePlan(ctx, db, tenantID, plan, canonical)
	case supply.SupplyActionRepairMissedAdmission, supply.SupplyActionReclaimDispatchClaim, supply.SupplyActionTransferUnitLease,
		supply.SupplyActionAdoptUnitJob, supply.SupplyActionRedeliverReceipt, supply.SupplyActionVerifyEffect,
		supply.SupplyActionFinalizeVerifiedNoChange, supply.SupplyActionCancelUnstarted, supply.SupplyActionPipelineResumeExactStage,
		supply.SupplyActionArtifactRequestTranscript, supply.SupplyActionArtifactRequestImageEmbedding,
		supply.SupplyActionArtifactRequestTextEmbedding, supply.SupplyActionArtifactRequestLLMMetadata,
		supply.SupplyActionAtomizationExecuteExactParent, supply.SupplyActionFeedGenerationAttachVerifiedMember:
		if len(canonical.TargetIDs) != 1 || len(canonical.NormalizedArguments) != 1 {
			return false, nil, map[string]any{"error_class": "invalid_supply_episode"}, nil
		}
		episodeID, err := uuid.Parse(canonical.TargetIDs[0])
		argumentEpisode, argumentOK := canonical.NormalizedArguments["episode_id"].(string)
		if err != nil || !argumentOK || argumentEpisode != canonical.TargetIDs[0] {
			return false, nil, map[string]any{"error_class": "invalid_supply_episode"}, nil
		}
		before := map[string]any{"schema_version": "operator-supply-handoff/v1", "episode_id": episodeID, "action_key": toolKey, "native_action_queued": false}
		request, err := queueOperatorMediaSupplyRecovery(db, tenantID, access.UserID, access, toolKey, episodeID)
		if err != nil {
			return false, before, map[string]any{"error_class": "native_supply_handoff_failed"}, nil
		}
		after := map[string]any{"schema_version": "operator-supply-handoff/v1", "episode_id": episodeID, "action_key": toolKey, "native_action_id": request.PublicID, "native_action_state": request.State}
		verified := map[string]any{"signed_native_action_queued": request.State == models.MediaSupplyActionRequestQueued, "native_action_id": request.PublicID, "affected_domains": request.AffectedDomains, "affected_subjects": request.AffectedSubjects, "deep_links": request.DeepLinks}
		return request.State == models.MediaSupplyActionRequestQueued, before, after, verified
	case "feed_integrity.pause.24h", "real_experience.pause.24h", "retention.pause.24h", "ai_economics.pause.24h", "news_circulation.pause.24h", "media_circulation.pause.24h", "redundancy.pause.24h", "pipeline.pause.24h", "enrichment.pause.24h", "embeddings.pause_campaigns.24h", "topics_preferences.pause.24h", "media_library.pause.24h":
		return executeOperatorDomainPausePlan(ctx, db, tenantID, toolKey, canonical)
	case "sources.pause", "sources.resume", "media_sources.pause", "media_sources.resume":
		return executeOperatorSourceStatePlan(ctx, db, tenantID, toolKey, canonical)
	case "sources.run_once", "media_sources.run_once":
		result, before, err := runOperatorSourceOnce(db, tenantID, toolKey, plan, step, canonical)
		if err != nil {
			return false, before, map[string]any{"error_class": "source_run_admission_failed"}, nil
		}
		var request models.SourceRunRequest
		admitted := db.Where("public_id=? AND tenant_id=?", result.RequestID, tenantID).First(&request).Error == nil && request.State == models.SourceRunRequested && request.OperatorPlanID != nil && *request.OperatorPlanID == plan.PublicID && request.OperatorStepID != nil && *request.OperatorStepID == step.PublicID && request.IdempotencyKey == result.IdempotencyKey
		after := map[string]any{"source_run_request_id": result.RequestID, "state": request.State, "admission_created": result.Created}
		verified := map[string]any{"durable_admission": admitted, "queue_acceptance_is_not_completion": true, "plan_correlation": request.OperatorPlanID != nil && *request.OperatorPlanID == plan.PublicID, "step_correlation": request.OperatorStepID != nil && *request.OperatorStepID == step.PublicID, "request_idempotency": request.IdempotencyKey == result.IdempotencyKey}
		return admitted, before, after, verified
	default:
		return false, nil, map[string]any{"error_class": "unregistered_executor"}, nil
	}
}
