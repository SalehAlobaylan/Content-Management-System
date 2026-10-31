package controllers

import (
	"content-management-system/src/models"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

func operatorTaskStates(group string) []string {
	switch group {
	case "active":
		return []string{"accepted", "backgrounded", "running", "queued", "claimed", "verifying", "active"}
	case "needs_approval":
		return []string{"awaiting_approval"}
	case "failed":
		return []string{"failed", "blocked", "paused"}
	case "completed":
		return []string{"completed", "done", "succeeded", "cancelled"}
	}
	return nil
}

// Bound each kind before merging it. Filters and the complete cursor ordering
// must run in SQL so busy histories cannot hide older matching work.
func operatorTaskQuery(db *gorm.DB, table, kind, state, group string, after *operatorCursor, limit int) *gorm.DB {
	if state != "" {
		db = db.Where(table+".state=?", state)
	}
	if states := operatorTaskStates(group); len(states) > 0 {
		db = db.Where(table+".state IN ?", states)
	}
	if after != nil {
		equal := ""
		args := []any{after.Time}
		if kind < after.Kind {
			equal = " OR " + table + ".updated_at=?"
			args = append(args, after.Time)
		}
		if kind == after.Kind {
			equal = " OR (" + table + ".updated_at=? AND " + table + ".public_id::text<?)"
			args = append(args, after.Time, after.ID)
		}
		db = db.Where("("+table+".updated_at<?"+equal+")", args...)
	}
	return db.Order(table + ".updated_at DESC, " + table + ".public_id DESC").Limit(limit + 1)
}

func ListOperatorTasks(c *gin.Context) {
	principal, tenant, db, ok := operatorGovernancePrincipal(c)
	if !ok {
		return
	}
	_, policy, err := operatorExecutionPolicy(db, tenant)
	if err != nil || !policy.ReadEnabled {
		c.Status(http.StatusNotFound)
		return
	}
	db = db.WithContext(c.Request.Context())
	fail := func(err error) {
		_ = err
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Operator activity is unavailable. Try again.", "code": "operator_activity_unavailable"})
	}
	limit := 50
	if raw := c.Query("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > 100 {
			c.JSON(400, gin.H{"message": "Invalid Operator task limit"})
			return
		}
	}
	kind, group, state := strings.TrimSpace(c.Query("kind")), strings.TrimSpace(c.Query("group")), strings.TrimSpace(c.Query("state"))
	if kind == "" {
		kind = "all"
	}
	if !map[string]bool{"all": true, "investigation": true, "plan": true, "schedule": true, "schedule_run": true}[kind] || !map[string]bool{"": true, "all": true, "active": true, "needs_approval": true, "failed": true, "completed": true}[group] {
		c.JSON(400, gin.H{"message": "Invalid Operator task filter"})
		return
	}
	var after *operatorCursor
	if raw := c.Query("cursor"); raw != "" {
		value, e := decodeOperatorCursor(raw)
		if e != nil || !map[string]bool{"investigation": true, "plan": true, "schedule": true, "schedule_run": true}[value.Kind] {
			c.JSON(400, gin.H{"message": "Invalid Operator task cursor"})
			return
		}
		if _, e := uuid.Parse(value.ID); e != nil {
			c.JSON(400, gin.H{"message": "Invalid Operator task cursor"})
			return
		}
		after = &value
	}
	var threadID *uint
	if raw := c.Query("thread_id"); raw != "" {
		id, e := uuid.Parse(raw)
		if e != nil {
			c.JSON(400, gin.H{"message": "Invalid Operator chat"})
			return
		}
		var thread models.OperatorThread
		result := db.Where("public_id=? AND tenant_id=? AND creator_id=? AND expires_at>?", id, tenant, principal.UserID, time.Now().UTC()).Limit(1).Find(&thread)
		if result.Error != nil {
			fail(result.Error)
			return
		}
		if result.RowsAffected == 0 {
			c.JSON(404, gin.H{"message": "Operator chat not found"})
			return
		}
		threadID = &thread.ID
	}
	tasks := make([]operatorTaskItem, 0)
	appendTask := func(id uuid.UUID, updated time.Time, kind string, item gin.H, investigationID *uint) error {
		tasks = append(tasks, operatorTaskItem{updated: updated, id: id.String(), kind: kind, value: item, investigationID: investigationID})
		return nil
	}
	if kind == "all" || kind == "investigation" {
		query := db.Model(&models.OperatorInvestigation{}).Where("tenant_id=? AND actor_id=?", tenant, principal.UserID)
		if threadID != nil {
			query = query.Where("thread_id=?", *threadID)
		}
		var rows []models.OperatorInvestigation
		if err := operatorTaskQuery(query, "operator_investigations", "investigation", state, group, after, limit).Find(&rows).Error; err != nil {
			fail(err)
			return
		}
		for _, row := range rows {
			item := gin.H{"id": row.PublicID, "kind": "investigation", "title": operatorTaskTitle(row), "domain": operatorTaskDomain(row), "state": row.State, "started_at": row.StartedAt, "updated_at": row.UpdatedAt, "unread": row.ReadAt == nil && (row.State == "completed" || row.State == "failed" || row.State == "cancelled"), "can_cancel": row.State == "accepted" || row.State == "backgrounded", "error_class": row.ErrorClass, "deep_link": "/platform/operator?investigation=" + row.PublicID.String()}
			if err := appendTask(row.PublicID, row.UpdatedAt, "investigation", item, &row.ID); err != nil {
				fail(err)
				return
			}
		}
	}
	if kind == "all" || kind == "plan" {
		query := db.Model(&models.OperatorActionPlan{}).Where("operator_action_plans.tenant_id=? AND operator_action_plans.actor_id=?", tenant, principal.UserID)
		if threadID != nil {
			query = query.Joins("JOIN operator_investigations i ON i.id=operator_action_plans.investigation_id AND i.tenant_id=operator_action_plans.tenant_id AND i.actor_id=operator_action_plans.actor_id").Where("i.thread_id=?", *threadID)
		}
		var rows []models.OperatorActionPlan
		if err := operatorTaskQuery(query, "operator_action_plans", "plan", state, group, after, limit).Find(&rows).Error; err != nil {
			fail(err)
			return
		}
		for _, row := range rows {
			item := gin.H{"id": row.PublicID, "kind": "plan", "title": row.ToolKey, "state": row.State, "risk_tier": row.RiskTier, "started_at": row.CreatedAt, "updated_at": row.UpdatedAt, "expires_at": row.ExpiresAt, "can_cancel": row.State == "awaiting_approval" || row.State == "queued", "plan_id": row.PublicID, "deep_link": "/platform/operator?plan=" + row.PublicID.String()}
			if err := appendTask(row.PublicID, row.UpdatedAt, "plan", item, row.InvestigationID); err != nil {
				fail(err)
				return
			}
		}
	}
	if threadID == nil && (kind == "all" || kind == "schedule") {
		query := db.Model(&models.OperatorSchedule{}).Where("tenant_id=? AND (creator_id=? OR owner_id=?)", tenant, principal.UserID, principal.UserID)
		var rows []models.OperatorSchedule
		if err := operatorTaskQuery(query, "operator_schedules", "schedule", state, group, after, limit).Find(&rows).Error; err != nil {
			fail(err)
			return
		}
		for _, row := range rows {
			item := gin.H{"id": row.PublicID, "kind": "schedule", "title": row.Cadence, "domain": "operator", "state": row.State, "started_at": row.CreatedAt, "updated_at": row.UpdatedAt, "unread": false, "can_cancel": false, "schedule_id": row.PublicID, "deep_link": "/platform/operator?task_kind=schedule&task_scope=all&panel=tasks"}
			if err := appendTask(row.PublicID, row.UpdatedAt, "schedule", item, nil); err != nil {
				fail(err)
				return
			}
		}
	}
	if kind == "all" || kind == "schedule_run" {
		query := db.Model(&models.OperatorScheduleRun{}).Joins("JOIN operator_schedules s ON s.id=operator_schedule_runs.schedule_id AND s.tenant_id=operator_schedule_runs.tenant_id").Where("operator_schedule_runs.tenant_id=? AND (s.creator_id=? OR s.owner_id=?)", tenant, principal.UserID, principal.UserID)
		if threadID != nil {
			query = query.Joins("JOIN operator_investigations i ON i.id=operator_schedule_runs.result_investigation_id AND i.tenant_id=operator_schedule_runs.tenant_id").Where("i.thread_id=? AND i.actor_id=?", *threadID, principal.UserID)
		}
		var rows []models.OperatorScheduleRun
		if err := operatorTaskQuery(query, "operator_schedule_runs", "schedule_run", state, group, after, limit).Find(&rows).Error; err != nil {
			fail(err)
			return
		}
		for _, row := range rows {
			item := gin.H{"id": row.PublicID, "kind": "schedule_run", "title": row.State, "domain": "operator", "state": row.State, "started_at": row.StartedAt, "updated_at": row.UpdatedAt, "unread": false, "can_cancel": false, "deep_link": "/platform/operator?task_kind=schedule_run&task_scope=all&panel=tasks"}
			if err := appendTask(row.PublicID, row.UpdatedAt, "schedule_run", item, row.ResultInvestigationID); err != nil {
				fail(err)
				return
			}
		}
	}
	// Two bounded projections attach canonical investigation/chat identities to
	// every kind; never perform one remote database round-trip per history row.
	ids := []uint{}
	for _, item := range tasks {
		if item.investigationID != nil {
			ids = append(ids, *item.investigationID)
		}
	}
	linked := map[uint]models.OperatorInvestigation{}
	chats := map[uint]uuid.UUID{}
	if len(ids) > 0 {
		var rows []models.OperatorInvestigation
		if err := db.Select("id,public_id,thread_id,visible_context").Where("id IN ? AND tenant_id=? AND actor_id=?", ids, tenant, principal.UserID).Find(&rows).Error; err != nil {
			fail(err)
			return
		}
		threadIDs := []uint{}
		for _, row := range rows {
			linked[row.ID] = row
			if row.ThreadID != nil {
				threadIDs = append(threadIDs, *row.ThreadID)
			}
		}
		if len(threadIDs) > 0 {
			var threads []models.OperatorThread
			if err := db.Select("id,public_id").Where("id IN ? AND tenant_id=? AND creator_id=?", threadIDs, tenant, principal.UserID).Find(&threads).Error; err != nil {
				fail(err)
				return
			}
			for _, thread := range threads {
				chats[thread.ID] = thread.PublicID
			}
		}
	}
	for i := range tasks {
		item := &tasks[i]
		params := []string{}
		if item.investigationID != nil {
			if inv, ok := linked[*item.investigationID]; ok {
				if inv.ThreadID != nil {
					if chat, ok := chats[*inv.ThreadID]; ok {
						item.value["thread_id"] = chat
						params = append(params, "thread="+chat.String())
					}
				}
				item.value["investigation_id"] = inv.PublicID
				item.value["domain"] = operatorTaskDomain(inv)
				params = append(params, "investigation="+inv.PublicID.String())
			}
		}
		if item.kind == "plan" {
			params = append(params, "plan="+item.id, "panel=plan")
		}
		if len(params) > 0 {
			item.value["deep_link"] = "/platform/operator?" + strings.Join(params, "&")
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
	next := ""
	if len(tasks) > limit {
		tasks = tasks[:limit]
		last := tasks[len(tasks)-1]
		next = encodeOperatorCursor(operatorCursor{Time: last.updated, ID: last.id, Kind: last.kind})
	}
	items := make([]gin.H, 0, len(tasks))
	for _, task := range tasks {
		items = append(items, task.value)
	}
	c.JSON(200, gin.H{"items": items, "next_cursor": next})
}
