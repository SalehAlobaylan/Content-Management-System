package controllers

import (
	"content-management-system/src/models"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"strings"
	"time"
)

func operatorSelectedThreadIDs(raw string) ([]uuid.UUID, error) {
	parts := strings.Split(raw, ",")
	if len(parts) == 0 || len(parts) > 30 {
		return nil, fmt.Errorf("invalid open chat count")
	}
	ids := make([]uuid.UUID, 0, len(parts))
	seen := map[uuid.UUID]bool{}
	for _, part := range parts {
		id, err := uuid.Parse(strings.TrimSpace(part))
		if err != nil || id == uuid.Nil {
			return nil, fmt.Errorf("invalid open chat ID")
		}
		if !seen[id] {
			ids = append(ids, id)
			seen[id] = true
		}
	}
	return ids, nil
}

type operatorThreadStateCount struct {
	ThreadID    uint
	State       string
	Count       int64
	UnreadCount int64
	UpdatedAt   time.Time
}
type operatorThreadActivity struct {
	State                string
	ActiveInvestigations int64
	Unread               int64
	priority             int
	terminalAt           time.Time
}

func operatorThreadActivities(investigations, plans []operatorThreadStateCount) map[uint]operatorThreadActivity {
	result := map[uint]operatorThreadActivity{}
	consume := func(row operatorThreadStateCount, investigation bool) {
		item := result[row.ThreadID]
		state, priority := "idle", 0
		switch row.State {
		case "accepted", "backgrounded", "queued":
			state, priority = "queued", 2
		case "running", "claimed", "verifying":
			state, priority = "working", 3
		case "awaiting_approval":
			state, priority = "needs_approval", 4
		case "failed":
			state = "failed"
		case "blocked", "expired":
			state = "needs_review"
		case "completed", "succeeded":
			state = "completed"
		case "cancelled":
			state = "cancelled"
		}
		if investigation {
			item.Unread += row.UnreadCount
			if priority > 0 {
				item.ActiveInvestigations += row.Count
			}
		}
		if priority > item.priority {
			item.State, item.priority = state, priority
		}
		if priority == 0 && item.priority == 0 && !row.UpdatedAt.Before(item.terminalAt) {
			item.State, item.terminalAt = state, row.UpdatedAt
		}
		result[row.ThreadID] = item
	}
	for _, row := range investigations {
		consume(row, true)
	}
	for _, row := range plans {
		consume(row, false)
	}
	return result
}

// One bounded owner-scoped summary read for the open chats. No transcripts,
// plan arguments, runtime changes, or implicit read acknowledgements.
func operatorOpenThreadSummaries(db *gorm.DB, tenant, actor string, threads []models.OperatorThread, now time.Time) ([]gin.H, error) {
	items := make([]gin.H, 0, len(threads))
	if len(threads) == 0 {
		return items, nil
	}
	ids := make([]uint, 0, len(threads))
	for _, thread := range threads {
		ids = append(ids, thread.ID)
	}
	var investigations, plans []operatorThreadStateCount
	if err := db.Model(&models.OperatorInvestigation{}).
		Select("thread_id, state, COUNT(*) AS count, SUM(CASE WHEN read_at IS NULL AND state IN ('completed','failed','cancelled') THEN 1 ELSE 0 END) AS unread_count, MAX(updated_at) AS updated_at").
		Where("tenant_id=? AND actor_id=? AND thread_id IN ?", tenant, actor, ids).
		Group("thread_id,state").Scan(&investigations).Error; err != nil {
		return nil, err
	}
	if err := db.Table("operator_action_plans AS p").
		Joins("JOIN operator_investigations AS i ON i.id=p.investigation_id AND i.tenant_id=p.tenant_id AND i.actor_id=p.actor_id").
		Select("i.thread_id, CASE WHEN p.state='awaiting_approval' AND p.expires_at<=? THEN 'expired' ELSE p.state END AS state, COUNT(*) AS count, MAX(p.updated_at) AS updated_at", now).
		Where("p.tenant_id=? AND p.actor_id=? AND i.thread_id IN ?", tenant, actor, ids).
		Group("1,2").Scan(&plans).Error; err != nil {
		return nil, err
	}
	activities := operatorThreadActivities(investigations, plans)
	for _, thread := range threads {
		item := operatorThreadResponse(thread)
		activity := activities[thread.ID]
		if activity.State == "" {
			activity.State = "idle"
		}
		item["activity_state"], item["active_investigation_count"], item["unread_count"] = activity.State, activity.ActiveInvestigations, activity.Unread
		items = append(items, item)
	}
	return items, nil
}
