package controllers

import (
	"github.com/google/uuid"
	"strings"
	"testing"
	"time"
)

func TestOperatorOpenChatIDsAreBounded(t *testing.T) {
	id := uuid.NewString()
	for _, raw := range []string{"", "bad", uuid.Nil.String(), id + ",", strings.Repeat(id+",", 30) + id} {
		if _, err := operatorSelectedThreadIDs(raw); err == nil {
			t.Fatalf("accepted invalid IDs: %q", raw)
		}
	}
	ids, err := operatorSelectedThreadIDs(" " + id + " ," + id)
	if err != nil || len(ids) != 1 || ids[0].String() != id {
		t.Fatalf("deduplication: %v %v", ids, err)
	}
}

func TestOperatorOpenChatActivityPrecedence(t *testing.T) {
	now := time.Now().UTC()
	rows := []operatorThreadStateCount{
		{ThreadID: 1, State: "failed", Count: 1, UnreadCount: 1, UpdatedAt: now.Add(-time.Hour)},
		{ThreadID: 1, State: "completed", Count: 2, UnreadCount: 1, UpdatedAt: now},
		{ThreadID: 2, State: "running", Count: 1, UpdatedAt: now},
		{ThreadID: 2, State: "accepted", Count: 2, UpdatedAt: now},
		{ThreadID: 3, State: "backgrounded", Count: 1, UpdatedAt: now},
		{ThreadID: 6, State: "cancelled", Count: 1, UpdatedAt: now},
	}
	plans := []operatorThreadStateCount{
		{ThreadID: 2, State: "awaiting_approval", Count: 1, UpdatedAt: now},
		{ThreadID: 3, State: "running", Count: 1, UpdatedAt: now},
		{ThreadID: 4, State: "expired", Count: 1, UpdatedAt: now},
		{ThreadID: 5, State: "verifying", Count: 1, UpdatedAt: now},
	}
	result := operatorThreadActivities(rows, plans)
	if result[1].State != "completed" || result[1].Unread != 2 {
		t.Fatalf("historical failure overrode latest answer: %+v", result[1])
	}
	if result[2].State != "needs_approval" || result[2].ActiveInvestigations != 3 {
		t.Fatalf("approval priority/read count: %+v", result[2])
	}
	if result[3].State != "working" || result[3].ActiveInvestigations != 1 {
		t.Fatalf("plan counted as investigation: %+v", result[3])
	}
	if result[4].State != "needs_review" || result[5].State != "working" || result[5].ActiveInvestigations != 0 || result[6].State != "cancelled" {
		t.Fatalf("plan states: %+v", result)
	}
}
