package controllers

import (
	"content-management-system/src/models"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestNewsDeleteReviewRejectsEqualCountReplacementAndNewRevision(t *testing.T) {
	first, second := uuid.New(), uuid.New()
	stamp := time.Now().UTC()
	expected := []newsDeleteTarget{{first, stamp}}
	for _, test := range []struct {
		name string
		id   uuid.UUID
		at   time.Time
		want bool
	}{
		{"same review", first, stamp, true},
		{"same count but different item", second, stamp, false},
		{"same item but edited", first, stamp.Add(time.Second), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			actual := []models.ContentItem{{PublicID: test.id, UpdatedAt: test.at}}
			if newsDeleteTargetsMatch(actual, expected) != test.want {
				t.Fatal("incorrect review matching")
			}
		})
	}
}
