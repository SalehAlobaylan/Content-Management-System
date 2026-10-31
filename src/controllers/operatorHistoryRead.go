package controllers

import (
	"content-management-system/src/models"
	"errors"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"sort"
)

var errOperatorMessageCursor = errors.New("invalid Operator history cursor")

// Return the newest page in display order. Older pages use immutable public-ID
// and creation-time boundaries; never replace recent messages with the oldest 200.
func operatorThreadMessagePage(db *gorm.DB, thread models.OperatorThread, raw string) ([]models.OperatorMessage, string, error) {
	query := db.Where("thread_id=? AND tenant_id=?", thread.ID, thread.TenantID)
	if raw != "" {
		cursor, err := decodeOperatorCursor(raw)
		if err != nil {
			return nil, "", errOperatorMessageCursor
		}
		if _, err := uuid.Parse(cursor.ID); err != nil {
			return nil, "", errOperatorMessageCursor
		}
		query = query.Where("created_at < ? OR (created_at = ? AND public_id::text < ?)", cursor.Time, cursor.Time, cursor.ID)
	}
	var rows []models.OperatorMessage
	if err := query.Order("created_at DESC, public_id DESC").Limit(101).Find(&rows).Error; err != nil {
		return nil, "", err
	}
	before := ""
	if len(rows) > 100 {
		rows = rows[:100]
		last := rows[len(rows)-1]
		before = encodeOperatorCursor(operatorCursor{Time: last.CreatedAt, ID: last.PublicID.String()})
	}
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}
	return rows, before, nil
}

// Include an explicitly opened question alongside recent history. This is a
// bounded owner read, not an arbitrary investigation lookup or a read receipt.
func operatorIncludeFocusedMessages(db *gorm.DB, thread models.OperatorThread, raw string, rows []models.OperatorMessage) ([]models.OperatorMessage, error) {
	id, err := uuid.Parse(raw)
	if err != nil {
		return nil, errOperatorMessageCursor
	}
	var investigation models.OperatorInvestigation
	if err := db.Where("public_id=? AND thread_id=? AND tenant_id=? AND actor_id=?", id, thread.ID, thread.TenantID, thread.CreatorID).First(&investigation).Error; err != nil {
		return nil, err
	}
	var focused []models.OperatorMessage
	if err := db.Where("thread_id=? AND tenant_id=? AND investigation_id=?", thread.ID, thread.TenantID, investigation.ID).Order("created_at ASC,public_id ASC").Limit(100).Find(&focused).Error; err != nil {
		return nil, err
	}
	seen := map[uuid.UUID]bool{}
	for _, row := range rows {
		seen[row.PublicID] = true
	}
	for _, row := range focused {
		if !seen[row.PublicID] {
			rows = append(rows, row)
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].CreatedAt.Equal(rows[j].CreatedAt) {
			return rows[i].PublicID.String() < rows[j].PublicID.String()
		}
		return rows[i].CreatedAt.Before(rows[j].CreatedAt)
	})
	return rows, nil
}
