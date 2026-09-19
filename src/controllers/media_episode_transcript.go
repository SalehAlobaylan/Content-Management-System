package controllers

import (
	"content-management-system/src/models"

	"gorm.io/gorm"
)

// Transcripts inherit tenant ownership from their content item; the transcript
// table has no tenant_id. Check both sides of the current episode association so
// a stale or foreign transcript pointer cannot expose text or govern a draft.
func episodeTranscriptQuery(db *gorm.DB, parent models.ContentItem) *gorm.DB {
	return db.Model(&models.Transcript{}).
		Where("transcripts.public_id=? AND transcripts.content_item_id=?", parent.TranscriptID, parent.PublicID).
		Where(`EXISTS (SELECT 1 FROM content_items AS owner
			WHERE owner.public_id=transcripts.content_item_id
			AND owner.tenant_id=? AND owner.transcript_id=transcripts.public_id)`, parent.TenantID)
}
