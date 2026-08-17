package operator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const conversationRetention = 30 * 24 * time.Hour

// ConversationStore owns only deletable Operator conversation material.
// Action-plan records live in PlanStore and are deliberately not reachable
// through this store or the conversation foreign-key graph.
type ConversationStore struct {
	db  *gorm.DB
	now func() time.Time
}

func NewConversationStore(db *gorm.DB) *ConversationStore {
	return &ConversationStore{db: db, now: func() time.Time { return time.Now().UTC() }}
}

func (store *ConversationStore) CreateThread(ctx context.Context, tenantID, creatorID, title, locale string) (models.OperatorThread, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(creatorID) == "" || (locale != "ar" && locale != "en") {
		return models.OperatorThread{}, fmt.Errorf("%w: invalid conversation thread identity", ErrInvalidContract)
	}
	now := store.now()
	thread := models.OperatorThread{
		TenantID: tenantID, CreatorID: creatorID, Title: strings.TrimSpace(title), Locale: locale,
		LastActivityAt: now, ExpiresAt: now.Add(conversationRetention),
	}
	if err := store.db.WithContext(ctx).Create(&thread).Error; err != nil {
		return models.OperatorThread{}, err
	}
	return thread, nil
}

// AppendMessage refreshes the retention window inside the same transaction as
// the message write. Tenant and creator constraints prevent cross-tenant or
// delegated writes from reviving another admin's conversation.
func (store *ConversationStore) AppendMessage(ctx context.Context, threadID uint, tenantID, creatorID, actorType, actorID string, content map[string]any) (models.OperatorMessage, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(creatorID) == "" || (actorType != "admin" && actorType != "operator" && actorType != "system") {
		return models.OperatorMessage{}, fmt.Errorf("%w: invalid conversation message identity", ErrInvalidContract)
	}
	raw, err := json.Marshal(content)
	if err != nil {
		return models.OperatorMessage{}, fmt.Errorf("%w: encode conversation content", ErrInvalidContract)
	}
	now := store.now()
	message := models.OperatorMessage{ThreadID: threadID, TenantID: tenantID, ActorType: actorType, ActorID: actorID, MessageKind: "note", Content: datatypes.JSON(raw)}
	err = store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var thread models.OperatorThread
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=? AND tenant_id=? AND creator_id=?", threadID, tenantID, creatorID).First(&thread).Error; err != nil {
			return err
		}
		if !now.Before(thread.ExpiresAt) {
			return fmt.Errorf("%w: conversation thread has expired", ErrInvalidContract)
		}
		if err := tx.Create(&message).Error; err != nil {
			return err
		}
		return tx.Model(&thread).Updates(map[string]any{"last_activity_at": now, "expires_at": now.Add(conversationRetention)}).Error
	})
	return message, err
}

// DeleteThreadContent deletes conversation rows and their related
// investigations/evidence, but cannot cascade to action plans, approvals, or
// their immutable events. The caller must own the same explicit tenant.
func (store *ConversationStore) DeleteThreadContent(ctx context.Context, publicID uuid.UUID, tenantID, creatorID string) error {
	if publicID == uuid.Nil || strings.TrimSpace(tenantID) == "" || strings.TrimSpace(creatorID) == "" {
		return fmt.Errorf("%w: invalid conversation deletion identity", ErrInvalidContract)
	}
	return store.deleteThreadContent(ctx, publicID, tenantID, creatorID)
}

func (store *ConversationStore) deleteThreadContent(ctx context.Context, publicID uuid.UUID, tenantID, creatorID string) error {
	return store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var thread models.OperatorThread
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=? AND tenant_id=? AND creator_id=?", publicID, tenantID, creatorID).First(&thread).Error; err != nil {
			return err
		}
		if _, err := deleteInvestigations(tx, tx.Model(&models.OperatorInvestigation{}).Where("thread_id=? AND tenant_id=?", thread.ID, tenantID)); err != nil {
			return err
		}
		if err := tx.Where("thread_id=? AND tenant_id=?", thread.ID, tenantID).Delete(&models.OperatorMessage{}).Error; err != nil {
			return err
		}
		return tx.Delete(&thread).Error
	})
}

type ConversationCleanupResult struct {
	ThreadsDeleted        int64
	InvestigationsDeleted int64
}

// SweepExpiredConversationContent implements the 30-day retention policy. It
// is intentionally an internal CMS lifecycle operation, not a registered
// Operator tool. Orphaned expired investigations are also removed; database
// foreign keys remove their evidence/event snapshots only.
func (store *ConversationStore) SweepExpiredConversationContent(ctx context.Context, limit int) (ConversationCleanupResult, error) {
	if limit < 1 || limit > 500 {
		return ConversationCleanupResult{}, fmt.Errorf("%w: cleanup limit must be 1..500", ErrInvalidContract)
	}
	now := store.now()
	var threads []models.OperatorThread
	if err := store.db.WithContext(ctx).Where("expires_at < ?", now).Order("expires_at ASC").Limit(limit).Find(&threads).Error; err != nil {
		return ConversationCleanupResult{}, err
	}
	result := ConversationCleanupResult{}
	for _, thread := range threads {
		if err := store.deleteThreadContent(ctx, thread.PublicID, thread.TenantID, thread.CreatorID); err != nil && err != gorm.ErrRecordNotFound {
			return result, err
		}
		result.ThreadsDeleted++
	}
	err := store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		deleted, err := deleteInvestigations(tx, tx.Model(&models.OperatorInvestigation{}).Where("thread_id IS NULL AND expires_at < ?", now).Limit(limit))
		result.InvestigationsDeleted = deleted
		return err
	})
	if err != nil {
		return result, err
	}
	return result, nil
}

// deleteInvestigations removes only time-bounded investigation material. It
// is intentionally explicit instead of relying on database cascades so the
// non-cascading audit boundary holds in every supported database environment.
func deleteInvestigations(tx *gorm.DB, query *gorm.DB) (int64, error) {
	var ids []uint
	if err := query.Select("id").Find(&ids).Error; err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	if err := tx.Where("investigation_id IN ?", ids).Delete(&models.OperatorEvidence{}).Error; err != nil {
		return 0, err
	}
	if err := tx.Where("investigation_id IN ?", ids).Delete(&models.OperatorInvestigationEvent{}).Error; err != nil {
		return 0, err
	}
	deleted := tx.Where("id IN ?", ids).Delete(&models.OperatorInvestigation{})
	return deleted.RowsAffected, deleted.Error
}
