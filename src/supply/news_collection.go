package supply

import (
	"content-management-system/src/models"
	"fmt"
	"gorm.io/gorm"
	"strings"
)

// All News admission/dispatch transactions share this lock with the explicit
// pause owner. Receipt retention and verification deliberately never take it.
func LockNewsCollection(db *gorm.DB, tenant string, exclusive bool) error {
	if db == nil || strings.TrimSpace(tenant) == "" {
		return fmt.Errorf("news collection requires tenant")
	}
	fn := "pg_advisory_xact_lock_shared"
	if exclusive {
		fn = "pg_advisory_xact_lock"
	}
	return db.Exec("SELECT "+fn+"(hashtextextended(?,0))", "news-collection/v1/"+tenant).Error
}

func NewsCollectionState(db *gorm.DB, tenant string) (models.NewsCollectionControl, error) {
	state := models.NewsCollectionControl{TenantID: tenant}
	err := db.Where("tenant_id=?", tenant).First(&state).Error
	if err == gorm.ErrRecordNotFound {
		return state, nil
	}
	return state, err
}

func MayPullNews(db *gorm.DB, tenant string) (bool, error) {
	if err := LockNewsCollection(db, tenant, false); err != nil {
		return false, err
	}
	state, err := NewsCollectionState(db, tenant)
	return !state.Paused, err
}

// Older environments can still serve Pods before this additive migration is
// explicitly applied. News fails closed until its pause contract is available.
// alias is a code-owned SQL alias, never supplied by an API request.
func newsIntakePredicate(db *gorm.DB, alias string) string {
	if !db.Migrator().HasTable(&models.NewsCollectionControl{}) {
		return alias + ".category <> 'news'"
	}
	return fmt.Sprintf("(%s.category <> 'news' OR NOT EXISTS (SELECT 1 FROM news_collection_controls nc WHERE nc.tenant_id=%s.tenant_id AND nc.paused=TRUE))", alias, alias)
}
