package economics

import (
	"database/sql"
	"fmt"
	"gorm.io/gorm"
	"sync"
)

var lifecycleKeys = struct {
	sync.Mutex
	values map[*sql.DB][]byte
}{values: map[*sql.DB][]byte{}}

// A database-owned random key survives JWT rotations. It is never returned by report APIs.
func lifecycleKey(db *gorm.DB) ([]byte, error) {
	pool, e := db.DB()
	if e != nil {
		return nil, e
	}
	lifecycleKeys.Lock()
	defer lifecycleKeys.Unlock()
	if key := lifecycleKeys.values[pool]; len(key) == 32 {
		return key, nil
	}
	var row struct{ Key []byte }
	if e := db.Table("economics_lifecycle_keys").Select("key").Where("singleton=true").Take(&row).Error; e != nil {
		return nil, e
	}
	if len(row.Key) != 32 {
		return nil, fmt.Errorf("economics lifecycle key unavailable")
	}
	lifecycleKeys.values[pool] = row.Key
	return row.Key, nil
}
