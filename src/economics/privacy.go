package economics

import (
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Account/usage SQL must not become an uncontrolled second analytics archive in
// slow-query logs. Bounded run/error/coverage evidence remains admin-visible.
func privateDB(db *gorm.DB) *gorm.DB { return db.Session(&gorm.Session{Logger: logger.Discard}) }
