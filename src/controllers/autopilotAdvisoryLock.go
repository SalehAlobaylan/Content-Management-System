package controllers

import (
	"context"
	"hash/fnv"
	"strings"

	"gorm.io/gorm"
)

// tryAcquireTenantAutopilotLock is the common replica-safe run guard for
// short CMS-owned Autopilot passes. The PostgreSQL session owns the advisory
// lock, so process exit or connection loss releases it without a stale in-memory
// flag becoming authority.
func tryAcquireTenantAutopilotLock(db *gorm.DB, family, tenantID string) (func(), bool) {
	if db == nil || strings.TrimSpace(family) == "" || strings.TrimSpace(tenantID) == "" {
		return func() {}, false
	}
	sqlDB, err := db.DB()
	if err != nil {
		return func() {}, false
	}
	conn, err := sqlDB.Conn(context.Background())
	if err != nil {
		return func() {}, false
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte("wahb:" + family + "/v1/" + tenantID))
	key := int64(h.Sum64())
	var acquired bool
	if err := conn.QueryRowContext(context.Background(), "SELECT pg_try_advisory_lock($1)", key).Scan(&acquired); err != nil || !acquired {
		_ = conn.Close()
		return func() {}, false
	}
	return func() {
		_, _ = conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", key)
		_ = conn.Close()
	}, true
}
