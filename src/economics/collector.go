package economics

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type usageKey struct {
	Tenant, Subject, Operation, Unit, Variant string
	Day                                       int64
}
type Collector struct {
	flushMu      sync.Mutex
	mu           sync.Mutex
	values       map[usageKey]int64
	pending      map[string]*Batch
	sequences    map[string]int64
	epochs       map[string]string
	retries      map[string]int
	pendingSince map[string]time.Time
	epoch        string
	lost         int64
	bytes        int
	enabled      atomic.Value
	references   atomic.Value
}

func NewCollector() *Collector {
	c := &Collector{values: map[usageKey]int64{}, pending: map[string]*Batch{}, sequences: map[string]int64{}, epochs: map[string]string{}, retries: map[string]int{}, pendingSince: map[string]time.Time{}, epoch: uuid.NewString()}
	c.enabled.Store(map[string]bool{})
	c.references.Store(map[string]bool{})
	return c
}

var DefaultCollector = NewCollector()

func (c *Collector) Enabled(tenant string) bool { return c.enabled.Load().(map[string]bool)[tenant] }
func (c *Collector) SetPolicies(p []Policy) {
	m := map[string]bool{}
	refs := map[string]bool{}
	for _, v := range p {
		if v.CollectionEnabled || v.ReportsEnabled {
			refs[v.TenantID] = true
		}
		if v.CollectionEnabled {
			m[v.TenantID] = true
		}
	}
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	for key := range c.values {
		if !m[key.Tenant] || time.Unix(key.Day, 0).Before(time.Now().UTC().AddDate(0, 0, -90).Truncate(24*time.Hour)) {
			delete(c.values, key)
			c.lost++
			c.bytes -= 160 + len(key.Tenant) + len(key.Subject) + len(key.Operation) + len(key.Unit) + len(key.Variant)
		}
	}
	for tenant, b := range c.pending {
		if !m[tenant] {
			c.discardPending(tenant, b)
		}
	}
	if c.bytes < 0 {
		c.bytes = 0
	}
	c.enabled.Store(m)
	c.references.Store(refs)
}
func (c *Collector) Observe(tenant, subject, operation, unit, variant string, value int64, at time.Time) {
	if !c.Enabled(tenant) || value <= 0 {
		return
	}
	if !ValidSubject(subject) || len(tenant) > 64 || len(variant) > 32 || Operations[operation] == "" || !Units[unit] {
		return
	}
	key := usageKey{tenant, subject, operation, unit, variant, at.UTC().Truncate(24 * time.Hour).Unix()}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Enabled(tenant) {
		c.addLocked(key, value)
	}
}

// addLocked shares capacity accounting between owner observations and request batches.
func (c *Collector) addLocked(key usageKey, value int64) {
	if _, ok := c.values[key]; !ok {
		cost := 160 + len(key.Tenant) + len(key.Subject) + len(key.Operation) + len(key.Unit) + len(key.Variant)
		if len(c.values)+len(c.pending)*MaxRows >= MaxKeys || c.bytes+cost > 16*1024*1024 {
			c.lost++
			return
		}
		c.bytes += cost
	}
	if c.values[key] > 1e15-value {
		c.lost++
		return
	}
	c.values[key] += value
}
func (c *Collector) seal(tenant string) *Batch {
	c.mu.Lock()
	defer c.mu.Unlock()
	if b := c.pending[tenant]; b != nil {
		return b
	}
	now := time.Now().UTC()
	b := &Batch{Version: 1, TenantID: tenant, Environment: EnvironmentName(), Epoch: c.currentEpoch(tenant), Sequence: c.sequences[tenant] + 1, StartedAt: now, EndedAt: now, Rows: []Usage{}}
	for k, value := range c.values {
		if k.Tenant != tenant {
			continue
		}
		day := time.Unix(k.Day, 0).UTC()
		b.Rows = append(b.Rows, Usage{Day: day, Subject: k.Subject, Operation: k.Operation, Unit: k.Unit, Variant: k.Variant, Value: value})
		if day.Before(b.StartedAt) {
			b.StartedAt = day
		}
		delete(c.values, k)
		if len(b.Rows) == MaxRows {
			break
		}
	}
	if len(b.Rows) == 0 {
		b.Health = true
	}
	b.Lost = c.lost
	c.lost = 0
	c.pending[tenant] = b
	c.pendingSince[tenant] = now
	return b
}
func (c *Collector) Flush(db *gorm.DB) error {
	if !c.flushMu.TryLock() {
		return nil
	}
	defer c.flushMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	deadline := time.Now().Add(5 * time.Second)
	for tenant, enabled := range c.enabled.Load().(map[string]bool) {
		if !enabled {
			continue
		}
		for time.Now().Before(deadline) {
			b := c.seal(tenant)
			if b == nil {
				break
			}
			if e := Ingest(db.WithContext(ctx), "cms", *b); e != nil {
				c.mu.Lock()
				c.retries[tenant]++
				if c.retries[tenant] >= 10 || time.Since(c.pendingSince[tenant]) >= 10*time.Minute {
					c.discardPending(tenant, b)
				}
				c.mu.Unlock()
				return e
			}
			c.mu.Lock()
			delete(c.pending, tenant)
			delete(c.retries, tenant)
			delete(c.pendingSince, tenant)
			c.sequences[tenant] = b.Sequence
			for _, row := range b.Rows {
				c.bytes -= 160 + len(tenant) + len(row.Subject) + len(row.Operation) + len(row.Unit) + len(row.Variant)
			}
			if c.bytes < 0 {
				c.bytes = 0
			}
			c.mu.Unlock()
			if b.Health && len(b.Rows) == 0 {
				break
			}
		}
	}
	return nil
}
func OperationForRoute(route string) string {
	switch {
	case strings.Contains(route, "/feed/pods"):
		return "pods_page"
	case strings.Contains(route, "/feed/news"):
		return "news_page"
	case strings.Contains(route, "/search"):
		return "search"
	case strings.Contains(route, "/transcribe"):
		return "transcription_request"
	case strings.Contains(route, "/transcript"):
		return "transcript_read"
	case strings.Contains(route, "/submissions"):
		return "upload"
	case strings.Contains(route, "/interactions"):
		return "interaction"
	case strings.Contains(route, "/content"):
		return "content_read"
	default:
		return "unknown"
	}
}
func (c *Collector) Middleware() gin.HandlerFunc {
	// The public tenant is boot-time infrastructure configuration.
	tenantID := PublicTenant()
	return func(ctx *gin.Context) {
		ctx.Next()
		if len(c.enabled.Load().(map[string]bool)) == 0 {
			return
		}
		if !strings.HasPrefix(ctx.Request.URL.Path, "/api/v1/") {
			return
		}
		claimed, _ := ctx.Get("tenant_id")
		claimedTenant, _ := claimed.(string)
		if !c.Enabled(tenantID) {
			return
		}
		subject := "public"
		if claimedTenant != "" && claimedTenant != tenantID {
			subject = "unknown"
		}
		if raw, ok := ctx.Get("user_id"); ok && claimedTenant == tenantID {
			if value, ok := raw.(string); ok {
				if id, e := uuid.Parse(value); e == nil {
					// Write a canonical UUID and prefix into one allocation.
					const digits = "0123456789abcdef"
					var account [44]byte
					copy(account[:], "account:")
					n := 8
					for i, b := range id {
						if i == 4 || i == 6 || i == 8 || i == 10 {
							account[n] = '-'
							n++
						}
						account[n], account[n+1] = digits[b>>4], digits[b&15]
						n += 2
					}
					subject = string(account[:])
				}
			}
		}
		variant := "ok"
		if ctx.Writer.Status() >= 400 {
			variant = "error"
		}
		key := usageKey{tenantID, subject, OperationForRoute(ctx.FullPath()), "requests", variant, time.Now().UTC().Truncate(24 * time.Hour).Unix()}
		c.mu.Lock()
		// A policy pause may have purged counters while this request was running.
		if c.Enabled(tenantID) {
			c.addLocked(key, 1)
			if size := ctx.Writer.Size(); size > 0 {
				key.Unit = "response_bytes"
				c.addLocked(key, int64(size))
			}
			if variant == "error" {
				key.Unit = "errors"
				c.addLocked(key, 1)
			}
		}
		c.mu.Unlock()
	}
}
func Start(ctx context.Context, db *gorm.DB) {
	db = privateDB(db)
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()
	var policies []Policy
	refresh := func() {
		if !Available(db) {
			return
		}
		var ps []Policy
		if db.Limit(100).Find(&ps).Error == nil {
			policies = ps
			DefaultCollector.SetPolicies(ps)
		}
	}
	refresh()
	if Available(db) {
		_ = SyncLifecycle(db)
	}
	ticks := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			refresh()
			_ = DefaultCollector.Flush(db)
			ticks++
			active, imports := false, false
			for _, policy := range policies {
				active = active || policy.CollectionEnabled || policy.ReportsEnabled
				imports = imports || policy.ImportsEnabled
			}
			if active || ticks%5 == 0 || !LifecycleReady(db) {
				_ = SyncLifecycle(db)
			}
			if imports {
				ImportDue(db)
			}
			if ticks%5 != 0 {
				continue
			}
			var ps []Policy
			if Available(db) && db.Limit(100).Find(&ps).Error == nil {
				for _, p := range ps {
					if !p.CollectionEnabled && !p.ReportsEnabled && p.CollectionSince == nil && p.UpdatedBy == "" {
						continue
					}
					// Privacy retention runs even when operators pause reporting/collection.
					_ = db.Transaction(func(tx *gorm.DB) error {
						if e := Lock(tx); e != nil {
							return e
						}
						return Retention(tx, p.TenantID, time.Now().UTC())
					})
					if p.ReportsEnabled {
						_ = Rebuild(db, p.TenantID)
					}
				}
			}
		}
	}
}

func (c *Collector) Close(db *gorm.DB) error {
	if e := c.Flush(db); e != nil {
		return e
	}
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	for tenant, enabled := range c.enabled.Load().(map[string]bool) {
		if !enabled {
			continue
		}
		c.mu.Lock()
		if len(c.pending) > 0 {
			c.mu.Unlock()
			continue
		}
		sequence := c.sequences[tenant] + 1
		c.mu.Unlock()
		now := time.Now().UTC()
		b := Batch{Version: 1, Health: true, Closed: true, TenantID: tenant, Environment: EnvironmentName(), Epoch: c.currentEpoch(tenant), Sequence: sequence, StartedAt: now, EndedAt: now, Rows: []Usage{}}
		if e := Ingest(db, "cms", b); e != nil {
			return e
		}
	}
	return nil
}

func (c *Collector) currentEpoch(tenant string) string {
	if value := c.epochs[tenant]; value != "" {
		return value
	}
	return c.epoch
}

// Caller holds mu and flushMu. An unacknowledged discard is uncertain delivery,
// never an exact count of lost requests. The old epoch remains visibly unclosed.
func (c *Collector) discardPending(tenant string, b *Batch) {
	c.lost += int64(len(b.Rows))
	for _, r := range b.Rows {
		c.bytes -= 160 + len(tenant) + len(r.Subject) + len(r.Operation) + len(r.Unit) + len(r.Variant)
	}
	delete(c.pending, tenant)
	delete(c.retries, tenant)
	delete(c.pendingSince, tenant)
	c.sequences[tenant] = 0
	c.epochs[tenant] = uuid.NewString()
	if c.bytes < 0 {
		c.bytes = 0
	}
}

func (c *Collector) CapturesReferences(tenant string) bool {
	return c.references.Load().(map[string]bool)[tenant]
}
