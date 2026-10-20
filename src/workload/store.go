package workload

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

var (
	ErrConflict     = errors.New("workload policy or permit changed; refresh before retrying")
	ErrInvalid      = errors.New("invalid workload governor request")
	ErrNotQualified = errors.New("workload class lacks reviewed qualification evidence")
	ErrUnsafeRevoke = errors.New("started or unresolved work requires native terminal proof")
	ErrNotAdmitted  = errors.New("workload permit no longer admits this effect")
)

const requestTimeout = 5 * time.Second

var configured struct {
	sync.RWMutex
	samples  SampleProvider
	spend    SpendProvider
	offCache *disabledPolicyCache
}

func SetSampleProvider(provider SampleProvider) {
	configured.Lock()
	configured.samples = provider
	configured.Unlock()
}
func SetSpendProvider(provider SpendProvider) {
	configured.Lock()
	configured.spend = provider
	configured.Unlock()
}
func NewEngine(db *gorm.DB) *Engine {
	configured.RLock()
	defer configured.RUnlock()
	return &Engine{DB: db, Clock: time.Now, SampleProvider: configured.samples, SpendProvider: configured.spend, offCache: configured.offCache}
}
func (e *Engine) now() time.Time {
	if e.Clock == nil {
		return time.Now().UTC()
	}
	return e.Clock().UTC()
}

func jsonValue(v any) datatypes.JSON { b, _ := json.Marshal(v); return datatypes.JSON(b) }
func hashValue(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func boundedContext(db *gorm.DB, timeout time.Duration) (context.Context, context.CancelFunc) {
	parent := context.Background()
	if db != nil && db.Statement != nil && db.Statement.Context != nil {
		parent = db.Statement.Context
	}
	return context.WithTimeout(parent, timeout)
}

// isolated protects an existing owner transaction with a SAVEPOINT. Recovery
// uses its original context, because the bounded query context may be expired.
// It never leaves a failed optional ledger statement aborting a native receipt.
func isolated(db *gorm.DB, fn func(*gorm.DB) error) (err error) {
	return isolatedFor(db, requestTimeout, fn)
}

// Background observation and review are not on the native admission critical
// path. Their larger bounded budget accommodates the approved remote database.
func isolatedFor(db *gorm.DB, timeout time.Duration, fn func(*gorm.DB) error) (err error) {
	if db == nil {
		return errors.New("governor database unavailable")
	}
	ctx, cancel := boundedContext(db, timeout)
	defer cancel()
	if _, nested := db.Statement.ConnPool.(gorm.TxCommitter); nested {
		name := "wg_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		if err = db.SavePoint(name).Error; err != nil {
			return err
		}
		defer func() {
			if p := recover(); p != nil {
				err = fmt.Errorf("governor panic: %v", p)
			}
			if err != nil {
				if rollbackErr := db.RollbackTo(name).Error; rollbackErr != nil {
					err = fmt.Errorf("%w (savepoint recovery: %v)", err, rollbackErr)
				}
			}
		}()
		return fn(db.WithContext(ctx))
	}
	return db.WithContext(ctx).Transaction(fn)
}

func lockPolicy(tx *gorm.DB, exclusive bool) error {
	function := "pg_advisory_xact_lock_shared"
	if exclusive {
		function = "pg_advisory_xact_lock"
	}
	return tx.Exec("SELECT "+function+"(hashtextextended(?, 0))", "workload-governor/policy/v1").Error
}

func defaultPolicy() Policy {
	limits := make(map[string]ResourceLimit, len(resources))
	for _, r := range resources {
		limits[r.Key] = r.DefaultLimit
	}
	return Policy{Scope: "platform", Mode: ModeOff, Revision: 1, EnforcedClasses: jsonValue([]string{}), ResourceLimits: jsonValue(limits), SnapshotMaxAgeSeconds: 90, RecoverySeconds: 120, LeaseSeconds: 600, StarvationMinutes: 120, ObservationHours: 24, MinClassRequests: 20}
}

func policyIn(tx *gorm.DB) (Policy, error) {
	var p Policy
	err := tx.Where("scope = ?", "platform").First(&p).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return defaultPolicy(), nil
	}
	if err != nil {
		return p, err
	}
	if err = validateStoredPolicy(p); err != nil {
		return p, err
	}
	return p, nil
}

func GetPolicy(db *gorm.DB) (Policy, error) {
	var p Policy
	err := isolated(db, func(tx *gorm.DB) error { var err error; p, err = policyIn(tx); return err })
	return p, err
}

func effectiveMode(p Policy, now time.Time) string {
	if p.PausedUntil != nil && p.PausedUntil.After(now) {
		return ModeOff
	}
	return p.Mode
}
func enforced(p Policy, class string) bool {
	var list []string
	_ = json.Unmarshal(p.EnforcedClasses, &list)
	for _, c := range list {
		if c == class {
			return true
		}
	}
	return false
}
func limitsFor(p Policy) map[string]ResourceLimit {
	var m map[string]ResourceLimit
	_ = json.Unmarshal(p.ResourceLimits, &m)
	return m
}

func validateStoredPolicy(p Policy) error {
	if p.Mode != ModeOff && p.Mode != ModeObserve && p.Mode != ModeEnforce {
		return fmt.Errorf("%w: policy mode", ErrInvalid)
	}
	if p.Revision < 1 || p.SnapshotMaxAgeSeconds < 10 || p.SnapshotMaxAgeSeconds > 300 || p.RecoverySeconds < 10 || p.RecoverySeconds > 3600 || p.LeaseSeconds < 30 || p.LeaseSeconds > 3600 || p.StarvationMinutes < 5 || p.StarvationMinutes > 1440 || p.ObservationHours < 24 || p.MinClassRequests < 20 {
		return fmt.Errorf("%w: policy bounds", ErrInvalid)
	}
	var classes []string
	if err := json.Unmarshal(p.EnforcedClasses, &classes); err != nil {
		return fmt.Errorf("%w: enforced classes", ErrInvalid)
	}
	seen := map[string]bool{}
	for _, key := range classes {
		if _, ok := Class(key); !ok || seen[key] {
			return fmt.Errorf("%w: unknown or duplicate class %s", ErrInvalid, key)
		}
		seen[key] = true
	}
	limits := limitsFor(p)
	if len(limits) != len(resources) {
		return fmt.Errorf("%w: resource limit inventory", ErrInvalid)
	}
	for key, l := range limits {
		r, ok := resourceByKey(key)
		if !ok {
			return fmt.Errorf("%w: resource %s", ErrInvalid, key)
		}
		if r.Kind == "pressure" && (!(l.Soft >= 0) || !(l.Hard > l.Soft) || l.Hard > 1000000 || l.Capacity != 0) {
			return fmt.Errorf("%w: pressure bounds %s", ErrInvalid, key)
		}
		if r.Kind == "slots" && (l.Capacity < 1 || l.Capacity > 1000 || l.Soft != 0 || l.Hard != 0) {
			return fmt.Errorf("%w: slot bounds %s", ErrInvalid, key)
		}
	}
	return nil
}

func requestIdentity(r Request) string {
	if r.RequestKey != "" {
		return r.RequestKey
	}
	return hashValue([]string{r.TenantID, r.System, r.Class, r.RunID, r.OwnerKind, r.OwnerID})
}

func validateRequest(r Request) error {
	fields := []struct {
		value string
		max   int
	}{{r.TenantID, 64}, {r.System, 80}, {r.RunID, 128}, {r.OwnerKind, 64}, {r.OwnerID, 128}}
	for _, f := range fields {
		if strings.TrimSpace(f.value) == "" || len(f.value) > f.max {
			return ErrInvalid
		}
	}
	if r.Units < 1 || r.Units > 10000 || len(r.RequestKey) > 128 {
		return ErrInvalid
	}
	if _, ok := Class(r.Class); !ok {
		return fmt.Errorf("%w: unknown action class", ErrInvalid)
	}
	if r.Origin != OriginAutomation && r.Origin != OriginCampaign && r.Origin != OriginHuman {
		return fmt.Errorf("%w: origin", ErrInvalid)
	}
	return nil
}

var unavailableCount atomic.Uint64
var lastUnavailableLog atomic.Int64

func UnavailableCount() uint64 { return unavailableCount.Load() }
func unavailable(r Request, reason string) Decision {
	recordUnavailable()
	return Decision{GrantedUnits: r.Units, Verdict: "admit", WouldVerdict: "unknown", Reasons: []string{"governor_unavailable", reason}, Mode: ModeOff, Bypass: true, Unavailable: true}
}

func recordUnavailable() {
	unavailableCount.Add(1)
	now := time.Now().Unix()
	prior := lastUnavailableLog.Load()
	if now-prior >= 60 && lastUnavailableLog.CompareAndSwap(prior, now) {
		log.Printf("[workload-governor] optional evidence unavailable; native owner authority retained (process count=%d)", unavailableCount.Load())
	}
}

func decisionFromPermit(p models.WorkloadPermit) Decision {
	var reasons []string
	_ = json.Unmarshal(p.Reasons, &reasons)
	decision := Decision{PermitID: p.ID, GrantedUnits: p.GrantedUnits, Verdict: p.Verdict, WouldVerdict: p.WouldVerdict, Reasons: reasons, Mode: p.Mode, Bypass: p.Bypass, ExpiresAt: &p.ExpiresAt}
	if p.State != StateAcquired && p.State != StateRunning && p.State != StateHandedOff && p.State != StateUncertain {
		decision.Verdict = "blocked"
		decision.GrantedUnits = 0
		decision.Reasons = append(decision.Reasons, "request_already_settled")
	}
	return decision
}

func ledger(tx *gorm.DB, p models.WorkloadPermit, event string, evidence any, actor string, now time.Time) error {
	return tx.Create(&models.WorkloadDecision{PermitID: p.ID, TenantID: p.TenantID, System: p.System, Class: p.Class, Event: event, Verdict: p.Verdict, WouldVerdict: p.WouldVerdict, RequestedUnits: p.RequestedUnits, GrantedUnits: p.GrantedUnits, ActualUnits: p.ActualUnits, Reasons: p.Reasons, Evidence: jsonValue(evidence), Mode: p.Mode, PolicyRevision: p.PolicyRevision, Actor: actor, CreatedAt: now}).Error
}
