package workload

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"content-management-system/src/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func activeState(state string) bool {
	return state == StateAcquired || state == StateRunning || state == StateHandedOff || state == StateUncertain
}

func mutatePermit(db *gorm.DB, id string, fn func(*gorm.DB, *models.WorkloadPermit, Policy, time.Time) error) error {
	if id == "" {
		return nil
	}
	return isolated(db, func(tx *gorm.DB) error {
		if err := lockPolicy(tx, false); err != nil {
			return err
		}
		policy, err := policyIn(tx)
		if err != nil {
			return err
		}
		var p models.WorkloadPermit
		if err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&p, "id=?", id).Error; err != nil {
			return err
		}
		return fn(tx, &p, policy, time.Now().UTC())
	})
}

func begin(tx *gorm.DB, p *models.WorkloadPermit, policy Policy, now time.Time) error {
	if p.State == StateRunning || p.State == StateHandedOff {
		return nil
	}
	// Disabling admission also lets safely expired, never-started native work
	// drain. A human revocation remains authoritative in every mode.
	if p.State == "expired" && p.BeganAt == nil && effectiveMode(policy, now) == ModeOff {
		return nil
	}
	if p.State != StateAcquired || (!p.ExpiresAt.After(now) && effectiveMode(policy, now) != ModeOff) {
		return ErrNotAdmitted
	}
	// A policy tightening after acquisition may invalidate an unstarted effect.
	// Native owners still recheck their own authority immediately before effect.
	if effectiveMode(policy, now) == ModeEnforce && enforced(policy, p.Class) && !p.Bypass && p.PolicyRevision != policy.Revision {
		// Reservations were valid at grant. A new policy with smaller capacities
		// needs a fresh acquisition; no unsafe "grandfathered" queued starts.
		return ErrNotAdmitted
	}
	p.State = StateRunning
	p.BeganAt = &now
	p.UpdatedAt = now
	if err := tx.Model(p).Updates(map[string]any{"state": p.State, "began_at": now, "updated_at": now}).Error; err != nil {
		return err
	}
	return ledger(tx, *p, "began", map[string]any{"owner_kind": p.OwnerKind, "owner_id": p.OwnerID}, "", now)
}

func BeginPermit(db *gorm.DB, id string) error { return mutatePermit(db, id, begin) }

// ownerMutation is optional instrumentation on native receipt transactions.
// It isolates all SQL failures and never prevents receipt or recovery progress.
// Logical Begin denial remains authoritative for a genuinely expired permit.
func ownerMutation(db *gorm.DB, tenant, kind, id string, fn func(*gorm.DB, *models.WorkloadPermit, Policy, time.Time) error) error {
	return mutateOwner(db, tenant, kind, id, false, fn)
}

func mutateOwner(db *gorm.DB, tenant, kind, id string, includeTerminal bool, fn func(*gorm.DB, *models.WorkloadPermit, Policy, time.Time) error) error {
	if tenant == "" || kind == "" || id == "" {
		return ErrInvalid
	}
	err := isolated(db, func(tx *gorm.DB) error {
		if err := lockPolicy(tx, false); err != nil {
			return err
		}
		policy, err := policyIn(tx)
		if err != nil {
			return err
		}
		var permits []models.WorkloadPermit
		query := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND owner_kind=? AND owner_id=?", tenant, kind, id)
		if !includeTerminal {
			query = query.Where("state IN ?", []string{StateAcquired, StateRunning, StateHandedOff, StateUncertain})
		}
		if err = query.Order("created_at DESC, id DESC").Limit(1).Find(&permits).Error; err != nil {
			return err
		}
		now := time.Now().UTC()
		for i := range permits {
			if err = fn(tx, &permits[i], policy, now); err != nil {
				return err
			}
		}
		return nil
	})
	if errors.Is(err, ErrNotAdmitted) || errors.Is(err, ErrInvalid) || errors.Is(err, ErrUnsafeRevoke) {
		return err
	}
	if err != nil {
		recordUnavailable()
	}
	return nil
}

func BeginOwner(db *gorm.DB, tenant, kind, id string) error {
	return mutateOwner(db, tenant, kind, id, true, func(tx *gorm.DB, p *models.WorkloadPermit, policy Policy, now time.Time) error {
		// A source-run permit covers its already-admitted finite native units.
		// Uncertainty in one child must not stop siblings draining under the same
		// native fences; the reservation remains uncertain until owner proof.
		if kind == "source_run" && p.State == StateUncertain && p.BeganAt != nil {
			return nil
		}
		return begin(tx, p, policy, now)
	})
}

func requestMutation(db *gorm.DB, r Request, fn func(*gorm.DB, *models.WorkloadPermit, Policy, time.Time) error) error {
	if r.TenantID == "" {
		return ErrInvalid
	}
	err := isolated(db, func(tx *gorm.DB) error {
		if err := lockPolicy(tx, false); err != nil {
			return err
		}
		policy, err := policyIn(tx)
		if err != nil {
			return err
		}
		var permit models.WorkloadPermit
		if err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND request_key=?", r.TenantID, requestIdentity(r)).First(&permit).Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		} else if err != nil {
			return err
		}
		return fn(tx, &permit, policy, time.Now().UTC())
	})
	if errors.Is(err, ErrNotAdmitted) || errors.Is(err, ErrInvalid) || errors.Is(err, ErrUnsafeRevoke) {
		return err
	}
	if err != nil {
		recordUnavailable()
	}
	return nil
}

func BeginRequest(db *gorm.DB, r Request) error { return requestMutation(db, r, begin) }
func SettleRequest(db *gorm.DB, r Request, actual int, proof string) error {
	return requestMutation(db, r, func(tx *gorm.DB, p *models.WorkloadPermit, _ Policy, now time.Time) error {
		return settle(tx, p, actual, proof, now)
	})
}
func MarkRequestUncertain(db *gorm.DB, r Request, reason string) error {
	return requestMutation(db, r, func(tx *gorm.DB, p *models.WorkloadPermit, _ Policy, now time.Time) error {
		return uncertain(tx, p, reason, now)
	})
}

func RenewPermit(db *gorm.DB, id string) error {
	return mutatePermit(db, id, func(tx *gorm.DB, p *models.WorkloadPermit, policy Policy, now time.Time) error {
		if p.State != StateAcquired && p.State != StateRunning && p.State != StateHandedOff {
			return ErrNotAdmitted
		}
		if p.State == StateAcquired && !p.ExpiresAt.After(now) {
			return ErrNotAdmitted
		}
		p.ExpiresAt = now.Add(time.Duration(policy.LeaseSeconds) * time.Second)
		p.UpdatedAt = now
		if err := tx.Model(p).Updates(map[string]any{"expires_at": p.ExpiresAt, "updated_at": now}).Error; err != nil {
			return err
		}
		return ledger(tx, *p, "renewed", map[string]any{"expires_at": p.ExpiresAt}, "", now)
	})
}

func settle(tx *gorm.DB, p *models.WorkloadPermit, actual int, proof string, now time.Time) error {
	if actual < 0 || actual > 10000 || strings.TrimSpace(proof) == "" || len(proof) > 512 {
		return ErrInvalid
	}
	if !activeState(p.State) {
		return nil
	}
	p.State = "released"
	p.ActualUnits = &actual
	p.ReleasedAt = &now
	p.UpdatedAt = now
	p.SettlementProof = proof
	if err := tx.Model(p).Updates(map[string]any{"state": p.State, "actual_units": actual, "released_at": now, "updated_at": now, "settlement_proof": proof}).Error; err != nil {
		return err
	}
	return ledger(tx, *p, "released", map[string]any{"proof": proof, "estimated_units": p.GrantedUnits, "actual_units": actual}, "", now)
}

// ReleasePermit asserts that a synchronous effect ended (or never began).
// Asynchronous enqueue acknowledgements MUST use HandoffPermit instead.
func ReleasePermit(db *gorm.DB, id string, actual int) error {
	return mutatePermit(db, id, func(tx *gorm.DB, p *models.WorkloadPermit, _ Policy, now time.Time) error {
		return settle(tx, p, actual, "caller_verified_synchronous_completion", now)
	})
}

func SettleOwner(db *gorm.DB, tenant, kind, id string, actual int, proof string) error {
	return ownerMutation(db, tenant, kind, id, func(tx *gorm.DB, p *models.WorkloadPermit, _ Policy, now time.Time) error {
		return settle(tx, p, actual, proof, now)
	})
}

func uncertain(tx *gorm.DB, p *models.WorkloadPermit, reason string, now time.Time) error {
	if strings.TrimSpace(reason) == "" || len(reason) > 512 {
		return ErrInvalid
	}
	if !activeState(p.State) {
		return nil
	}
	p.State = StateUncertain
	p.UpdatedAt = now
	if err := tx.Model(p).Updates(map[string]any{"state": StateUncertain, "updated_at": now}).Error; err != nil {
		return err
	}
	return ledger(tx, *p, "uncertain", map[string]any{"reason": reason}, "", now)
}

func MarkUncertain(db *gorm.DB, id, reason string) error {
	return mutatePermit(db, id, func(tx *gorm.DB, p *models.WorkloadPermit, _ Policy, now time.Time) error {
		return uncertain(tx, p, reason, now)
	})
}

func MarkOwnerUncertain(db *gorm.DB, tenant, kind, id, reason string) error {
	return ownerMutation(db, tenant, kind, id, func(tx *gorm.DB, p *models.WorkloadPermit, _ Policy, now time.Time) error {
		return uncertain(tx, p, reason, now)
	})
}

func HandoffPermit(db *gorm.DB, id, kind, ownerID string) error {
	if strings.TrimSpace(kind) == "" || len(kind) > 64 || strings.TrimSpace(ownerID) == "" || len(ownerID) > 128 {
		return ErrInvalid
	}
	return mutatePermit(db, id, func(tx *gorm.DB, p *models.WorkloadPermit, _ Policy, now time.Time) error {
		if p.State != StateRunning {
			return ErrNotAdmitted
		}
		p.State = StateHandedOff
		p.OwnerKind = kind
		p.OwnerID = ownerID
		p.UpdatedAt = now
		if err := tx.Model(p).Updates(map[string]any{"state": p.State, "owner_kind": kind, "owner_id": ownerID, "updated_at": now}).Error; err != nil {
			return err
		}
		return ledger(tx, *p, "handed_off", map[string]any{"owner_kind": kind, "owner_id": ownerID}, "", now)
	})
}

// HandoffOwner journals a native asynchronous acknowledgement. It does not
// release capacity or claim that the remote effect has completed.
func HandoffOwner(db *gorm.DB, tenant, kind, id string) error {
	return ownerMutation(db, tenant, kind, id, func(tx *gorm.DB, p *models.WorkloadPermit, _ Policy, now time.Time) error {
		if p.State == StateHandedOff || p.State == StateUncertain {
			return nil
		}
		if p.State != StateRunning {
			return ErrNotAdmitted
		}
		p.State = StateHandedOff
		p.UpdatedAt = now
		if err := tx.Model(p).Updates(map[string]any{"state": p.State, "updated_at": now}).Error; err != nil {
			return err
		}
		return ledger(tx, *p, "handed_off", map[string]any{"owner_kind": kind, "owner_id": id}, "", now)
	})
}

func RevokePermit(db *gorm.DB, id, actor, reason string) error {
	if strings.TrimSpace(actor) == "" || len(actor) > 120 || strings.TrimSpace(reason) == "" || len(reason) > 512 {
		return ErrInvalid
	}
	return mutatePermit(db, id, func(tx *gorm.DB, p *models.WorkloadPermit, _ Policy, now time.Time) error {
		if p.State == "revoked" {
			return nil
		}
		if p.State != StateAcquired || p.BeganAt != nil {
			return ErrUnsafeRevoke
		}
		p.State = "revoked"
		p.UpdatedAt = now
		p.ReleasedAt = &now
		p.SettlementProof = "human_revoked_before_start: " + reason
		if err := tx.Model(p).Updates(map[string]any{"state": p.State, "updated_at": now, "released_at": now, "settlement_proof": p.SettlementProof}).Error; err != nil {
			return err
		}
		return ledger(tx, *p, "revoked", map[string]any{"reason": reason}, actor, now)
	})
}

// Maintain expires only effects proven not started. A process crash or missing
// heartbeat cannot prove a remote owner stopped, so begun leases stay reserved.
// All cleanup is bounded; unresolved permits and their ledgers are retained.
func Maintain(db *gorm.DB) error {
	return isolatedFor(db, 8*time.Second, func(tx *gorm.DB) error {
		if err := lockPolicy(tx, false); err != nil {
			return err
		}
		now := time.Now().UTC()
		var expired []models.WorkloadPermit
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Where("state='acquired' AND began_at IS NULL AND expires_at<=?", now).Limit(10).Find(&expired).Error; err != nil {
			return err
		}
		for _, p := range expired {
			p.State = "expired"
			p.ReleasedAt = &now
			p.UpdatedAt = now
			p.SettlementProof = "lease_expired_before_start"
			if err := tx.Model(&p).Updates(map[string]any{"state": p.State, "released_at": now, "updated_at": now, "settlement_proof": p.SettlementProof}).Error; err != nil {
				return err
			}
			if err := ledger(tx, p, "expired", map[string]any{"proof": p.SettlementProof}, "", now); err != nil {
				return err
			}
		}
		// Keep 30 days of terminal admission evidence and 7 days of capacity
		// samples. Never delete a live permit or any decision referring to it.
		queries := []struct {
			query string
			arg   time.Time
		}{
			{`DELETE FROM workload_decisions WHERE id IN (SELECT d.id FROM workload_decisions d WHERE d.created_at<? AND d.event NOT IN ('policy_changed','policy_paused') AND NOT EXISTS (SELECT 1 FROM workload_permits p WHERE p.id::text=d.permit_id AND p.state IN ('acquired','running','handed_off','uncertain')) ORDER BY d.id LIMIT 1000)`, now.Add(-30 * 24 * time.Hour)},
			{`DELETE FROM workload_permits WHERE id IN (SELECT p.id FROM workload_permits p WHERE p.state IN ('released','expired','revoked') AND p.updated_at<? AND NOT EXISTS (SELECT 1 FROM workload_decisions d WHERE d.permit_id=p.id::text) ORDER BY p.updated_at LIMIT 500)`, now.Add(-30 * 24 * time.Hour)},
			{`DELETE FROM workload_capacity_snapshots WHERE id IN (SELECT s.id FROM workload_capacity_snapshots s WHERE s.created_at<? AND EXISTS(SELECT 1 FROM workload_capacity_snapshots newer WHERE newer.resource=s.resource AND newer.id>s.id) ORDER BY s.id LIMIT 1000)`, now.Add(-7 * 24 * time.Hour)},
		}
		for _, q := range queries {
			if err := tx.Exec(q.query, q.arg).Error; err != nil {
				return fmt.Errorf("workload retention: %w", err)
			}
		}
		return nil
	})
}
