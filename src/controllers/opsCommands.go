package controllers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/utils"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

const opsCommandFreshness = 90 * time.Second
const opsCommandDeadline = 30 * time.Second

// The preview is a read, not a persisted workflow. Execution locks and rereads
// these targets and compares their digest before changing a pause field.
type opsCommandScope struct {
	Command         string `json:"command"`
	MemberKey       string `json:"member_key,omitempty"`
	LaneKey         string `json:"lane_key,omitempty"`
	TenantID        string `json:"tenant_id,omitempty"`
	SourceCommandID string `json:"source_command_id,omitempty"`
}
type opsCommandReview struct {
	SnapshotAt   time.Time `json:"snapshot_at"`
	TargetDigest string    `json:"target_digest"`
}
type opsPauseRequest struct {
	opsCommandScope
	opsCommandReview
	Reason         string `json:"reason"`
	TTLMinutes     int    `json:"ttl_minutes"`
	IdempotencyKey string `json:"idempotency_key"`
}
type opsResumeRequest struct {
	opsCommandReview
	SourceCommandID string `json:"source_command_id"`
	IdempotencyKey  string `json:"idempotency_key"`
}
type opsPauseTarget struct {
	Member       opsFleetMember                `json:"-"`
	Lane         opsFleetLane                  `json:"-"`
	MemberKey    string                        `json:"member_key"`
	LaneKey      string                        `json:"lane_key"`
	TenantID     string                        `json:"tenant_id"`
	Prior        *time.Time                    `json:"prior_paused_until,omitempty"`
	State        string                        `json:"state"`
	Enabled      bool                          `json:"enabled"`
	Mode         string                        `json:"mode,omitempty"`
	Exists       bool                          `json:"exists"`
	SourceAction *models.OpsFleetCommandAction `json:"-"`
}

func opsPauseValuesMatch(current, written *time.Time) bool {
	return current != nil && written != nil && current.Equal(*written)
}
func opsTargetKey(t opsPauseTarget) string {
	return t.Member.Key + "\x00" + t.Lane.Key + "\x00" + t.TenantID
}
func opsSortTargets(targets []opsPauseTarget) {
	sort.Slice(targets, func(i, j int) bool {
		// Containment takes its own policy lock before any sibling policy.
		// Match that order, then use stable lane/tenant ordering within siblings.
		if (targets[i].Member.Key == "system_health") != (targets[j].Member.Key == "system_health") {
			return targets[i].Member.Key == "system_health"
		}
		return opsTargetKey(targets[i]) < opsTargetKey(targets[j])
	})
}
func opsTargetsDigest(scope opsCommandScope, targets []opsPauseTarget) string {
	parts := []string{scope.Command, scope.MemberKey, scope.LaneKey, scope.TenantID, scope.SourceCommandID}
	for _, t := range targets {
		parts = append(parts, opsTargetKey(t), opsTimeFingerprint(t.Prior), fmt.Sprint(t.Exists), fmt.Sprint(t.Enabled), t.Mode)
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return hex.EncodeToString(sum[:])
}
func opsValidateReview(review opsCommandReview, now time.Time) error {
	if review.SnapshotAt.IsZero() || len(review.TargetDigest) != 64 || now.Sub(review.SnapshotAt) > opsCommandFreshness || review.SnapshotAt.After(now.Add(5*time.Second)) {
		return fmt.Errorf("review expired or missing; refresh the command preview")
	}
	return nil
}

// Query identities before taking any locks, with a hard cap. Unconfigured
// lanes are represented explicitly so the command records its skipped scope.
func opsPauseTargets(db *gorm.DB, scope opsCommandScope) ([]opsPauseTarget, error) {
	targets := []opsPauseTarget{}
	for _, member := range opsFleetRegistry {
		for _, lane := range member.Lanes {
			if lane.PauseColumn == "" {
				continue
			}
			if scope.Command != "pause_all" && (member.Key != scope.MemberKey || lane.Key != scope.LaneKey) {
				continue
			}
			query := fmt.Sprintf("SELECT %s AS tenant_id, %s AS paused_until FROM %s", lane.TenantColumn, lane.PauseColumn, lane.Table)
			args := []any{}
			if scope.TenantID != "" {
				query += fmt.Sprintf(" WHERE %s=?", lane.TenantColumn)
				args = append(args, scope.TenantID)
			}
			query += fmt.Sprintf(" ORDER BY %s LIMIT %d", lane.TenantColumn, opsCommandMaxTargets+1)
			var rows []struct {
				TenantID    string
				PausedUntil *time.Time
			}
			if err := db.Raw(query, args...).Scan(&rows).Error; err != nil {
				return targets, fmt.Errorf("%s/%s target read unavailable: %w", member.Key, lane.Key, err)
			}
			if len(rows) == 0 {
				targets = append(targets, opsPauseTarget{Member: member, Lane: lane, MemberKey: member.Key, LaneKey: lane.Key, TenantID: scope.TenantID, State: "unconfigured"})
			}
			for _, row := range rows {
				targets = append(targets, opsPauseTarget{Member: member, Lane: lane, MemberKey: member.Key, LaneKey: lane.Key, TenantID: row.TenantID, Prior: row.PausedUntil, Exists: true})
			}
			if len(targets) > opsCommandMaxTargets {
				return targets[:opsCommandMaxTargets], fmt.Errorf("fleet command exceeds the %d-target safety cap", opsCommandMaxTargets)
			}
		}
	}
	if len(targets) == 0 {
		return targets, fmt.Errorf("member lane is not pausable")
	}
	opsSortTargets(targets)
	return targets, nil
}
func opsResumeTargets(db *gorm.DB, tenant, id string) ([]opsPauseTarget, *models.OpsFleetCommand, error) {
	sourceID, err := uuid.Parse(id)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid source_command_id")
	}
	var source models.OpsFleetCommand
	if err = db.Where("tenant_id=? AND public_id=?", tenant, sourceID).First(&source).Error; err != nil {
		return nil, nil, fmt.Errorf("source pause command unavailable: %w", err)
	}
	if source.Command != "pause_member" && source.Command != "pause_all" {
		return nil, nil, fmt.Errorf("source command is not a pause")
	}
	var actions []models.OpsFleetCommandAction
	if err = db.Where("command_id=? AND outcome='paused'", source.ID).Order("member_key,lane_key,tenant_id").Limit(opsCommandMaxTargets + 1).Find(&actions).Error; err != nil {
		return nil, nil, err
	}
	if len(actions) > opsCommandMaxTargets {
		return nil, nil, fmt.Errorf("source command exceeds target cap")
	}
	targets := make([]opsPauseTarget, 0, len(actions))
	for i := range actions {
		action := &actions[i]
		member, lane, ok := opsFindLane(action.MemberKey, action.LaneKey)
		if !ok || lane.PauseColumn == "" {
			return targets, nil, fmt.Errorf("source action references unknown pause lane")
		}
		target := opsPauseTarget{Member: member, Lane: lane, MemberKey: member.Key, LaneKey: lane.Key, TenantID: action.TenantID, SourceAction: action}
		if err := opsReadTarget(db, &target, false); err != nil {
			return targets, nil, err
		}
		targets = append(targets, target)
	}
	opsSortTargets(targets)
	return targets, &source, nil
}
func opsReadTarget(db *gorm.DB, target *opsPauseTarget, lock bool) error {
	query := fmt.Sprintf("SELECT %s AS paused_until FROM %s WHERE %s=?", target.Lane.PauseColumn, target.Lane.Table, target.Lane.TenantColumn)
	if lock {
		query += " FOR UPDATE"
	}
	var rows []struct{ PausedUntil *time.Time }
	if err := db.Raw(query, target.TenantID).Scan(&rows).Error; err != nil {
		return err
	}
	target.Exists = len(rows) > 0
	target.Prior = nil
	if target.Exists {
		target.Prior = rows[0].PausedUntil
	} else {
		target.State = "unconfigured"
	}
	return nil
}
func opsCheckTargetStatus(db *gorm.DB, targets []opsPauseTarget, now time.Time) error {
	byLane := map[string][]opsMemberStatus{}
	for i := range targets {
		t := &targets[i]
		if !t.Exists {
			continue
		}
		key := t.Member.Key + ":" + t.Lane.Key
		statuses, ok := byLane[key]
		if !ok {
			statuses = opsStatusForLane(db, t.Member, t.Lane, now)
			byLane[key] = statuses
		}
		found := false
		for _, status := range statuses {
			if status.Error != "" || status.State == "errored" {
				return fmt.Errorf("%s status is unreadable; use its owning cockpit", key)
			}
			if status.TenantID == t.TenantID {
				if status.State == "unknown" || status.State == "unconfigured" {
					return fmt.Errorf("%s target evidence is incomplete", key)
				}
				t.State, t.Enabled, t.Mode = status.State, status.Enabled, status.Mode
				found = true
			}
		}
		if !found {
			return fmt.Errorf("%s target evidence is incomplete", key)
		}
	}
	return nil
}
func opsPreviewTargets(db *gorm.DB, principal utils.AdminPrincipal, scope opsCommandScope) ([]opsPauseTarget, *models.OpsFleetCommand, error) {
	if scope.Command == "resume" {
		return opsResumeTargets(db, principal.TenantID, scope.SourceCommandID)
	}
	if scope.Command != "pause_all" && scope.Command != "pause_member" {
		return nil, nil, fmt.Errorf("unsupported command")
	}
	if scope.Command == "pause_all" && scope.TenantID != "" {
		return nil, nil, fmt.Errorf("fleet pause cannot select a tenant")
	}
	targets, err := opsPauseTargets(db, scope)
	return targets, nil, err
}
func PreviewOpsCommand(c *gin.Context) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(401, gin.H{"error": "unauthorized"})
		return
	}
	var scope opsCommandScope
	if c.ShouldBindJSON(&scope) != nil {
		c.JSON(400, gin.H{"error": "invalid command scope"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), opsReadDeadline)
	defer cancel()
	db := c.MustGet("db").(*gorm.DB).WithContext(ctx)
	targets, _, err := opsPreviewTargets(db, principal, scope)
	if err == nil {
		err = opsCheckTargetStatus(db, targets, time.Now().UTC())
	}
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	now := time.Now().UTC()
	configured := 0
	for _, t := range targets {
		if t.Exists {
			configured++
		}
	}
	c.JSON(200, gin.H{"data": gin.H{"snapshot_at": now, "target_digest": opsTargetsDigest(scope, targets), "targets": targets, "target_count": configured, "scope": scope, "expires_at": now.Add(opsCommandFreshness), "excluded": []string{"Services and queues", "In-flight work", "Workload protection", "Ranking Intelligence", "Storage operations", "Atomization execution", "Pods pipeline (use its own Pause/Resume controls)", "System Health observation (only containment is pausable)"}}})
}

func opsWriteAudit(tx *gorm.DB, principal utils.AdminPrincipal, action, resource, status string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return tx.Create(&models.AuditLog{TenantID: principal.TenantID, UserID: principal.UserID, UserEmail: principal.Email, Action: action, TargetService: "cms", TargetResource: resource, Status: status, Payload: datatypes.JSON(raw)}).Error
}
func opsWriteTarget(tx *gorm.DB, target opsPauseTarget, value *time.Time, now time.Time) (*time.Time, error) {
	// RETURNING captures the policy column's actual PostgreSQL precision/type.
	// Legacy pause columns are timestamp WITHOUT time zone, interpreted as UTC.
	// Ledger timestamptz values may decode in the host's local zone; normalize
	// before pgx drops that zone when binding a legacy timestamp parameter.
	if value != nil {
		utc := value.UTC()
		value = &utc
	}
	query := fmt.Sprintf("UPDATE %s SET %s=?, updated_at=? WHERE %s=? RETURNING %s AS paused_until", target.Lane.Table, target.Lane.PauseColumn, target.Lane.TenantColumn, target.Lane.PauseColumn)
	var rows []struct{ PausedUntil *time.Time }
	if err := tx.Raw(query, value, now, target.TenantID).Scan(&rows).Error; err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, fmt.Errorf("target disappeared during command")
	}
	return rows[0].PausedUntil, nil
}

// A single transaction advisory lock serializes these short human commands.
// It also makes idempotent retries wait for, and return, the original result.
// Owning systems continue to use their ordinary policy-row locks.
func opsExecuteCommand(db *gorm.DB, principal utils.AdminPrincipal, scope opsCommandScope, review opsCommandReview, reason string, ttl int, idempotency string) (models.OpsFleetCommand, error) {
	response := models.OpsFleetCommand{}
	var interruptedEnvelope *models.OpsFleetCommand
	var frozenTargets []opsPauseTarget
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("SET LOCAL lock_timeout = '3s'").Error; err != nil {
			return err
		}
		if err := tx.Exec("SET LOCAL statement_timeout = '5s'").Error; err != nil {
			return err
		}
		if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtext('ops_command_center'))").Error; err != nil {
			return err
		}
		var existing models.OpsFleetCommand
		if err := tx.Where("tenant_id=? AND idempotency_key=?", principal.TenantID, idempotency).First(&existing).Error; err == nil {
			response = existing
			return nil
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if err := opsValidateReview(review, time.Now().UTC()); err != nil {
			return err
		}
		commandScope := "all"
		if scope.Command == "pause_member" {
			commandScope = scope.MemberKey + ":" + scope.LaneKey
		}
		var source *models.OpsFleetCommand
		if scope.Command == "resume" {
			sourceID, err := uuid.Parse(scope.SourceCommandID)
			if err != nil {
				return fmt.Errorf("invalid source_command_id")
			}
			source = &models.OpsFleetCommand{}
			if err := tx.Where("tenant_id=? AND public_id=?", principal.TenantID, sourceID).First(source).Error; err != nil {
				return err
			}
			if source.Command != "pause_member" && source.Command != "pause_all" {
				return fmt.Errorf("source command is not a pause")
			}
			commandScope = source.Scope
		}
		row := models.OpsFleetCommand{TenantID: principal.TenantID, Command: scope.Command, Scope: commandScope, Reason: reason, IdempotencyKey: idempotency, ActorID: principal.UserID, ActorEmail: principal.Email, Status: "succeeded", Counts: datatypes.JSON([]byte(`{}`))}
		if scope.Command != "resume" {
			row.TTLMinutes = &ttl
		} else if source != nil {
			row.SourceCommandID = &source.PublicID
		}
		// Target-query failures are rolled back below; insert the envelope
		// before running database operations that may abort a statement.
		if err := tx.Create(&row).Error; err != nil {
			return err
		}
		interruptedEnvelope = &row
		if err := tx.SavePoint("ops_effects").Error; err != nil {
			return err
		}
		counts := map[string]int{}
		attempted := []models.OpsFleetCommandAction{}
		var targets []opsPauseTarget
		runErr := func() error {
			// Per-member commands must also serialize with containment before
			// locking sibling rows; this lock never enrolls or changes a policy.
			if err := tx.Exec("SELECT scope FROM system_autopilot_policies WHERE scope='platform' FOR UPDATE").Error; err != nil {
				return err
			}
			var err error
			targets, _, err = opsPreviewTargets(tx, principal, scope)
			frozenTargets = targets
			if err != nil {
				return err
			}
			for i := range targets {
				if targets[i].Exists {
					if err := opsReadTarget(tx, &targets[i], true); err != nil {
						return err
					}
				}
			}
			if err := opsCheckTargetStatus(tx, targets, time.Now().UTC()); err != nil {
				return err
			}
			if opsTargetsDigest(scope, targets) != review.TargetDigest {
				return fmt.Errorf("targets changed after review; refresh the command preview")
			}
			if err := opsValidateReview(review, time.Now().UTC()); err != nil {
				return err
			}
			if err := tx.Statement.Context.Err(); err != nil {
				return err
			}
			now := time.Now().UTC()
			until := now.Add(time.Duration(ttl) * time.Minute)
			for _, target := range targets {
				action := models.OpsFleetCommandAction{CommandID: row.ID, MemberKey: target.Member.Key, LaneKey: target.Lane.Key, TenantID: target.TenantID, PriorPausedUntil: target.Prior}
				if target.SourceAction != nil {
					action.SourceActionID = &target.SourceAction.ID
				}
				if !target.Exists {
					action.Outcome = "skipped"
					action.Guardrail = "unconfigured"
					action.Reason = "No configured policy row"
				} else if scope.Command == "resume" {
					action.SourceActionID = &target.SourceAction.ID
					if !opsPauseValuesMatch(target.Prior, target.SourceAction.WrittenPausedUntil) {
						action.Outcome = "skipped"
						action.Guardrail = "foreign_pause"
						action.Reason = "Pause changed after source command"
					} else {
						restore := target.SourceAction.PriorPausedUntil
						if restore != nil && !restore.After(now) {
							restore = nil
						}
						canonical, err := opsWriteTarget(tx, target, restore, now)
						if err != nil {
							return err
						}
						action.WrittenPausedUntil = canonical
						action.Outcome = "restored"
						if restore == nil {
							action.Outcome = "cleared"
						}
					}
				} else if target.Prior != nil && !target.Prior.Before(until) {
					action.Outcome = "skipped"
					action.Guardrail = "longer_existing_pause"
					action.Reason = "Existing pause is at least as long"
				} else {
					canonical, err := opsWriteTarget(tx, target, &until, now)
					if err != nil {
						return err
					}
					action.WrittenPausedUntil = canonical
					action.Outcome = "paused"
				}
				attempted = append(attempted, action)
				if err := tx.Create(&action).Error; err != nil {
					return err
				}
				counts[action.Outcome]++
			}
			return nil
		}()
		if runErr != nil {
			if err := tx.RollbackTo("ops_effects").Error; err != nil {
				return err
			}
			counts = map[string]int{}
			row.Status = "failed"
			if len(targets) == 0 {
				targets = []opsPauseTarget{{Member: opsFleetMember{Key: scope.MemberKey}, Lane: opsFleetLane{Key: scope.LaneKey}, TenantID: scope.TenantID}}
			}
			// Every frozen target is accounted for even if the first effect
			// failed. Successful attempts become rolled_back, remaining failed.
			done := map[string]models.OpsFleetCommandAction{}
			for _, a := range attempted {
				done[a.MemberKey+"\x00"+a.LaneKey+"\x00"+a.TenantID] = a
			}
			for _, target := range targets {
				a, ok := done[opsTargetKey(target)]
				if !ok {
					a = models.OpsFleetCommandAction{CommandID: row.ID, MemberKey: target.Member.Key, LaneKey: target.Lane.Key, TenantID: target.TenantID, PriorPausedUntil: target.Prior, Outcome: "failed"}
				} else if a.Outcome != "skipped" {
					a.Outcome = "rolled_back"
				}
				a.ID = 0
				a.PublicID = uuid.Nil
				a.Guardrail = "command_rolled_back"
				a.Reason = runErr.Error()
				a.CreatedAt = time.Time{}
				a.UpdatedAt = time.Time{}
				if err := tx.Create(&a).Error; err != nil {
					return err
				}
				counts[a.Outcome]++
			}
		} else if counts["skipped"] > 0 {
			row.Status = "partial"
		}
		raw, _ := json.Marshal(counts)
		row.Counts = datatypes.JSON(raw)
		if err := tx.Save(&row).Error; err != nil {
			return err
		}
		auditStatus := "success"
		if row.Status == "failed" {
			auditStatus = "failure"
		}
		payload := gin.H{"command_id": row.PublicID, "reason": reason, "counts": counts, "status": row.Status}
		if runErr != nil {
			payload["error"] = runErr.Error()
		}
		if err := opsWriteAudit(tx, principal, "ops."+scope.Command, row.Scope, auditStatus, payload); err != nil {
			return err
		}
		response = row
		return nil
	})
	// Request cancellation rolls back the entire outer transaction. Preserve a
	// bounded failure receipt on an independent connection after that rollback;
	// it never replays effects. A concurrent retry may already own this key.
	if err != nil && interruptedEnvelope != nil && db.Statement.Context.Err() != nil {
		return opsRecordInterruptedCommand(db, principal, *interruptedEnvelope, frozenTargets)
	}
	return response, err
}

func opsRecordInterruptedCommand(db *gorm.DB, principal utils.AdminPrincipal, row models.OpsFleetCommand, targets []opsPauseTarget) (models.OpsFleetCommand, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if e := tx.Exec("SET LOCAL lock_timeout = '2s'").Error; e != nil {
			return e
		}
		if e := tx.Exec("SELECT pg_advisory_xact_lock(hashtext('ops_command_center'))").Error; e != nil {
			return e
		}
		var existing models.OpsFleetCommand
		if e := tx.Where("tenant_id=? AND idempotency_key=?", row.TenantID, row.IdempotencyKey).First(&existing).Error; e == nil {
			row = existing
			return nil
		} else if !errors.Is(e, gorm.ErrRecordNotFound) {
			return e
		}
		row.ID = 0
		row.PublicID = uuid.Nil
		row.CreatedAt = time.Time{}
		row.UpdatedAt = time.Time{}
		row.Status = "failed"
		if len(targets) == 0 {
			targets = []opsPauseTarget{{Member: opsFleetMember{Key: "fleet"}, Lane: opsFleetLane{Key: "preflight"}, TenantID: principal.TenantID}}
		}
		raw, _ := json.Marshal(map[string]int{"failed": len(targets)})
		row.Counts = datatypes.JSON(raw)
		if e := tx.Create(&row).Error; e != nil {
			return e
		}
		actions := make([]models.OpsFleetCommandAction, 0, len(targets))
		for _, target := range targets {
			actions = append(actions, models.OpsFleetCommandAction{CommandID: row.ID, MemberKey: target.Member.Key, LaneKey: target.Lane.Key, TenantID: target.TenantID, PriorPausedUntil: target.Prior, Outcome: "failed", Guardrail: "request_interrupted", Reason: "Request cancelled or command deadline exceeded; transaction rolled back without replay"})
		}
		if e := tx.Create(&actions).Error; e != nil {
			return e
		}
		return opsWriteAudit(tx, principal, "ops."+row.Command, row.Scope, "failure", gin.H{"command_id": row.PublicID, "reason": "request_interrupted", "counts": row.Counts})
	})
	return row, err
}
func runOpsPause(c *gin.Context, all bool) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(401, gin.H{"error": "unauthorized"})
		return
	}
	var req opsPauseRequest
	if c.ShouldBindJSON(&req) != nil || strings.TrimSpace(req.Reason) == "" || len(req.Reason) > 1000 || req.TTLMinutes < 1 || req.TTLMinutes > opsCommandMaxTTLMinutes || strings.TrimSpace(req.IdempotencyKey) == "" || len(req.IdempotencyKey) > 128 {
		c.JSON(400, gin.H{"error": "reason, ttl_minutes (1..1440), and idempotency_key (max 128) are required"})
		return
	}
	req.Command = "pause_member"
	if all {
		req.Command = "pause_all"
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), opsCommandDeadline)
	defer cancel()
	row, err := opsExecuteCommand(c.MustGet("db").(*gorm.DB).WithContext(ctx), principal, req.opsCommandScope, req.opsCommandReview, req.Reason, req.TTLMinutes, req.IdempotencyKey)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"data": row})
}
func PauseOpsMember(c *gin.Context) { runOpsPause(c, false) }
func PauseOpsFleet(c *gin.Context)  { runOpsPause(c, true) }
func ResumeOpsCommand(c *gin.Context) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(401, gin.H{"error": "unauthorized"})
		return
	}
	var req opsResumeRequest
	if c.ShouldBindJSON(&req) != nil || req.SourceCommandID == "" || strings.TrimSpace(req.IdempotencyKey) == "" || len(req.IdempotencyKey) > 128 {
		c.JSON(400, gin.H{"error": "source_command_id and idempotency_key are required"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), opsCommandDeadline)
	defer cancel()
	row, err := opsExecuteCommand(c.MustGet("db").(*gorm.DB).WithContext(ctx), principal, opsCommandScope{Command: "resume", SourceCommandID: req.SourceCommandID}, req.opsCommandReview, "Resume reviewed source pause", 0, req.IdempotencyKey)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"data": row})
}
