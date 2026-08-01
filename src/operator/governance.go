package operator

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const scheduleClaimLease = 90 * time.Second

// ScheduledTemplate is intentionally a read-only subset of an investigation.
// Resolve is rejected, so schedules cannot persist a mutation intent or any
// action approval.
type ScheduledTemplate struct {
	VisibleContext VisibleContext       `json:"visible_context"`
	Request        InvestigationRequest `json:"request"`
	AdapterKeys    []string             `json:"adapter_keys"`
}

func (template ScheduledTemplate) Validate() error {
	if err := template.VisibleContext.Validate(); err != nil || template.Request.Validate() != nil {
		return fmt.Errorf("%w: invalid schedule template", ErrInvalidContract)
	}
	if template.Request.Intent == IntentResolve || !visibleIntentAllowed(template.VisibleContext, template.Request.Intent) {
		return fmt.Errorf("%w: schedules are read-only", ErrForbiddenTool)
	}
	if len(template.AdapterKeys) > 32 {
		return fmt.Errorf("%w: too many schedule adapters", ErrInvalidContract)
	}
	for _, key := range template.AdapterKeys {
		if strings.TrimSpace(key) == "" || len(key) > 160 {
			return fmt.Errorf("%w: invalid schedule adapter", ErrInvalidContract)
		}
	}
	return nil
}

func ScheduleCadence(minutes int) (string, time.Duration, error) {
	if minutes < 15 || minutes > 7*24*60 {
		return "", 0, fmt.Errorf("%w: schedule cadence must be 15 minutes through 7 days", ErrInvalidContract)
	}
	return fmt.Sprintf("%dm", minutes), time.Duration(minutes) * time.Minute, nil
}

func ParseScheduleCadence(cadence string) (time.Duration, error) {
	value := strings.TrimSuffix(strings.TrimSpace(cadence), "m")
	var minutes int
	if _, err := fmt.Sscanf(value, "%d", &minutes); err != nil {
		return 0, fmt.Errorf("%w: invalid schedule cadence", ErrInvalidContract)
	}
	_, duration, err := ScheduleCadence(minutes)
	return duration, err
}

type ScheduleStore struct {
	db  *gorm.DB
	now func() time.Time
}

func NewScheduleStore(db *gorm.DB) *ScheduleStore {
	return &ScheduleStore{db: db, now: func() time.Time { return time.Now().UTC() }}
}

func (store *ScheduleStore) Create(ctx context.Context, access AccessSnapshot, template ScheduledTemplate, requiredPermissions []string, locale string, cadenceMinutes int) (models.OperatorSchedule, error) {
	if err := access.ValidateFor(access.UserID, access.TenantID); err != nil || !access.IsAdmin {
		return models.OperatorSchedule{}, ErrAccessUnavailable
	}
	if err := template.Validate(); err != nil {
		return models.OperatorSchedule{}, err
	}
	if locale != "en" && locale != "ar" {
		return models.OperatorSchedule{}, fmt.Errorf("%w: invalid schedule locale", ErrInvalidContract)
	}
	cadence, duration, err := ScheduleCadence(cadenceMinutes)
	if err != nil {
		return models.OperatorSchedule{}, err
	}
	requiredPermissions = uniqueSorted(requiredPermissions)
	for _, permission := range requiredPermissions {
		if !access.HasPermission(permission) {
			return models.OperatorSchedule{}, fmt.Errorf("%w: required permission unavailable", ErrAccessUnavailable)
		}
	}
	scope, _ := json.Marshal(template.VisibleContext)
	payload, _ := json.Marshal(template)
	permissions, _ := json.Marshal(requiredPermissions)
	now := store.now()
	next := now.Add(duration)
	schedule := models.OperatorSchedule{TenantID: access.TenantID, CreatorID: access.UserID, OwnerID: access.UserID, State: "active", Scope: datatypes.JSON(scope), Template: datatypes.JSON(payload), Locale: locale, Cadence: cadence, RequiredPermissions: datatypes.JSON(permissions), AccessVersion: access.AccessVersion, NextRunAt: &next}
	err = store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&schedule).Error; err != nil {
			return err
		}
		return appendScheduleEvent(tx, schedule.ID, schedule.TenantID, "created", access.UserID, map[string]any{"cadence": cadence, "read_only": true, "required_permissions": requiredPermissions})
	})
	return schedule, err
}

func (store *ScheduleStore) List(ctx context.Context, tenantID string, limit int) ([]models.OperatorSchedule, error) {
	var schedules []models.OperatorSchedule
	err := store.db.WithContext(ctx).Where("tenant_id=?", tenantID).Order("updated_at DESC").Limit(limit).Find(&schedules).Error
	return schedules, err
}

func (store *ScheduleStore) Load(ctx context.Context, tenantID, publicID string) (models.OperatorSchedule, error) {
	var schedule models.OperatorSchedule
	err := store.db.WithContext(ctx).Where("tenant_id=? AND public_id=?", tenantID, publicID).First(&schedule).Error
	return schedule, err
}

func (store *ScheduleStore) Pause(ctx context.Context, scheduleID uint, tenantID, actorID, reason string) error {
	return store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var schedule models.OperatorSchedule
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=? AND tenant_id=?", scheduleID, tenantID).First(&schedule).Error; err != nil {
			return err
		}
		now := store.now()
		if err := tx.Model(&schedule).Updates(map[string]any{"state": "paused", "paused_reason": boundedReason(reason), "next_run_at": nil, "updated_at": now}).Error; err != nil {
			return err
		}
		return appendScheduleEvent(tx, schedule.ID, tenantID, "paused", actorID, map[string]any{"reason": boundedReason(reason)})
	})
}

func (store *ScheduleStore) Takeover(ctx context.Context, scheduleID uint, access AccessSnapshot) (models.OperatorSchedule, error) {
	if err := access.ValidateFor(access.UserID, access.TenantID); err != nil || !access.IsAdmin {
		return models.OperatorSchedule{}, ErrAccessUnavailable
	}
	var updated models.OperatorSchedule
	err := store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var schedule models.OperatorSchedule
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=? AND tenant_id=?", scheduleID, access.TenantID).First(&schedule).Error; err != nil {
			return err
		}
		permissions, err := decodePermissions(schedule.RequiredPermissions)
		if err != nil {
			return err
		}
		for _, permission := range permissions {
			if !access.HasPermission(permission) {
				return ErrAccessUnavailable
			}
		}
		duration, err := ParseScheduleCadence(schedule.Cadence)
		if err != nil {
			return err
		}
		next := store.now().Add(duration)
		if err := tx.Model(&schedule).Updates(map[string]any{"owner_id": access.UserID, "access_version": access.AccessVersion, "state": "active", "paused_reason": "", "next_run_at": next}).Error; err != nil {
			return err
		}
		if err := appendScheduleEvent(tx, schedule.ID, schedule.TenantID, "taken_over", access.UserID, map[string]any{"previous_owner": schedule.OwnerID, "read_only": true}); err != nil {
			return err
		}
		schedule.OwnerID, schedule.AccessVersion, schedule.State, schedule.PausedReason, schedule.NextRunAt = access.UserID, access.AccessVersion, "active", "", &next
		updated = schedule
		return nil
	})
	return updated, err
}

func (store *ScheduleStore) Resume(ctx context.Context, scheduleID uint, access AccessSnapshot) (models.OperatorSchedule, error) {
	if err := access.ValidateFor(access.UserID, access.TenantID); err != nil || !access.IsAdmin {
		return models.OperatorSchedule{}, ErrAccessUnavailable
	}
	var updated models.OperatorSchedule
	err := store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var schedule models.OperatorSchedule
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=? AND tenant_id=?", scheduleID, access.TenantID).First(&schedule).Error; err != nil {
			return err
		}
		if schedule.OwnerID != access.UserID {
			return fmt.Errorf("%w: only the current owner may resume; use takeover", ErrForbiddenTool)
		}
		permissions, err := decodePermissions(schedule.RequiredPermissions)
		if err != nil {
			return err
		}
		for _, permission := range permissions {
			if !access.HasPermission(permission) {
				return ErrAccessUnavailable
			}
		}
		duration, err := ParseScheduleCadence(schedule.Cadence)
		if err != nil {
			return err
		}
		next := store.now().Add(duration)
		if err := tx.Model(&schedule).Updates(map[string]any{"access_version": access.AccessVersion, "state": "active", "paused_reason": "", "next_run_at": next}).Error; err != nil {
			return err
		}
		if err := appendScheduleEvent(tx, schedule.ID, schedule.TenantID, "resumed", access.UserID, map[string]any{"read_only": true}); err != nil {
			return err
		}
		schedule.AccessVersion, schedule.State, schedule.PausedReason, schedule.NextRunAt = access.AccessVersion, "active", "", &next
		updated = schedule
		return nil
	})
	return updated, err
}

func (store *ScheduleStore) ClaimDue(ctx context.Context, tenantID string, scheduleID uint) (models.OperatorSchedule, models.OperatorScheduleRun, ScheduledTemplate, error) {
	var schedule models.OperatorSchedule
	var run models.OperatorScheduleRun
	var template ScheduledTemplate
	err := store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=? AND tenant_id=?", scheduleID, tenantID).First(&schedule).Error; err != nil {
			return err
		}
		now := store.now()
		if schedule.State != "active" || schedule.NextRunAt == nil || schedule.NextRunAt.After(now) {
			return gorm.ErrRecordNotFound
		}
		if err := json.Unmarshal(schedule.Template, &template); err != nil || template.Validate() != nil {
			return fmt.Errorf("%w: stored schedule template invalid", ErrInvalidContract)
		}
		duration, err := ParseScheduleCadence(schedule.Cadence)
		if err != nil {
			return err
		}
		claim := uuid.New()
		expires := now.Add(scheduleClaimLease)
		next := now.Add(duration)
		run = models.OperatorScheduleRun{ScheduleID: schedule.ID, TenantID: schedule.TenantID, State: "running", ClaimToken: &claim, ClaimExpiresAt: &expires, StartedAt: now}
		if err := tx.Create(&run).Error; err != nil {
			return err
		}
		if err := tx.Model(&schedule).Updates(map[string]any{"next_run_at": next}).Error; err != nil {
			return err
		}
		schedule.NextRunAt = &next
		return appendScheduleEvent(tx, schedule.ID, schedule.TenantID, "run_claimed", "system", map[string]any{"run_id": run.PublicID.String()})
	})
	return schedule, run, template, err
}

func (store *ScheduleStore) FinishRun(ctx context.Context, runID uint, tenantID, state, reason string, investigationID *uint) error {
	if state != "completed" && state != "failed" && state != "paused" {
		return fmt.Errorf("%w: invalid schedule result", ErrInvalidContract)
	}
	now := store.now()
	return store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var run models.OperatorScheduleRun
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=? AND tenant_id=?", runID, tenantID).First(&run).Error; err != nil {
			return err
		}
		if err := tx.Model(&run).Updates(map[string]any{"state": state, "pause_reason": boundedReason(reason), "result_investigation_id": investigationID, "finished_at": now, "claim_token": nil, "claim_expires_at": nil}).Error; err != nil {
			return err
		}
		return appendScheduleEvent(tx, run.ScheduleID, tenantID, "run_"+state, "system", map[string]any{"run_id": run.PublicID.String(), "reason": boundedReason(reason)})
	})
}

func (store *ScheduleStore) PauseDueFailure(ctx context.Context, schedule models.OperatorSchedule, run models.OperatorScheduleRun, reason string) {
	_ = store.Pause(ctx, schedule.ID, schedule.TenantID, "system", reason)
	_ = store.FinishRun(ctx, run.ID, schedule.TenantID, "paused", reason, nil)
}

func appendScheduleEvent(tx *gorm.DB, scheduleID uint, tenantID, eventType, actorID string, payload map[string]any) error {
	var sequence int64
	if err := tx.Model(&models.OperatorScheduleEvent{}).Where("schedule_id=?", scheduleID).Select("COALESCE(MAX(sequence), 0)").Scan(&sequence).Error; err != nil {
		return err
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return tx.Create(&models.OperatorScheduleEvent{ScheduleID: scheduleID, TenantID: tenantID, Sequence: sequence + 1, EventType: eventType, ActorID: actorID, Payload: datatypes.JSON(raw)}).Error
}
func decodePermissions(raw datatypes.JSON) ([]string, error) {
	var permissions []string
	if err := json.Unmarshal(raw, &permissions); err != nil {
		return nil, err
	}
	return uniqueSorted(permissions), nil
}
func uniqueSorted(values []string) []string {
	seen := map[string]struct{}{}
	out := []string{}
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v != "" {
			seen[v] = struct{}{}
		}
	}
	for v := range seen {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}
func boundedReason(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 200 {
		return value[:200]
	}
	return value
}
