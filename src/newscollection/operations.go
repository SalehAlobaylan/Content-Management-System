// Package newscollection owns explicit News roster changes and the independent
// News intake stop. It never enqueues work or changes another category.
package newscollection

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"content-management-system/src/lifecycle"
	"content-management-system/src/models"
	"content-management-system/src/supply"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const MaxSources = 500

var ErrConflict = errors.New("News settings changed; refresh the preview")

type OperationError struct{ Message string }

func (e *OperationError) Error() string { return e.Message }
func failure(format string, args ...any) error {
	return &OperationError{Message: fmt.Sprintf(format, args...)}
}

type Provider struct {
	Ready  bool   `json:"ready"`
	Reason string `json:"reason,omitempty"`
}
type Capabilities struct {
	Providers      map[string]Provider `json:"providers"`
	CheckedAt      time.Time           `json:"checked_at"`
	SchedulerReady bool                `json:"scheduler_ready"`
}
type Readiness struct {
	Ready          bool     `json:"ready"`
	Paused         bool     `json:"paused"`
	ControlVersion int64    `json:"control_version"`
	Blockers       []string `json:"blockers"`
}
type Source struct {
	ID      uuid.UUID `json:"id"`
	Name    string    `json:"name"`
	Type    string    `json:"type"`
	Version int64     `json:"version"`
	Active  bool      `json:"active"`
	Outcome string    `json:"outcome"`
	Reason  string    `json:"reason,omitempty"`
}
type Snapshot struct {
	Sources   []Source  `json:"sources"`
	Readiness Readiness `json:"readiness"`
	Scope     string    `json:"scope"`
}
type Request struct {
	Action string      `json:"action"`
	Scope  string      `json:"scope"`
	IDs    []uuid.UUID `json:"ids"`
}
type Result struct {
	ID        uuid.UUID `json:"id"`
	Action    string    `json:"action"`
	Changed   int       `json:"changed"`
	Unchanged int       `json:"unchanged"`
	Excluded  int       `json:"excluded"`
	AppliedAt time.Time `json:"applied_at"`
}

func Ready(db *gorm.DB, tenant string, caps Capabilities) (Readiness, error) {
	r := Readiness{Blockers: []string{}}
	state, err := supply.NewsCollectionState(db, tenant)
	if err != nil {
		return r, err
	}
	r.Paused, r.ControlVersion = state.Paused, state.Version
	if err := supply.RequireDurableAdmission(db, tenant, "news"); err != nil {
		r.Blockers = append(r.Blockers, "news_admission_unavailable")
	}
	enabled, err := supply.MayScheduleNormalIntake(db, tenant)
	if err != nil {
		return r, err
	}
	if !enabled {
		r.Blockers = append(r.Blockers, "tenant_intake_paused")
	}
	if !caps.SchedulerReady {
		r.Blockers = append(r.Blockers, "scheduler_unavailable")
	}
	if caps.CheckedAt.IsZero() || time.Since(caps.CheckedAt) > time.Minute || caps.CheckedAt.After(time.Now().Add(10*time.Second)) {
		r.Blockers = append(r.Blockers, "provider_readiness_unavailable")
	}
	r.Ready = len(r.Blockers) == 0
	return r, nil
}

func SourceBlocker(source models.ContentSource, caps Capabilities) string {
	key := string(source.Type)
	if key == "MANUAL" {
		return "manual_source"
	}
	if source.FeedURL == nil || strings.TrimSpace(*source.FeedURL) == "" {
		return "source_address_missing"
	}
	u, err := url.Parse(*source.FeedURL)
	if err != nil || u.Hostname() == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return "source_address_invalid"
	}
	if key == "TWITTER" {
		var settings map[string]any
		_ = json.Unmarshal(source.APIConfig, &settings)
		if settings["mode"] == "api" {
			key = "TWITTER_API"
		}
	}
	p, ok := caps.Providers[key]
	if !ok {
		return "provider_readiness_unavailable"
	}
	if !p.Ready {
		if p.Reason != "" {
			return p.Reason
		}
		return "provider_not_configured"
	}
	return ""
}

func Preview(db *gorm.DB, tenant, actor string, req Request, caps Capabilities) (models.NewsSourceOperation, error) {
	var op models.NewsSourceOperation
	if strings.TrimSpace(tenant) == "" || strings.TrimSpace(actor) == "" {
		return op, failure("explicit actor and tenant required")
	}
	if req.Action != "enable" && req.Action != "disable" && req.Action != "pause" && req.Action != "resume" {
		return op, failure("invalid News action")
	}
	if req.Scope != "all" && req.Scope != "selected" {
		return op, failure("invalid selection scope")
	}
	if len(req.IDs) > MaxSources || (req.Scope == "all" && len(req.IDs) > 0) || (req.Scope == "selected" && len(req.IDs) == 0) {
		return op, failure("select 1..%d explicit sources or all News sources", MaxSources)
	}
	if (req.Action == "pause" || req.Action == "resume") && (req.Scope != "all" || len(req.IDs) > 0) {
		return op, failure("collection controls apply to News, not selected sources")
	}
	readiness, err := Ready(db, tenant, caps)
	if err != nil {
		return op, err
	}
	snapshot := Snapshot{Sources: []Source{}, Readiness: readiness, Scope: req.Scope}
	if req.Action == "enable" || req.Action == "disable" {
		q := db.Where("tenant_id=? AND category='news'", tenant)
		if req.Scope == "selected" {
			seen := map[uuid.UUID]bool{}
			for _, id := range req.IDs {
				if id == uuid.Nil || seen[id] {
					return op, failure("invalid or duplicate source ID")
				}
				seen[id] = true
			}
			q = q.Where("public_id IN ?", req.IDs)
		}
		var sources []models.ContentSource
		if err := q.Order("public_id").Limit(MaxSources + 1).Find(&sources).Error; err != nil {
			return op, err
		}
		if len(sources) > MaxSources {
			return op, failure("select at most %d sources", MaxSources)
		}
		if req.Scope == "selected" && len(sources) != len(req.IDs) {
			return op, failure("selection contains unavailable News sources")
		}
		for _, s := range sources {
			row := Source{ID: s.PublicID, Name: s.Name, Type: string(s.Type), Version: s.SourceConfigVersion, Active: s.IsActive}
			if req.Action == "enable" {
				row.Outcome = "will_enable"
				if s.IsActive {
					row.Outcome = "already_enabled"
					if s.NextDueAt == nil {
						row.Outcome = "will_schedule"
						if reason := SourceBlocker(s, caps); reason != "" {
							row.Outcome, row.Reason = "needs_configuration", reason
						}
					}
				} else if reason := SourceBlocker(s, caps); reason != "" {
					row.Outcome, row.Reason = "needs_configuration", reason
				}
			} else {
				row.Outcome = "will_disable"
				if !s.IsActive {
					row.Outcome = "already_disabled"
				}
			}
			snapshot.Sources = append(snapshot.Sources, row)
		}
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return op, err
	}
	now := time.Now().UTC()
	op = models.NewsSourceOperation{PublicID: uuid.New(), TenantID: tenant, Actor: actor, Action: req.Action, Snapshot: raw, CreatedAt: now, ExpiresAt: now.Add(5 * time.Minute)}
	return op, db.Create(&op).Error
}

// SetSourceActive is shared with single-source activation. The DB trigger owns
// config-version increments. Only the activation and missing schedule change.
func SetSourceActive(tx *gorm.DB, source models.ContentSource, active bool) error {
	if source.Category != "news" {
		return failure("News source required")
	}
	if err := lifecycle.Check(tx, lifecycle.Scope{TenantID: source.TenantID, Lane: "news", SourceID: source.PublicID.String()}, lifecycle.PhaseSourceAdmission); err != nil {
		return err
	}
	if source.IsActive == active && !(active && source.NextDueAt == nil) {
		return nil
	}
	updates := map[string]any{"is_active": active, "updated_at": time.Now().UTC()}
	if active && source.NextDueAt == nil {
		updates["next_due_at"] = time.Now().UTC()
	}
	return tx.Model(&models.ContentSource{}).Where("tenant_id=? AND public_id=?", source.TenantID, source.PublicID).Updates(updates).Error
}

func Apply(db *gorm.DB, tenant, actor string, id uuid.UUID, caps Capabilities) (Result, error) {
	var result Result
	err := db.Transaction(func(tx *gorm.DB) error {
		var op models.NewsSourceOperation
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=? AND tenant_id=? AND actor=?", id, tenant, actor).First(&op).Error; err != nil {
			return err
		}
		if op.AppliedAt != nil {
			return json.Unmarshal(op.Result, &result)
		}
		if time.Now().After(op.ExpiresAt) {
			return failure("preview expired; refresh the preview")
		}
		var snapshot Snapshot
		if err := json.Unmarshal(op.Snapshot, &snapshot); err != nil {
			return err
		}
		controlAction := op.Action == "pause" || op.Action == "resume"

		sources := make([]models.ContentSource, len(snapshot.Sources))
		if !controlAction {
			sort.Slice(snapshot.Sources, func(i, j int) bool { return snapshot.Sources[i].ID.String() < snapshot.Sources[j].ID.String() })
			// Lock and validate the complete captured set before changing any member.
			for i, row := range snapshot.Sources {
				if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND category='news' AND public_id=?", tenant, row.ID).First(&sources[i]).Error; err != nil {
					return ErrConflict
				}
				if sources[i].SourceConfigVersion != row.Version || sources[i].IsActive != row.Active {
					return ErrConflict
				}
			}
		}
		if err := supply.LockNewsCollection(tx, tenant, controlAction); err != nil {
			return err
		}
		current, err := Ready(tx, tenant, caps)
		if err != nil {
			return err
		}
		if current.ControlVersion != snapshot.Readiness.ControlVersion {
			return ErrConflict
		}
		if (op.Action == "enable" || op.Action == "resume") && (!current.Ready || !snapshot.Readiness.Ready) {
			return failure("News collection is not ready: %s", strings.Join(current.Blockers, ", "))
		}
		if op.Action != "pause" {
			if err := lifecycle.Check(tx, lifecycle.Scope{TenantID: tenant, Lane: "news"}, lifecycle.PhaseSourceAdmission); err != nil {
				return err
			}
		}
		result = Result{ID: op.PublicID, Action: op.Action, AppliedAt: time.Now().UTC()}
		if controlAction {
			desired := op.Action == "pause"
			if current.Paused == desired {
				result.Unchanged = 1
			} else {
				state := models.NewsCollectionControl{TenantID: tenant, Paused: desired, Version: current.ControlVersion + 1, UpdatedAt: result.AppliedAt, UpdatedBy: actor}
				if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}}, DoUpdates: clause.AssignmentColumns([]string{"paused", "version", "updated_at", "updated_by"})}).Create(&state).Error; err != nil {
					return err
				}
				result.Changed = 1
			}
		} else {

			for i, row := range snapshot.Sources {
				if row.Outcome == "needs_configuration" {
					result.Excluded++
					continue
				}
				desired := op.Action == "enable"
				if sources[i].IsActive == desired && !(desired && sources[i].NextDueAt == nil) {
					result.Unchanged++
					continue
				}
				if desired && SourceBlocker(sources[i], caps) != "" {
					return failure("provider configuration changed; refresh the preview")
				}
				if err := SetSourceActive(tx, sources[i], desired); err != nil {
					return err
				}
				result.Changed++
			}
		}
		raw, err := json.Marshal(result)
		if err != nil {
			return err
		}
		return tx.Model(&op).Updates(map[string]any{"result": datatypes.JSON(raw), "applied_at": result.AppliedAt}).Error
	})
	return result, err
}
