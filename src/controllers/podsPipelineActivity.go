package controllers

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type podsPipelineActivityCounts struct {
	RunningSteps        int64 `json:"running_steps"`
	ClaimedSteps        int64 `json:"claimed_steps"`
	VerifyingSteps      int64 `json:"verifying_steps"`
	UnresolvedSteps     int64 `json:"unresolved_steps"`
	QueuedSteps         int64 `json:"queued_steps"`
	RunningSourceRuns   int64 `json:"running_source_runs"`
	AdmittedSourceRuns  int64 `json:"admitted_source_runs"`
	VerifyingSourceRuns int64 `json:"verifying_source_runs"`
	QueuedSourceRuns    int64 `json:"queued_source_runs"`
}

func readPodsPipelineActivityCounts(db *gorm.DB, tenant string) (podsPipelineActivityCounts, error) {
	var result podsPipelineActivityCounts
	err := db.Raw(`SELECT
	 COUNT(*) FILTER (WHERE r.state='running') AS running_steps,
	 COUNT(*) FILTER (WHERE r.state='claimed') AS claimed_steps,
	 COUNT(*) FILTER (WHERE r.state='verifying') AS verifying_steps,
	 COUNT(*) FILTER (WHERE r.state IN ('uncertain','reconciling')) AS unresolved_steps,
	 COUNT(*) FILTER (WHERE r.state IN ('queued','deferred')) AS queued_steps
	 FROM content_stage_requests r JOIN content_items i
	 ON i.tenant_id=r.tenant_id AND i.public_id=r.content_item_id AND i.processing_generation=r.processing_generation
	 WHERE r.tenant_id=? AND r.lane='pods' AND i.status<>'ARCHIVED'`, tenant).Scan(&result).Error
	if err != nil {
		return result, err
	}
	var sources struct{ Running, Admitted, Verifying, Queued int64 }
	err = db.Raw(`SELECT COUNT(*) FILTER (WHERE state='running') AS running,
	 COUNT(*) FILTER (WHERE state='accepted') AS admitted,
	 COUNT(*) FILTER (WHERE state='verification_required') AS verifying,
	 COUNT(*) FILTER (WHERE state='requested') AS queued
	 FROM source_run_requests WHERE tenant_id=? AND lane='media'`, tenant).Scan(&sources).Error
	result.RunningSourceRuns, result.AdmittedSourceRuns = sources.Running, sources.Admitted
	result.VerifyingSourceRuns, result.QueuedSourceRuns = sources.Verifying, sources.Queued
	return result, err
}

type podsPipelineActivityEvent struct {
	ID        uuid.UUID  `json:"id"`
	EventAt   time.Time  `json:"event_at"`
	Kind      string     `json:"kind"`
	Title     string     `json:"title"`
	Trigger   string     `json:"trigger"`
	Actor     string     `json:"actor,omitempty"`
	Outcome   string     `json:"outcome"`
	ContentID *uuid.UUID `json:"content_id,omitempty"`
	SourceID  *uuid.UUID `json:"source_id,omitempty"`
	Units     int        `json:"units,omitempty"`
}

type podsPipelineActivityCursor struct {
	At   time.Time `json:"at"`
	Kind string    `json:"kind"`
	ID   uuid.UUID `json:"id"`
}

// Reads only recorded source admission and generation activation evidence.
// A scheduled ingest is never used to infer the actor/reason of publication.
func GetPodsPipelineActivity(c *gin.Context) {
	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	var cursor podsPipelineActivityCursor
	if raw := c.Query("cursor"); raw != "" {
		if len(raw) > 512 {
			c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid activity cursor"})
			return
		}
		bytes, err := base64.RawURLEncoding.DecodeString(raw)
		if err != nil || json.Unmarshal(bytes, &cursor) != nil || cursor.At.IsZero() || cursor.ID == uuid.Nil || (cursor.Kind != "source_run" && cursor.Kind != "publication") {
			c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid activity cursor"})
			return
		}
	}
	db := c.MustGet("db").(*gorm.DB).WithContext(c.Request.Context())
	query := db.Table(`(
	 SELECT r.public_id AS id,r.requested_at AS event_at,'source_run'::text AS kind,
	 COALESCE(s.name,'Source no longer available') AS title,r.requested_by AS trigger,
	 r.requested_by_actor_id AS actor,r.state AS outcome,NULL::uuid AS content_id,s.public_id AS source_id,0 AS units
	 FROM source_run_requests r LEFT JOIN content_sources s ON s.tenant_id=r.tenant_id AND s.public_id=r.content_source_id
	 WHERE r.tenant_id=? AND r.lane='media'
	 UNION ALL
	 SELECT g.public_id,g.activation_at,'publication',COALESCE(i.title,'Published episode'),
	 'unrecorded','',g.state,g.parent_content_item_id,i.content_source_id,g.expected_units
	 FROM atomization_generations g LEFT JOIN content_items i ON i.tenant_id=g.tenant_id AND i.public_id=g.parent_content_item_id
	 WHERE g.tenant_id=? AND g.activation_at IS NOT NULL
	) AS events`, p.TenantID, p.TenantID)
	if !cursor.At.IsZero() {
		query = query.Where("event_at<? OR (event_at=? AND (kind>? OR (kind=? AND id>?)))", cursor.At, cursor.At, cursor.Kind, cursor.Kind, cursor.ID)
	}
	items := []podsPipelineActivityEvent{}
	if err := query.Order("event_at DESC,kind ASC,id ASC").Limit(21).Find(&items).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Pipeline activity is unavailable"})
		return
	}
	next := ""
	if len(items) > 20 {
		items = items[:20]
		last := items[len(items)-1]
		bytes, _ := json.Marshal(podsPipelineActivityCursor{At: last.EventAt, Kind: last.Kind, ID: last.ID})
		next = base64.RawURLEncoding.EncodeToString(bytes)
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, gin.H{"items": items, "next_cursor": next, "observed_at": time.Now().UTC()})
}
