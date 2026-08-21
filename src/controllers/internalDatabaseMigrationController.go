package controllers

import (
	"content-management-system/src/utils"
	"database/sql"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type migrationOwnerRequest struct {
	ProgramID     string `json:"program_id" binding:"required"`
	ExpectedEpoch int64  `json:"expected_epoch" binding:"min=0"`
}

type migrationOwnerControl struct {
	State     string
	ProgramID sql.NullString
	Epoch     int64
}

type migrationInflightEvidence struct {
	Subject string `json:"subject"`
	Count   int64  `json:"count"`
	State   string `json:"state"`
}

var cmsInflightQueries = []struct{ subject, query string }{
	{"source_run_attempts", `SELECT count(*) FROM source_run_attempts WHERE state IN ('claimed','running','verifying','uncertain','reconciling')`},
	{"content_stage_attempts", `SELECT count(*) FROM content_stage_attempts WHERE state IN ('claimed','running','verifying','uncertain','reconciling')`},
	{"pipeline_repair_requests", `SELECT count(*) FROM pipeline_repair_requests WHERE state IN ('claimed','running','verifying','uncertain')`},
	{"artifact_coverage_requests", `SELECT count(*) FROM artifact_coverage_requests WHERE state IN ('claimed','running','verifying','uncertain')`},
	{"atomization_work_requests", `SELECT count(*) FROM atomization_work_requests WHERE state IN ('claimed','running','verifying','uncertain')`},
	{"studio_clearance_requests", `SELECT count(*) FROM studio_clearance_requests WHERE state IN ('claimed','running','verifying','uncertain')`},
	{"media_supply_actions", `SELECT count(*) FROM media_supply_actions WHERE state IN ('claimed','running','verifying','uncertain')`},
}

func readMigrationOwnerControl(db *gorm.DB) (migrationOwnerControl, error) {
	var out migrationOwnerControl
	err := db.Raw(`SELECT state, migration_program_id::text, fence_epoch FROM database_migration_owner_control WHERE singleton = TRUE`).Row().Scan(&out.State, &out.ProgramID, &out.Epoch)
	return out, err
}

func readCMSInflight(db *gorm.DB) ([]migrationInflightEvidence, int64, bool) {
	items := make([]migrationInflightEvidence, 0, len(cmsInflightQueries))
	var total int64
	complete := true
	for _, item := range cmsInflightQueries {
		var exists bool
		table := strings.Split(item.subject, ".")[0]
		if err := db.Raw(`SELECT to_regclass(?) IS NOT NULL`, "public."+table).Scan(&exists).Error; err != nil || !exists {
			items = append(items, migrationInflightEvidence{Subject: item.subject, State: "unknown"})
			complete = false
			continue
		}
		var count int64
		if err := db.Raw(item.query).Scan(&count).Error; err != nil {
			items = append(items, migrationInflightEvidence{Subject: item.subject, State: "unknown"})
			complete = false
			continue
		}
		state := "absent"
		if count > 0 {
			state = "present"
		}
		items = append(items, migrationInflightEvidence{Subject: item.subject, Count: count, State: state})
		total += count
	}
	return items, total, complete
}

func InternalGetDatabaseMigrationQuiescence(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	fence, err := utils.ReadWriterFence(db)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"state": "unknown", "reason": "writer_fence_unavailable"})
		return
	}
	control, err := readMigrationOwnerControl(db)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"state": "unknown", "reason": "owner_control_unavailable"})
		return
	}
	inflight, count, complete := readCMSInflight(db)
	state := "not_quiesced"
	if control.State != "running" {
		state = "draining"
	}
	if !complete {
		state = "unknown"
	} else if control.State == "quiescing" && fence.State == "sealed" && count == 0 {
		state = "quiesced"
	}
	c.JSON(http.StatusOK, gin.H{"state": state, "writer_fence": fence, "owner_control": gin.H{"state": control.State, "program_id": control.ProgramID.String, "fence_epoch": control.Epoch}, "inflight": inflight, "active_count": count, "observed_at": time.Now().UTC()})
}

func bindMigrationOwnerRequest(c *gin.Context) (migrationOwnerRequest, bool) {
	var req migrationOwnerRequest
	if c.ShouldBindJSON(&req) != nil || uuid.Validate(req.ProgramID) != nil || req.ExpectedEpoch < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "valid program_id and expected_epoch are required"})
		return req, false
	}
	return req, true
}

func InternalQuiesceDatabaseMigrationOwner(c *gin.Context) {
	req, ok := bindMigrationOwnerRequest(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	fence, err := utils.ReadWriterFence(db)
	if err != nil || fence.Epoch != req.ExpectedEpoch || (fence.State != "quiescing" && fence.State != "sealed") {
		c.JSON(http.StatusConflict, gin.H{"error": "writer_fence_precondition_changed"})
		return
	}
	sqlDB, err := db.DB()
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database_unavailable"})
		return
	}
	result, err := sqlDB.ExecContext(c, `UPDATE database_migration_owner_control SET state='quiescing', migration_program_id=$1, fence_epoch=$2, changed_at=now(), changed_by='migration-coordinator' WHERE singleton=TRUE AND (state='running' OR (migration_program_id=$1 AND fence_epoch=$2))`, req.ProgramID, req.ExpectedEpoch)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "owner_control_update_failed"})
		return
	}
	rows, _ := result.RowsAffected()
	if rows != 1 {
		c.JSON(http.StatusConflict, gin.H{"error": "owner_control_owned_by_another_program"})
		return
	}
	InternalGetDatabaseMigrationQuiescence(c)
}

func InternalResumeDatabaseMigrationOwner(c *gin.Context) {
	req, ok := bindMigrationOwnerRequest(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	fence, err := utils.ReadWriterFence(db)
	if err != nil || fence.Epoch != req.ExpectedEpoch || (fence.State != "open" && fence.State != "successor_open") {
		c.JSON(http.StatusConflict, gin.H{"error": "writer_fence_does_not_permit_resume"})
		return
	}
	sqlDB, err := db.DB()
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database_unavailable"})
		return
	}
	result, err := sqlDB.ExecContext(c, `UPDATE database_migration_owner_control SET state='running', migration_program_id=NULL, fence_epoch=$2, changed_at=now(), changed_by='migration-coordinator' WHERE singleton=TRUE AND migration_program_id=$1`, req.ProgramID, req.ExpectedEpoch)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "owner_control_update_failed"})
		return
	}
	rows, _ := result.RowsAffected()
	if rows != 1 {
		c.JSON(http.StatusConflict, gin.H{"error": "owner_control_not_owned_by_program"})
		return
	}
	InternalGetDatabaseMigrationQuiescence(c)
}
