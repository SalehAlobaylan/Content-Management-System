package controllers

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/utils"
	"content-management-system/src/workload"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Workload policy is deployment-wide. Route registration requires the verified
// administrator role, independently of the caller's tenant feed permissions.
func workloadActor(c *gin.Context) string {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok || !principal.HasRole("admin") {
		return ""
	}
	return strings.TrimSpace(principal.UserID)
}

func workloadFailure(c *gin.Context, err error) {
	status, code, message := http.StatusServiceUnavailable, "WORKLOAD_UNAVAILABLE", "Capacity evidence is unavailable. Retry after the CMS migration and health checks complete."
	if errors.Is(err, gorm.ErrRecordNotFound) {
		status, code, message = http.StatusNotFound, "NOT_FOUND", "The capacity record was not found."
	} else if errors.Is(err, workload.ErrConflict) {
		status, code, message = http.StatusConflict, "POLICY_CONFLICT", "Settings changed. Reload the current policy before saving your draft."
	} else if errors.Is(err, workload.ErrNotQualified) {
		status, code, message = http.StatusConflict, "EVIDENCE_REQUIRED", "Enforcement requires a current qualified evidence review. Reload Evidence before confirming."
	} else if errors.Is(err, workload.ErrInvalid) || errors.Is(err, workload.ErrUnsafeRevoke) || errors.Is(err, workload.ErrNotAdmitted) {
		status, code, message = http.StatusBadRequest, "INVALID_WORKLOAD_ACTION", "The requested capacity action is invalid or the work has already started. Reload its current evidence."
	}
	c.JSON(status, gin.H{"error": message, "message": message, "code": code})
}

func decodeWorkloadBody(c *gin.Context, value any) bool {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 32<<10)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid capacity request", "code": "INVALID_BODY"})
		return false
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Expected one capacity request", "code": "INVALID_BODY"})
		return false
	}
	return true
}

func GetWorkloadStatus(c *gin.Context) {
	status, err := workload.GetStatus(c.MustGet("db").(*gorm.DB))
	if err != nil {
		workloadFailure(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": status})
}

func GetWorkloadPolicy(c *gin.Context) {
	policy, err := workload.GetPolicy(c.MustGet("db").(*gorm.DB))
	if err != nil {
		workloadFailure(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": policy})
}

func UpdateWorkloadPolicy(c *gin.Context) {
	var body workload.PolicyUpdate
	if !decodeWorkloadBody(c, &body) {
		return
	}
	policy, err := workload.UpdatePolicy(c.MustGet("db").(*gorm.DB), body, workloadActor(c))
	if err != nil {
		workloadFailure(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": policy})
}

func GetWorkloadQualification(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	evidence, err := workload.GetQualification(db)
	if err != nil {
		workloadFailure(c, err)
		return
	}
	correlations, correlationErr := workloadHealthCorrelations(db, evidence.WindowStart, evidence.WindowEnd)
	reason := "Same-resource time overlap is supporting context, not proof that admission would have prevented an incident."
	if correlationErr != nil {
		reason = "System Health correlation evidence is unavailable; no prevention claim can be established."
	}
	type qualificationView struct {
		workload.Qualification
		Correlations         []workloadHealthCorrelation `json:"correlations"`
		CorrelationAvailable bool                        `json:"correlation_available"`
		CorrelationReason    string                      `json:"correlation_reason"`
	}
	c.JSON(http.StatusOK, gin.H{"data": qualificationView{Qualification: evidence, Correlations: correlations, CorrelationAvailable: correlationErr == nil, CorrelationReason: reason}})
}

func GetWorkloadRegistry(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"data": workload.Registry()})
}

func workloadFilter(c *gin.Context, permitHistory ...bool) (workload.ListFilter, bool) {
	limit := 50
	if raw := c.Query("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 100 {
			c.JSON(400, gin.H{"error": "limit must be 1..100"})
			return workload.ListFilter{}, false
		}
		limit = n
	}
	var before uint64
	var beforeTime *time.Time
	var beforePermitID string
	if raw := c.Query("before"); raw != "" {
		if len(permitHistory) > 0 && permitHistory[0] {
			if len(raw) > 200 {
				c.JSON(400, gin.H{"error": "Invalid permit history cursor"})
				return workload.ListFilter{}, false
			}
			decoded, err := base64.RawURLEncoding.DecodeString(raw)
			parts := strings.SplitN(string(decoded), "|", 2)
			if err != nil || len(parts) != 2 || len(raw) > 200 {
				c.JSON(400, gin.H{"error": "Invalid permit history cursor"})
				return workload.ListFilter{}, false
			}
			at, timeErr := time.Parse(time.RFC3339Nano, parts[0])
			_, idErr := uuid.Parse(parts[1])
			if timeErr != nil || idErr != nil {
				c.JSON(400, gin.H{"error": "Invalid permit history cursor"})
				return workload.ListFilter{}, false
			}
			beforeTime, beforePermitID = &at, parts[1]
		} else {
			n, err := strconv.ParseUint(raw, 10, 64)
			if err != nil || n == 0 {
				c.JSON(400, gin.H{"error": "Invalid history cursor"})
				return workload.ListFilter{}, false
			}
			before = n
		}
	}
	for _, key := range []string{"tenant", "system", "class", "state", "verdict"} {
		if len(c.Query(key)) > 80 {
			c.JSON(400, gin.H{"error": "Invalid history filter"})
			return workload.ListFilter{}, false
		}
	}
	return workload.ListFilter{TenantID: c.Query("tenant"), System: c.Query("system"), Class: c.Query("class"), State: c.Query("state"), Verdict: c.Query("verdict"), Limit: limit, BeforeID: before, BeforeTime: beforeTime, BeforePermitID: beforePermitID}, true
}

func ListWorkloadPermits(c *gin.Context) {
	filter, ok := workloadFilter(c, true)
	if !ok {
		return
	}
	rows, err := workload.ListPermits(c.MustGet("db").(*gorm.DB), filter)
	if err != nil {
		workloadFailure(c, err)
		return
	}
	type permitView struct {
		models.WorkloadPermit
		CanRevoke bool `json:"can_revoke"`
	}
	items := make([]permitView, 0, len(rows))
	for _, row := range rows {
		// Authorization is server-owned; a running or uncertain reservation cannot
		// be revoked as a substitute for stopping or verifying its native owner.
		items = append(items, permitView{WorkloadPermit: row, CanRevoke: row.State == workload.StateAcquired && row.BeganAt == nil && row.ExpiresAt.After(time.Now())})
	}
	response := gin.H{"data": items}
	if len(rows) == filter.Limit && len(rows) > 0 {
		last := rows[len(rows)-1]
		response["next_cursor"] = base64.RawURLEncoding.EncodeToString([]byte(last.CreatedAt.UTC().Format(time.RFC3339Nano) + "|" + last.ID))
	}
	c.JSON(http.StatusOK, response)
}

func ListWorkloadDecisions(c *gin.Context) {
	filter, ok := workloadFilter(c)
	if !ok {
		return
	}
	rows, err := workload.ListDecisions(c.MustGet("db").(*gorm.DB), filter)
	if err != nil {
		workloadFailure(c, err)
		return
	}
	response := gin.H{"data": rows}
	if len(rows) == filter.Limit && len(rows) > 0 {
		response["next_cursor"] = strconv.FormatUint(rows[len(rows)-1].ID, 10)
	}
	c.JSON(http.StatusOK, response)
}

func ListWorkloadSnapshots(c *gin.Context) {
	filter, ok := workloadFilter(c)
	if !ok {
		return
	}
	rows, err := workload.ListSnapshots(c.MustGet("db").(*gorm.DB), filter.Limit, c.Query("resource"))
	if err != nil {
		workloadFailure(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": rows})
}

func PauseWorkloadGovernor(c *gin.Context) {
	var body struct {
		Revision int64  `json:"revision"`
		Minutes  int    `json:"minutes"`
		Reason   string `json:"reason"`
	}
	if !decodeWorkloadBody(c, &body) {
		return
	}
	policy, err := workload.Pause(c.MustGet("db").(*gorm.DB), body.Revision, body.Minutes, workloadActor(c), body.Reason)
	if err != nil {
		workloadFailure(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": policy})
}

func RevokeWorkloadPermit(c *gin.Context) {
	if _, err := uuid.Parse(c.Param("id")); err != nil {
		c.JSON(400, gin.H{"error": "Invalid permit ID"})
		return
	}
	var body struct {
		Reason string `json:"reason"`
	}
	if !decodeWorkloadBody(c, &body) {
		return
	}
	if err := workload.RevokePermit(c.MustGet("db").(*gorm.DB), c.Param("id"), workloadActor(c), body.Reason); err != nil {
		workloadFailure(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"revoked": true}})
}
