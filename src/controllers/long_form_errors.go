package controllers

import (
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"log"
	"net/http"
	"strings"
)

// Never serialize or log the raw database error: driver details can include
// transcript payloads, object URLs and credentials embedded in SQL values.
func longFormError(c *gin.Context, fallbackStatus int, code, message string, err error) {
	status, retryable, sqlstate := fallbackStatus, fallbackStatus == http.StatusServiceUnavailable, ""
	var databaseError interface{ SQLState() string }
	if errors.As(err, &databaseError) {
		sqlstate = databaseError.SQLState()
		switch {
		case strings.HasPrefix(sqlstate, "08"), strings.HasPrefix(sqlstate, "40"), strings.HasPrefix(sqlstate, "53"), sqlstate == "55P03", sqlstate == "57014":
			status, code, message, retryable = http.StatusServiceUnavailable, "persistence_temporarily_unavailable", "Persistence temporarily unavailable", true
		case strings.HasPrefix(sqlstate, "42"):
			status, code, message, retryable = http.StatusInternalServerError, "schema_contract_failure", "CMS schema requires operator repair", false
		default:
			status, code, message, retryable = http.StatusInternalServerError, "persistence_failure", "CMS persistence failed", false
		}
	}
	correlation := uuid.NewString()
	log.Printf("long_form_failure correlation_id=%s code=%s sqlstate=%s", correlation, code, sqlstate)
	c.Header("X-Correlation-ID", correlation)
	c.JSON(status, gin.H{"code": code, "message": message, "error": message, "correlation_id": correlation, "retryable": retryable})
}
