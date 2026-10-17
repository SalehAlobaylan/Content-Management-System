package controllers

import (
	"content-management-system/src/economics"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"strings"
	"time"
)

func writeEconomicsStorageWindow(c *gin.Context, db *gorm.DB, req writeOpMetricsRequest) {
	start, e := time.Parse(time.RFC3339, req.WindowStart)
	end, ee := time.Parse(time.RFC3339, req.WindowEnd)
	if e != nil || ee != nil || !start.Equal(start.UTC().Truncate(time.Hour)) || end.Sub(start) != time.Hour || end.After(time.Now()) || start.Before(time.Now().AddDate(0, 0, -90)) || req.WindowKey == "" || len(req.WindowKey) > 256 || len(req.Items) > 100 {
		c.JSON(400, gin.H{"error": "cloudflare requires an exact completed UTC hour and stable account/bucket window key"})
		return
	}
	if !economics.Available(db) {
		c.JSON(503, gin.H{"error": "Economics window migration required; no additive fallback"})
		return
	}
	tenant := strings.TrimSpace(req.TenantID)
	if tenant == "" {
		tenant = "default"
	}
	if req.Date != start.UTC().Format("2006-01-02") {
		c.JSON(400, gin.H{"error": "date must match UTC window start"})
		return
	}
	for _, i := range req.Items {
		if i.Count < 0 || i.Count > 1e15 || i.Tier != "primary" && i.Tier != "cold" || i.OpClass != "A" && i.OpClass != "B" && i.OpClass != "F" && i.OpClass != "U" || len(i.OpType) > 32 {
			c.JSON(400, gin.H{"error": "Invalid operation window row"})
			return
		}
	}
	payload, _ := json.Marshal(req.Items)
	if len(req.Items) == 0 {
		payload = []byte(`[]`)
	}
	checksum := fmt.Sprintf("%x", sha256.Sum256(payload))
	key := req.WindowKey + ":" + start.UTC().Format(time.RFC3339)
	e = db.Transaction(func(tx *gorm.DB) error {
		if e := economics.Lock(tx); e != nil {
			return e
		}
		// Preserve the old non-idempotent daily observation separately on the
		// first verified window. It cannot be summed with replacement windows.
		day := start.UTC().Truncate(24 * time.Hour)
		if e := tx.Exec(`UPDATE storage_op_metrics SET source='cloudflare_legacy' WHERE tenant_id=? AND date=? AND source='cloudflare' AND NOT EXISTS(SELECT 1 FROM economics_import_windows WHERE tenant_id=? AND source='cloudflare' AND observed_at>=? AND observed_at<?)`, tenant, day, tenant, day, day.AddDate(0, 0, 1)).Error; e != nil {
			return e
		}
		if e := tx.Exec(`INSERT INTO economics_import_windows(tenant_id,source,window_key,checksum,payload,observed_at) VALUES(?,'cloudflare',?,?,?::jsonb,?) ON CONFLICT(tenant_id,source,window_key) DO UPDATE SET checksum=EXCLUDED.checksum,payload=EXCLUDED.payload,observed_at=EXCLUDED.observed_at`, tenant, key, checksum, string(payload), start.UTC()).Error; e != nil {
			return e
		}
		if e := tx.Exec(`DELETE FROM storage_op_metrics WHERE tenant_id=? AND date=? AND source='cloudflare'`, tenant, day).Error; e != nil {
			return e
		}
		return tx.Exec(`INSERT INTO storage_op_metrics(tenant_id,date,tier,op_class,op_type,source,count,updated_at) SELECT ?,?,item->>'tier',item->>'op_class',item->>'op_type','cloudflare',sum((item->>'count')::bigint),now() FROM economics_import_windows w CROSS JOIN LATERAL jsonb_array_elements(w.payload) item WHERE w.tenant_id=? AND w.source='cloudflare' AND observed_at>=? AND observed_at<? GROUP BY item->>'tier',item->>'op_class',item->>'op_type'`, tenant, day, tenant, day, day.AddDate(0, 0, 1)).Error
	})
	if e != nil {
		c.JSON(503, gin.H{"error": "Window was not committed; retry identical evidence"})
		return
	}
	c.JSON(200, gin.H{"success": true, "written": len(req.Items), "window_key": key, "coverage": "observed windows only; usage is not an invoice"})
}
