package economics

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"gorm.io/gorm"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

type NeonConfig struct {
	OrgID     string `json:"org_id"`
	ProjectID string `json:"project_id"`
}

var neonID = regexp.MustCompile(`^[a-z0-9-]{1,60}$`)

type NeonConsumption struct {
	Projects []struct {
		ProjectID string `json:"project_id"`
		Periods   []struct {
			PeriodID    string `json:"period_id"`
			PeriodPlan  string `json:"period_plan"`
			Consumption []struct {
				Start   time.Time `json:"timeframe_start"`
				End     time.Time `json:"timeframe_end"`
				Metrics []struct {
					Name  string      `json:"metric_name"`
					Value json.Number `json:"value"`
				} `json:"metrics"`
			} `json:"consumption"`
		} `json:"periods"`
	} `json:"projects"`
	Pagination struct {
		Cursor string `json:"cursor"`
	} `json:"pagination"`
}

func FetchNeon(ctx context.Context, client *http.Client, endpoint, token string, cfg NeonConfig, from, to time.Time) (NeonConsumption, error) {
	var out NeonConsumption
	if !neonID.MatchString(cfg.OrgID) || !neonID.MatchString(cfg.ProjectID) {
		return out, fmt.Errorf("explicit Neon organization and project IDs required")
	}
	q := url.Values{"org_id": {cfg.OrgID}, "project_ids": {cfg.ProjectID}, "from": {from.Format(time.RFC3339)}, "to": {to.Format(time.RFC3339)}, "granularity": {"daily"}, "metrics": {"compute_unit_seconds,root_branch_bytes_month,child_branch_bytes_month,instant_restore_bytes_month,public_network_transfer_bytes,private_network_transfer_bytes,snapshot_storage_bytes_month"}, "limit": {"1"}}
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+q.Encode(), nil)
	if e != nil {
		return out, e
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, e := client.Do(req)
	if e != nil {
		return out, fmt.Errorf("Neon read unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return out, fmt.Errorf("Neon consumption unavailable (HTTP %d); use invoice fallback", resp.StatusCode)
	}
	decoder := json.NewDecoder(io.LimitReader(resp.Body, 2*1024*1024))
	decoder.UseNumber()
	if e := decoder.Decode(&out); e != nil {
		return out, fmt.Errorf("invalid bounded Neon response")
	}
	if len(out.Projects) > 1 || out.Pagination.Cursor != "" {
		return out, fmt.Errorf("unexpected project pagination; no partial import published")
	}
	for _, p := range out.Projects {
		if p.ProjectID != cfg.ProjectID || len(p.Periods) > 4 {
			return out, fmt.Errorf("provider scope mismatch")
		}
		for _, period := range p.Periods {
			if len(period.Consumption) > 60 {
				return out, fmt.Errorf("provider window exceeds limit")
			}
			for _, window := range period.Consumption {
				if window.Start.Before(from) || window.End.After(to) || !window.End.After(window.Start) || len(window.Metrics) > 8 {
					return out, fmt.Errorf("invalid provider time window")
				}
				for _, m := range window.Metrics {
					value, e := m.Value.Float64()
					if e != nil || value < 0 || math.IsNaN(value) || math.IsInf(value, 0) || value > 1e30 || !strings.Contains(",compute_unit_seconds,root_branch_bytes_month,child_branch_bytes_month,instant_restore_bytes_month,public_network_transfer_bytes,private_network_transfer_bytes,snapshot_storage_bytes_month,", ","+m.Name+",") {
						return out, fmt.Errorf("invalid resource metric")
					}
				}
			}
		}
	}
	return out, nil
}
func ImportNeon(db *gorm.DB, s Source) error {
	db = privateDB(db)
	token := os.Getenv("NEON_API_KEY")
	var cfg NeonConfig
	if token == "" || json.Unmarshal(s.Config, &cfg) != nil {
		return markSource(db, s, "unavailable", "Neon credential or explicit organization/project mapping is unavailable; actual bills may be entered manually")
	}
	to := time.Now().UTC().Truncate(24 * time.Hour)
	from := to.AddDate(0, 0, -3)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, e := FetchNeon(ctx, &http.Client{Timeout: 10 * time.Second}, "https://console.neon.tech/api/v2/consumption_history/v2/projects", token, cfg, from, to)
	if e != nil {
		_ = markSource(db, s, "unavailable", e.Error())
		return e
	}
	e = db.Transaction(func(tx *gorm.DB) error {
		if e := Lock(tx); e != nil {
			return e
		}
		for _, p := range result.Projects {
			for _, period := range p.Periods {
				for _, window := range period.Consumption {
					payload, _ := json.Marshal(window)
					checksum := fmt.Sprintf("%x", sha256.Sum256(payload))
					key := s.ID + ":" + window.Start.Format(time.RFC3339)
					if e := tx.Exec(`INSERT INTO economics_import_windows(tenant_id,source,window_key,checksum,payload,observed_at) VALUES(?,'neon',?,?,?::jsonb,?) ON CONFLICT(tenant_id,source,window_key) DO UPDATE SET checksum=EXCLUDED.checksum,payload=EXCLUDED.payload,observed_at=EXCLUDED.observed_at`, s.TenantID, key, checksum, string(payload), time.Now().UTC()).Error; e != nil {
						return e
					}
				}
			}
		}
		return nil
	})
	if e != nil {
		return e
	}
	return markSource(db, s, "usage_only", "Read-only provider consumption imported. Account allocation and actual charges still require a qualified serving pool and invoice/contract evidence")
}
func markSource(db *gorm.DB, s Source, status, note string) error {
	db = privateDB(db)
	return db.Transaction(func(tx *gorm.DB) error {
		if e := Lock(tx); e != nil {
			return e
		}
		return tx.Model(&Source{}).Where("tenant_id=? AND id=?", s.TenantID, s.ID).Updates(map[string]any{"status": status, "note": note, "updated_at": time.Now().UTC()}).Error
	})
}
func ImportDue(db *gorm.DB) {
	var sources []Source
	if db.Table("economics_sources s").Select("s.*").Joins("JOIN economics_policies p ON p.tenant_id=s.tenant_id AND p.imports_enabled=true").Where("s.capability='neon_consumption' AND s.updated_at<?", time.Now().Add(-time.Hour)).Order("s.updated_at").Limit(2).Scan(&sources).Error == nil {
		for _, s := range sources {
			_ = ImportNeon(db, s)
		}
	}
}
