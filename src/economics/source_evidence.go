package economics

import (
	"encoding/json"
	"gorm.io/gorm"
	"time"
)

type ProviderUsageMetric struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}
type ProviderUsageWindow struct {
	SourceID   string                `json:"source_id"`
	From       time.Time             `json:"from"`
	To         time.Time             `json:"to"`
	ObservedAt time.Time             `json:"observed_at"`
	Metrics    []ProviderUsageMetric `json:"metrics"`
}

// Read already imported resource evidence; a Console read never calls providers
// or turns consumption into charges using an assumed public rate.
func sourceEvidence(db *gorm.DB, tenant string, ids []string) ([]ProviderUsageWindow, error) {
	out := []ProviderUsageWindow{}
	if len(ids) == 0 {
		return out, nil
	}
	var rows []struct {
		SourceID   string
		Payload    json.RawMessage
		ObservedAt time.Time
	}
	if e := db.Table("economics_import_windows").Select("split_part(window_key,':',1) source_id,payload,observed_at").Where("tenant_id=? AND source='neon' AND split_part(window_key,':',1) IN ?", tenant, ids).Order("observed_at DESC,window_key").Limit(500).Scan(&rows).Error; e != nil {
		return nil, e
	}
	for _, r := range rows {
		var data struct {
			From    time.Time `json:"timeframe_start"`
			To      time.Time `json:"timeframe_end"`
			Metrics []struct {
				Name  string      `json:"metric_name"`
				Value json.Number `json:"value"`
			} `json:"metrics"`
		}
		if e := json.Unmarshal(r.Payload, &data); e != nil {
			return nil, e
		}
		window := ProviderUsageWindow{SourceID: r.SourceID, From: data.From, To: data.To, ObservedAt: r.ObservedAt, Metrics: []ProviderUsageMetric{}}
		for _, m := range data.Metrics {
			window.Metrics = append(window.Metrics, ProviderUsageMetric{Name: m.Name, Value: m.Value.String()})
		}
		out = append(out, window)
	}
	return out, nil
}
