package controllers

import (
	"encoding/json"
	"fmt"
	"time"
)

// PostgreSQL jsonb serializes legacy timestamp-without-time-zone columns
// without an offset. Match the driver's UTC interpretation of those columns
// at this aggregate boundary; RFC3339 values retain their explicit offset.
func decodeMediaProjectionJSON(raw []byte, target any) error {
	var rows []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &rows); err != nil {
		return err
	}
	for _, row := range rows {
		for _, field := range []string{"updated_at", "created_at", "manual_atomization_requested_at", "last_progress_at", "publication_at"} {
			value, exists := row[field]
			if !exists || string(value) == "null" {
				continue
			}
			var stamp string
			if err := json.Unmarshal(value, &stamp); err != nil {
				return fmt.Errorf("invalid %s: %w", field, err)
			}
			if _, err := time.Parse(time.RFC3339Nano, stamp); err == nil {
				continue
			}
			parsed, err := time.ParseInLocation("2006-01-02T15:04:05.999999999", stamp, time.UTC)
			if err != nil {
				return fmt.Errorf("invalid %s: %w", field, err)
			}
			row[field], err = json.Marshal(parsed)
			if err != nil {
				return err
			}
		}
	}
	normalized, err := json.Marshal(rows)
	if err != nil {
		return err
	}
	return json.Unmarshal(normalized, target)
}
