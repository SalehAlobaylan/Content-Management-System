package utils

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Cursor encodes and decodes cursor tokens for pagination
// Format: base64(timestamp_nano:uuid)

// EncodeCursor creates a cursor from a timestamp and UUID
func EncodeCursor(publishedAt time.Time, id uuid.UUID) string {
	if publishedAt.IsZero() || id == uuid.Nil {
		return ""
	}
	raw := fmt.Sprintf("%d:%s", publishedAt.UnixNano(), id.String())
	return base64.URLEncoding.EncodeToString([]byte(raw))
}

// EncodeCursorWithView binds a pagination boundary to one immutable serving
// generation. The existing two-field cursor remains available to endpoints
// whose pagination is not generation scoped.
func EncodeCursorWithView(publishedAt time.Time, id, viewID uuid.UUID) string {
	if publishedAt.IsZero() || id == uuid.Nil || viewID == uuid.Nil {
		return EncodeCursor(publishedAt, id)
	}
	raw := fmt.Sprintf("%d:%s:%s", publishedAt.UnixNano(), id.String(), viewID.String())
	return base64.URLEncoding.EncodeToString([]byte(raw))
}

// DecodeCursor parses a cursor string back to timestamp and UUID
func DecodeCursor(cursor string) (time.Time, uuid.UUID, error) {
	timestamp, id, _, err := DecodeCursorWithView(cursor)
	return timestamp, id, err
}

// DecodeCursorWithView parses both legacy timestamp/UUID cursors and cursors
// pinned to a serving generation. A missing view is represented by uuid.Nil.
func DecodeCursorWithView(cursor string) (time.Time, uuid.UUID, uuid.UUID, error) {
	if cursor == "" {
		return time.Time{}, uuid.Nil, uuid.Nil, nil
	}

	decoded, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, uuid.Nil, uuid.Nil, fmt.Errorf("invalid cursor encoding: %w", err)
	}

	parts := strings.Split(string(decoded), ":")
	if len(parts) != 2 && len(parts) != 3 {
		return time.Time{}, uuid.Nil, uuid.Nil, fmt.Errorf("invalid cursor format")
	}

	nanoTimestamp, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return time.Time{}, uuid.Nil, uuid.Nil, fmt.Errorf("invalid cursor timestamp: %w", err)
	}

	id, err := uuid.Parse(parts[1])
	if err != nil {
		return time.Time{}, uuid.Nil, uuid.Nil, fmt.Errorf("invalid cursor uuid: %w", err)
	}
	var viewID uuid.UUID
	if len(parts) == 3 {
		viewID, err = uuid.Parse(parts[2])
		if err != nil || viewID == uuid.Nil {
			return time.Time{}, uuid.Nil, uuid.Nil, fmt.Errorf("invalid cursor view")
		}
	}

	return time.Unix(0, nanoTimestamp), id, viewID, nil
}

// CursorPagination holds cursor pagination parameters
type CursorPagination struct {
	Cursor    string
	Limit     int
	Timestamp time.Time
	LastID    uuid.UUID
	ViewID    uuid.UUID
}

// ParseCursorParams extracts cursor pagination from query params
func ParseCursorParams(cursorStr string, limitStr string) (*CursorPagination, error) {
	limit := 20 // default
	if limitStr != "" {
		parsed, err := strconv.Atoi(limitStr)
		if err == nil && parsed > 0 {
			if parsed > 50 {
				parsed = 50 // max limit for feeds
			}
			limit = parsed
		}
	}

	pagination := &CursorPagination{
		Cursor: cursorStr,
		Limit:  limit,
	}

	if cursorStr != "" {
		ts, id, viewID, err := DecodeCursorWithView(cursorStr)
		if err != nil {
			return nil, err
		}
		pagination.Timestamp = ts
		pagination.LastID = id
		pagination.ViewID = viewID
	}

	return pagination, nil
}
