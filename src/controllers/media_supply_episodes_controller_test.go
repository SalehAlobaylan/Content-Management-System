package controllers

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestMediaSupplyCursorRoundTripAndRejectsMalformedInput(t *testing.T) {
	now := time.Date(2026, time.August, 9, 15, 0, 0, 0, time.UTC)
	id := uuid.New()
	raw := encodeMediaSupplyCursor(now, id)
	cursor, err := decodeMediaSupplyCursor(raw)
	if err != nil || !cursor.At.Equal(now) || cursor.ID != id {
		t.Fatalf("cursor round trip failed: %#v %v", cursor, err)
	}
	if _, err := decodeMediaSupplyCursor("not-a-cursor"); err == nil {
		t.Fatal("malformed cursor must be rejected")
	}
}
