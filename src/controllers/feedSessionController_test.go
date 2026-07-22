package controllers

import (
	"testing"

	"github.com/google/uuid"
)

func TestFrozenForYouSessionCursorRoundTrip(t *testing.T) {
	cursor := frozenSessionCursor(20, 50)
	if cursor == nil {
		t.Fatal("expected a cursor before the frozen snapshot is exhausted")
	}
	offset, err := parseFrozenSessionCursor(*cursor)
	if err != nil || offset != 20 {
		t.Fatalf("expected offset 20, got offset=%d err=%v", offset, err)
	}
	if cursor := frozenSessionCursor(50, 50); cursor != nil {
		t.Fatal("expected no cursor after the frozen snapshot is exhausted")
	}
}

func TestFrozenForYouSessionCursorRejectsMalformedValues(t *testing.T) {
	if _, err := parseFrozenSessionCursor("not-a-cursor"); err == nil {
		t.Fatal("expected malformed cursor to be rejected")
	}
}

func TestHasNewFrozenForYouCandidateOnlyReportsUnseenIDs(t *testing.T) {
	first := uuid.New()
	second := uuid.New()
	if hasNewFrozenForYouCandidate(
		[]ForYouItem{{ID: first}},
		[]ForYouItem{{ID: first}},
	) {
		t.Fatal("existing candidate must not claim freshness")
	}
	if !hasNewFrozenForYouCandidate(
		[]ForYouItem{{ID: first}},
		[]ForYouItem{{ID: first}, {ID: second}},
	) {
		t.Fatal("unseen candidate must claim freshness")
	}
}
