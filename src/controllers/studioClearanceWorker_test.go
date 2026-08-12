package controllers

import (
	"testing"

	"github.com/google/uuid"
)

func TestStudioClearanceChildDigestIsOrderIndependent(t *testing.T) {
	first := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	second := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	if studioChildSetDigest([]uuid.UUID{first, second}) != studioChildSetDigest([]uuid.UUID{second, first}) {
		t.Fatal("exact Studio child set digest must be order independent")
	}
	if studioChildSetDigest([]uuid.UUID{first}) == studioChildSetDigest([]uuid.UUID{first, second}) {
		t.Fatal("Studio child set digest must bind every child")
	}
}
