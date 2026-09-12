package controllers

import (
	"content-management-system/src/models"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"net/http/httptest"
	"testing"
	"time"
)

func TestInternalUnitClaimsExposeCredentialsOnlyInClaimDTO(t *testing.T) {
	token, fence := uuid.New(), uuid.New()
	expires := time.Now().UTC().Add(2 * time.Minute)
	chapter := models.AtomizationChapterUnit{ClaimToken: &token, FenceToken: &fence, LeaseExpiresAt: &expires, AttemptCount: 3}
	segment := models.TranscriptionSegmentUnit{ClaimToken: &token, FenceToken: &fence, LeaseExpiresAt: &expires, AttemptCount: 3}
	for _, tc := range []struct {
		name         string
		claim, model any
	}{
		{"chapter", newAtomizationUnitClaim(chapter), chapter},
		{"segment", newTranscriptionUnitClaim(segment), segment},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			context, _ := gin.CreateTestContext(recorder)
			context.JSON(200, gin.H{"unit": tc.claim})
			var response struct {
				Unit map[string]any `json:"unit"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if response.Unit["claim_token"] != token.String() || response.Unit["unit_fence_token"] != fence.String() || response.Unit["fence_token"] != fence.String() || response.Unit["attempt_count"] != float64(3) || response.Unit["lease_expires_at"] == nil {
				t.Fatalf("missing authoritative claim fields: %v", response.Unit)
			}
			raw, err := json.Marshal(tc.model)
			if err != nil {
				t.Fatal(err)
			}
			var ordinary map[string]any
			if err := json.Unmarshal(raw, &ordinary); err != nil {
				t.Fatal(err)
			}
			if _, exists := ordinary["claim_token"]; exists {
				t.Fatal("ordinary serialization leaked claim token")
			}
			if _, exists := ordinary["fence_token"]; exists {
				t.Fatal("ordinary serialization leaked fence token")
			}
		})
	}
}
