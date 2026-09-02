package controllers

import (
	"testing"

	"github.com/gin-gonic/gin"
)

func TestRenditionGenerationLiteralTransitionStateIsAccepted(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(nil)
	c.Set("media_rendition_generation_state", "running")
	if state := renditionGenerationTransitionState(c); state != "running" {
		t.Fatalf("expected literal transition state, got %q", state)
	}
}

func TestRenditionGenerationNamedTransitionStateRemainsSupported(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(nil)
	c.Params = gin.Params{{Key: "state", Value: "verifying"}}
	if state := renditionGenerationTransitionState(c); state != "verifying" {
		t.Fatalf("expected named transition state, got %q", state)
	}
}
