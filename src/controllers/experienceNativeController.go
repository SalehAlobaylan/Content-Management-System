package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"
)

// A ticket grants only bounded telemetry admission. It is not a user JWT and
// cannot authorize business APIs or attest an installed binary's provenance.
type nativeExperienceTicket struct {
	Platform string `json:"platform"`
	Session  string `json:"session_id"`
	Page     string `json:"page_load_id"`
	Release  string `json:"release"`
	Expires  int64  `json:"expires"`
	Runtime  string `json:"runtime,omitempty"`
}

var nativeExperienceBootstrapLimiter = utils.NewTelemetryRateLimiter(60, time.Minute, 50000)

func experienceClientEnabled(policy models.ExperiencePolicy, platform string) bool {
	clients := policy.EnabledClients
	if clients == "" {
		clients = "web"
	}
	for _, client := range strings.Split(clients, ",") {
		if client == models.RuxPlatform(platform) {
			return true
		}
	}
	return false
}
func nativeTicketSignature(payload, key string) []byte {
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte("wahb-rux-native/v1\n" + payload))
	return mac.Sum(nil)
}
func issueNativeTicket(ticket nativeExperienceTicket, key string) string {
	raw, _ := json.Marshal(ticket)
	payload := base64.RawURLEncoding.EncodeToString(raw)
	return payload + "." + base64.RawURLEncoding.EncodeToString(nativeTicketSignature(payload, key))
}
func readNativeTicket(value, key string, now time.Time) (nativeExperienceTicket, bool) {
	var ticket nativeExperienceTicket
	if key == "" || len(value) > 1024 {
		return ticket, false
	}
	payload, signature, ok := strings.Cut(value, ".")
	if !ok {
		return ticket, false
	}
	sig, err := base64.RawURLEncoding.DecodeString(signature)
	if err != nil || !hmac.Equal(sig, nativeTicketSignature(payload, key)) {
		return ticket, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil || json.Unmarshal(raw, &ticket) != nil || ticket.Expires <= now.Unix() || ticket.Expires > now.Add(31*time.Minute).Unix() {
		return ticket, false
	}
	if ticket.Platform != "ios" && ticket.Platform != "android" {
		return ticket, false
	}
	if _, err := uuid.Parse(ticket.Session); err != nil {
		return ticket, false
	}
	if _, err := uuid.Parse(ticket.Page); err != nil {
		return ticket, false
	}
	if !regexp.MustCompile(`^[A-Za-z0-9._-]{1,80}$`).MatchString(ticket.Release) {
		return ticket, false
	}
	return ticket, true
}
func CreateNativeExperienceAdmission(c *gin.Context) {
	key := strings.TrimSpace(os.Getenv("RUX_INGEST_TOKEN"))
	if key == "" {
		c.JSON(200, gin.H{"disabled": true})
		return
	}
	// Use the transport peer, never a client-supplied forwarding/rate header.
	peer := base64.RawURLEncoding.EncodeToString(nativeTicketSignature(c.RemoteIP(), key))
	if allowed, _ := nativeExperienceBootstrapLimiter.AllowN(peer, 1); !allowed {
		c.JSON(429, gin.H{"error": "rate limited"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 2048)
	var body nativeExperienceTicket
	if c.ShouldBindJSON(&body) != nil || (body.Platform != "ios" && body.Platform != "android") || !regexp.MustCompile(`^[A-Za-z0-9._-]{1,80}$`).MatchString(body.Release) {
		c.JSON(400, gin.H{"error": "invalid native admission"})
		return
	}
	if _, err := uuid.Parse(body.Session); err != nil {
		c.JSON(400, gin.H{"error": "invalid process session"})
		return
	}
	if _, err := uuid.Parse(body.Page); err != nil {
		c.JSON(400, gin.H{"error": "invalid process epoch"})
		return
	}
	policy, err := loadExperiencePolicy(c.MustGet("db").(*gorm.DB), ruxDefaultTenant)
	if err != nil {
		c.JSON(503, gin.H{"error": "telemetry policy unavailable"})
		return
	}
	if !policy.IngestEnabled || !experienceClientEnabled(policy, body.Platform) {
		c.JSON(200, gin.H{"disabled": true})
		return
	}
	body.Runtime = strings.TrimSpace(os.Getenv("WAHB_RUNTIME_ID"))
	body.Expires = time.Now().UTC().Add(30 * time.Minute).Unix()
	c.JSON(200, gin.H{"ticket": issueNativeTicket(body, key), "expires_at": body.Expires, "runtime_id": body.Runtime})
}
func NativeExperienceAdmissionMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		ticket, ok := readNativeTicket(c.GetHeader("X-RUX-Native-Ticket"), strings.TrimSpace(os.Getenv("RUX_INGEST_TOKEN")), time.Now().UTC())
		if ticket.Expires > 0 && ticket.Runtime != strings.TrimSpace(os.Getenv("WAHB_RUNTIME_ID")) {
			c.AbortWithStatusJSON(409, gin.H{"disabled": true, "error": "stale telemetry runtime"})
			return
		}
		if !ok {
			c.AbortWithStatusJSON(401, gin.H{"error": "native telemetry admission required"})
			return
		}
		c.Set("rux_native_ticket", ticket)
		// Reissued tickets share a process budget; arbitrary client rate keys have
		// no influence. Session scope and client/build claims are ticket-bound.
		c.Request.Header.Set(ruxRateKeyHeader, "native:"+ticket.Platform+":"+ticket.Session)
		c.Next()
	}
}

func experienceClient(clients []string) string {
	if len(clients) > 0 {
		return models.RuxPlatform(clients[0])
	}
	return "web"
}
func enabledExperienceClients(policy models.ExperiencePolicy) []string {
	raw := policy.EnabledClients
	if raw == "" {
		raw = "web"
	}
	out := []string{}
	for _, v := range strings.Split(raw, ",") {
		if models.RuxClientPlatforms[v] {
			out = append(out, v)
		}
	}
	return out
}
func requestedExperienceClient(c *gin.Context) (string, bool) {
	value := c.DefaultQuery("client_platform", "web")
	if !models.RuxClientPlatforms[value] {
		c.JSON(400, gin.H{"error": "unsupported client platform"})
		return "", false
	}
	return value, true
}
