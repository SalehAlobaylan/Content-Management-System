package utils

import (
	"crypto/subtle"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type machineCredential struct {
	principal    MachinePrincipal
	credentialID string
	secret       string
}

// InternalAuthMiddleware authenticates a named machine principal. It never
// grants access by itself: each route applies its capability policy below.
func InternalAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		principal, credentialID, ok := authenticateMachineToken(c.GetHeader("Authorization"))
		if !ok {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Invalid service credential"})
			return
		}
		c.Set(InternalPrincipalContextKey, principal)
		c.Set(InternalCredentialIDContextKey, credentialID)
		c.Next()
	}
}

// RequireInternalRoutePolicy authorizes the already-authenticated machine
// principal for one explicit table row and emits non-secret audit attribution.
func RequireInternalRoutePolicy(policy InternalRoutePolicy) gin.HandlerFunc {
	return func(c *gin.Context) {
		principal, ok := GetMachinePrincipal(c)
		if !ok || !policy.Allows(principal) {
			logInternalAuthorization(c, policy, principal, "denied")
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "Service principal lacks required capability"})
			return
		}
		logInternalAuthorization(c, policy, principal, "allowed")
		c.Next()
	}
}

func GetMachinePrincipal(c ContextReader) (MachinePrincipal, bool) {
	value, exists := c.Get(InternalPrincipalContextKey)
	principal, ok := value.(MachinePrincipal)
	return principal, exists && ok
}

func GetMachineCredentialID(c ContextReader) (string, bool) {
	value, exists := c.Get(InternalCredentialIDContextKey)
	id, ok := value.(string)
	return id, exists && ok
}

func authenticateMachineToken(header string) (MachinePrincipal, string, bool) {
	const bearer = "Bearer "
	if !strings.HasPrefix(header, bearer) {
		return "", "", false
	}
	presented := strings.TrimSpace(strings.TrimPrefix(header, bearer))
	if presented == "" {
		return "", "", false
	}
	for _, credential := range configuredMachineCredentials() {
		if credential.secret == "" || len(presented) != len(credential.secret) {
			continue
		}
		if subtle.ConstantTimeCompare([]byte(presented), []byte(credential.secret)) == 1 {
			return credential.principal, credential.credentialID, true
		}
	}
	return "", "", false
}

func configuredMachineCredentials() []machineCredential {
	credentials := make([]machineCredential, 0, 8)
	add := func(principal MachinePrincipal, env, id string) {
		if secret := strings.TrimSpace(os.Getenv(env)); secret != "" {
			credentials = append(credentials, machineCredential{principal: principal, credentialID: id, secret: secret})
		}
	}
	add(MachinePrincipalAggregation, "CMS_AGGREGATION_SERVICE_TOKEN", "aggregation/current")
	add(MachinePrincipalAggregation, "CMS_AGGREGATION_SERVICE_TOKEN_NEXT", "aggregation/next")
	add(MachinePrincipalEnrichment, "CMS_ENRICHMENT_SERVICE_TOKEN", "enrichment/current")
	add(MachinePrincipalEnrichment, "CMS_ENRICHMENT_SERVICE_TOKEN_NEXT", "enrichment/next")
	add(MachinePrincipalMedia, "CMS_MEDIA_SERVICE_TOKEN", "media/current")
	add(MachinePrincipalMedia, "CMS_MEDIA_SERVICE_TOKEN_NEXT", "media/next")
	add(MachinePrincipalIAM, "CMS_IAM_SERVICE_TOKEN", "iam/current")

	// The old broad token is a migration bridge only. Production requires an
	// explicit, future removal time; development remains convenient for the
	// local single-token stack. The route table controls the bridge's surface.
	if legacySharedCredentialEnabled() {
		add(MachinePrincipalLegacy, "CMS_SERVICE_TOKEN", "legacy-shared/deprecated")
	}
	return credentials
}

func legacySharedCredentialEnabled() bool {
	if strings.TrimSpace(os.Getenv("CMS_SERVICE_TOKEN")) == "" {
		return false
	}
	if strings.ToLower(strings.TrimSpace(os.Getenv("ENV"))) != "production" {
		return true
	}
	raw := strings.TrimSpace(os.Getenv("CMS_LEGACY_SERVICE_TOKEN_UNTIL"))
	until, err := time.Parse(time.RFC3339, raw)
	return err == nil && time.Now().Before(until)
}

func logInternalAuthorization(c *gin.Context, policy InternalRoutePolicy, principal MachinePrincipal, result string) {
	credentialID, _ := GetMachineCredentialID(c)
	requestID := c.GetHeader("X-Request-ID")
	tenantID := c.GetHeader("X-Tenant-ID")
	if tenantID == "" {
		tenantID = c.Query("tenant_id")
	}
	log.Printf("internal authorization result=%s principal=%s credential_id=%s capability=%s route=%s tenant=%s request_id=%s", result, principal, credentialID, policy.Capability, policy.Method+" "+policy.Path, tenantID, requestID)
}
