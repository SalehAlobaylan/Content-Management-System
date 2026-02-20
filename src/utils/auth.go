package utils

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

// JWTClaims defines token claims accepted by CMS admin middleware.
// It supports both legacy CMS-issued tokens and IAM-issued canonical tokens.
type JWTClaims struct {
	UserID      string   `json:"user_id,omitempty"`
	Email       string   `json:"email"`
	TenantID    string   `json:"tenant_id,omitempty"`
	Role        string   `json:"role,omitempty"`
	Roles       []string `json:"roles,omitempty"`
	Permissions []string `json:"permissions,omitempty"`
	jwt.RegisteredClaims
}

type AdminPrincipal struct {
	UserID      string
	Email       string
	TenantID    string
	Role        string
	Roles       []string
	Permissions []string
	Issuer      string
	ExpiresAt   time.Time
}

const AdminPrincipalContextKey = "admin_principal"

var (
	ErrTokenExpired          = errors.New("token expired")
	ErrTokenInvalid          = errors.New("token invalid")
	ErrTokenSignatureInvalid = errors.New("token signature invalid")
)

func HashPassword(password string) (string, error) {
	hashed, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hashed), nil
}

func CheckPassword(hash string, password string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
}

func GetJWTSecret() ([]byte, error) {
	secret := strings.TrimSpace(os.Getenv("JWT_SECRET"))
	if secret == "" {
		return nil, fmt.Errorf("JWT_SECRET is not set")
	}
	return []byte(secret), nil
}

func GetJWTIssuer() string {
	if issuer := strings.TrimSpace(os.Getenv("JWT_ISSUER")); issuer != "" {
		return issuer
	}
	return "cms-service"
}

func GetJWTAudience() string {
	if audience := strings.TrimSpace(os.Getenv("JWT_AUDIENCE")); audience != "" {
		return audience
	}
	return "platform-console"
}

func GetJWTAllowedIssuers() []string {
	allowed := strings.TrimSpace(os.Getenv("JWT_ALLOWED_ISSUERS"))
	if allowed == "" {
		return []string{"cms-service", "iam-authorization-service"}
	}

	parts := strings.Split(allowed, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		value := strings.TrimSpace(strings.ToLower(part))
		if value == "" {
			continue
		}
		out = append(out, value)
	}
	if len(out) == 0 {
		return []string{"cms-service", "iam-authorization-service"}
	}
	return out
}

func IsTenantClaimRequired() bool {
	required := strings.TrimSpace(strings.ToLower(os.Getenv("JWT_REQUIRE_TENANT_ID")))
	return required == "1" || required == "true" || required == "yes"
}

func GetDefaultTenantID() string {
	if tenantID := strings.TrimSpace(os.Getenv("DEFAULT_TENANT_ID")); tenantID != "" {
		return tenantID
	}
	return "default"
}

func GetJWTExpiration() time.Duration {
	hoursValue := os.Getenv("JWT_EXPIRATION_HOURS")
	if hoursValue == "" {
		return 24 * time.Hour
	}
	hours, err := strconv.Atoi(hoursValue)
	if err != nil || hours <= 0 {
		return 24 * time.Hour
	}
	return time.Duration(hours) * time.Hour
}

func GenerateJWT(userID, email, tenantID, role string, permissions []string) (string, error) {
	secret, err := GetJWTSecret()
	if err != nil {
		return "", err
	}

	normalizedRole := strings.ToLower(strings.TrimSpace(role))
	if normalizedRole == "" {
		normalizedRole = "user"
	}
	normalizedTenant := strings.TrimSpace(tenantID)
	if normalizedTenant == "" {
		normalizedTenant = GetDefaultTenantID()
	}

	now := time.Now()
	claims := JWTClaims{
		UserID:      userID,
		Email:       strings.ToLower(strings.TrimSpace(email)),
		TenantID:    normalizedTenant,
		Role:        normalizedRole,
		Roles:       []string{normalizedRole},
		Permissions: normalizePermissions(permissions),
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID,
			Issuer:    GetJWTIssuer(),
			Audience:  []string{GetJWTAudience()},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(GetJWTExpiration())),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(secret)
}

func ParseJWT(tokenString string, secret []byte) (*JWTClaims, error) {
	claims := &JWTClaims{}
	token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return secret, nil
	})

	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, ErrTokenExpired
		}
		if errors.Is(err, jwt.ErrTokenSignatureInvalid) {
			return nil, ErrTokenSignatureInvalid
		}
		return nil, ErrTokenInvalid
	}
	if !token.Valid {
		return nil, ErrTokenInvalid
	}

	if !isAllowedIssuer(claims.Issuer) {
		return nil, ErrTokenInvalid
	}

	normalizeClaims(claims)

	if claims.Subject == "" || claims.Email == "" || claims.Role == "" {
		return nil, ErrTokenInvalid
	}
	if IsTenantClaimRequired() && strings.TrimSpace(claims.TenantID) == "" {
		return nil, ErrTokenInvalid
	}

	return claims, nil
}

func BuildAdminPrincipal(claims *JWTClaims) AdminPrincipal {
	tenantID := strings.TrimSpace(claims.TenantID)
	if tenantID == "" {
		tenantID = GetDefaultTenantID()
	}

	expiry := time.Time{}
	if claims.ExpiresAt != nil {
		expiry = claims.ExpiresAt.Time
	}

	return AdminPrincipal{
		UserID:      claims.UserID,
		Email:       claims.Email,
		TenantID:    tenantID,
		Role:        claims.Role,
		Roles:       claims.Roles,
		Permissions: claims.Permissions,
		Issuer:      claims.Issuer,
		ExpiresAt:   expiry,
	}
}

func GetAdminPrincipal(c ContextReader) (AdminPrincipal, bool) {
	value, exists := c.Get(AdminPrincipalContextKey)
	if !exists {
		return AdminPrincipal{}, false
	}
	principal, ok := value.(AdminPrincipal)
	return principal, ok
}

// ContextReader is a minimal interface implemented by gin.Context for testing.
type ContextReader interface {
	Get(key string) (value interface{}, exists bool)
}

func (p AdminPrincipal) HasRole(role string) bool {
	target := strings.ToLower(strings.TrimSpace(role))
	if target == "" {
		return false
	}
	if strings.EqualFold(p.Role, target) {
		return true
	}
	for _, candidate := range p.Roles {
		if strings.EqualFold(candidate, target) {
			return true
		}
	}
	return false
}

func (p AdminPrincipal) HasPermission(permission string) bool {
	required := strings.ToLower(strings.TrimSpace(permission))
	if required == "" {
		return false
	}
	if p.HasRole("admin") {
		return true
	}

	for _, granted := range p.Permissions {
		normalized := strings.ToLower(strings.TrimSpace(granted))
		if normalized == required || normalized == "*:*" {
			return true
		}

		requiredParts := strings.Split(required, ":")
		grantedParts := strings.Split(normalized, ":")
		if len(requiredParts) == 2 && len(grantedParts) == 2 {
			if grantedParts[0] == requiredParts[0] && grantedParts[1] == "*" {
				return true
			}
		}
	}
	return false
}

func normalizeClaims(claims *JWTClaims) {
	if claims.UserID == "" {
		claims.UserID = strings.TrimSpace(claims.Subject)
	}
	claims.Email = strings.ToLower(strings.TrimSpace(claims.Email))
	claims.Role = strings.ToLower(strings.TrimSpace(claims.Role))

	normalizedRoles := make([]string, 0, len(claims.Roles))
	for _, role := range claims.Roles {
		normalizedRole := strings.ToLower(strings.TrimSpace(role))
		if normalizedRole == "" {
			continue
		}
		if !slices.Contains(normalizedRoles, normalizedRole) {
			normalizedRoles = append(normalizedRoles, normalizedRole)
		}
	}
	if claims.Role != "" && !slices.Contains(normalizedRoles, claims.Role) {
		normalizedRoles = append(normalizedRoles, claims.Role)
	}
	if claims.Role == "" && len(normalizedRoles) > 0 {
		claims.Role = primaryRole(normalizedRoles)
	}
	if len(normalizedRoles) == 0 && claims.Role != "" {
		normalizedRoles = append(normalizedRoles, claims.Role)
	}
	claims.Roles = normalizedRoles
	claims.Permissions = normalizePermissions(claims.Permissions)
}

func primaryRole(roles []string) string {
	priority := []string{"admin", "manager", "agent", "user"}
	for _, candidate := range priority {
		if slices.Contains(roles, candidate) {
			return candidate
		}
	}
	if len(roles) == 0 {
		return "user"
	}
	return roles[0]
}

func normalizePermissions(permissions []string) []string {
	normalized := make([]string, 0, len(permissions))
	for _, permission := range permissions {
		candidate := strings.ToLower(strings.TrimSpace(permission))
		if candidate == "" || slices.Contains(normalized, candidate) {
			continue
		}
		normalized = append(normalized, candidate)
	}
	return normalized
}

func isAllowedIssuer(issuer string) bool {
	normalized := strings.ToLower(strings.TrimSpace(issuer))
	if normalized == "" {
		return true
	}
	return slices.Contains(GetJWTAllowedIssuers(), normalized)
}
