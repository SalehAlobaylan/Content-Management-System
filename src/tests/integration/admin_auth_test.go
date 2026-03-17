package integration

import (
	"content-management-system/src/utils"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// generateTestJWT creates a JWT token compatible with CMS's ParseJWT for testing.
func generateTestJWT(t *testing.T, userID, email, role string, permissions []string) string {
	t.Helper()
	now := time.Now()
	claims := utils.JWTClaims{
		UserID:      userID,
		Email:       email,
		TenantID:    "default",
		Role:        role,
		Roles:       []string{role},
		Permissions: permissions,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID,
			Issuer:    "iam-authorization-service",
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(1 * time.Hour)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte("test_secret"))
	if err != nil {
		t.Fatalf("failed to sign test JWT: %v", err)
	}
	return signed
}

func TestAdminMeUnauthorized(t *testing.T) {
	clearTables()
	req := httptest.NewRequest(http.MethodGet, "/admin/me", nil)
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.Code)
	}
}

func TestAdminMeSuccess(t *testing.T) {
	clearTables()
	token := generateTestJWT(t, "550e8400-e29b-41d4-a716-446655440000", "admin@me.test", "admin", []string{"content:read", "content:write"})

	req := httptest.NewRequest(http.MethodGet, "/admin/me", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.Code, resp.Body.String())
	}

	var result map[string]any
	if err := json.Unmarshal(resp.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if result["email"] != "admin@me.test" {
		t.Fatalf("expected email admin@me.test, got %v", result["email"])
	}
	if result["role"] != "admin" {
		t.Fatalf("expected role admin, got %v", result["role"])
	}
}
