package utils

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

func TestAdminPrincipalHasPermission(t *testing.T) {
	cases := []struct {
		name     string
		role     string
		roles    []string
		perms    []string
		required string
		want     bool
	}{
		{"admin role bypasses everything", "admin", []string{"admin"}, nil, "content:delete", true},
		{"exact permission granted", "editor", []string{"editor"}, []string{"content:write"}, "content:write", true},
		{"resource wildcard grants action", "manager", []string{"manager"}, []string{"source:*"}, "source:delete", true},
		{"global wildcard grants anything", "agent", []string{"agent"}, []string{"*:*"}, "feed:manage", true},
		{"plain user with no perms is denied", "user", []string{"user"}, []string{"feed:read"}, "content:write", false},
		{"unrelated permission is denied", "editor", []string{"editor"}, []string{"content:read"}, "source:write", false},
		{"empty required is denied", "editor", []string{"editor"}, []string{"content:write"}, "", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := AdminPrincipal{Role: tc.role, Roles: tc.roles, Permissions: tc.perms}
			if got := p.HasPermission(tc.required); got != tc.want {
				t.Fatalf("HasPermission(%q) = %v, want %v", tc.required, got, tc.want)
			}
		})
	}
}

func TestAdminPrincipalHasRole(t *testing.T) {
	p := AdminPrincipal{Role: "editor", Roles: []string{"editor", "agent"}}
	if !p.HasRole("admin") && p.HasRole("editor") && p.HasRole("agent") {
		// editor/agent present, admin absent — expected
	} else {
		t.Fatalf("unexpected HasRole results for %+v", p)
	}
	if p.HasRole("admin") {
		t.Fatalf("editor/agent principal should not have admin role")
	}
}

func TestIsAllowedIssuerRejectsEmpty(t *testing.T) {
	t.Setenv("JWT_ALLOWED_ISSUERS", "")
	if isAllowedIssuer("") {
		t.Fatal("empty issuer must be rejected")
	}
	if !isAllowedIssuer("iam-authorization-service") {
		t.Fatal("default IAM issuer must be allowed")
	}
	if isAllowedIssuer("evil-issuer") {
		t.Fatal("unknown issuer must be rejected")
	}
}

func TestHasAllowedAudience(t *testing.T) {
	// No allowlist configured → validation disabled (always true).
	t.Setenv("JWT_ALLOWED_AUDIENCES", "")
	if !hasAllowedAudience(&JWTClaims{}) {
		t.Fatal("audience check must be skipped when JWT_ALLOWED_AUDIENCES is unset")
	}

	t.Setenv("JWT_ALLOWED_AUDIENCES", "platform-console")
	matching := &JWTClaims{}
	matching.Audience = []string{"platform-console"}
	if !hasAllowedAudience(matching) {
		t.Fatal("matching audience must be allowed")
	}
	mismatch := &JWTClaims{}
	mismatch.Audience = []string{"some-other-app"}
	if hasAllowedAudience(mismatch) {
		t.Fatal("non-matching audience must be rejected")
	}
	if hasAllowedAudience(&JWTClaims{}) {
		t.Fatal("missing audience must be rejected when allowlist is set")
	}
}

func TestHasAllowedAudienceFailsClosedInProduction(t *testing.T) {
	t.Setenv("ENV", "production")
	t.Setenv("JWT_ALLOWED_AUDIENCES", "")
	if hasAllowedAudience(&JWTClaims{RegisteredClaims: jwt.RegisteredClaims{Audience: []string{"platform-console"}}}) {
		t.Fatal("production must reject human JWTs when no CMS audience is configured")
	}
}

func runWithPrincipal(t *testing.T, mw gin.HandlerFunc, principal *AdminPrincipal) int {
	t.Helper()
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/x", func(c *gin.Context) {
		if principal != nil {
			c.Set(AdminPrincipalContextKey, *principal)
		}
		mw(c)
		if !c.IsAborted() {
			c.Status(http.StatusOK)
		}
	})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	router.ServeHTTP(w, req)
	return w.Code
}

func TestRequireAdminPermissionMiddleware(t *testing.T) {
	mw := RequireAdminPermission("source", "write")

	// No principal in context → 401.
	if code := runWithPrincipal(t, mw, nil); code != http.StatusUnauthorized {
		t.Fatalf("missing principal: got %d, want 401", code)
	}
	// Plain user lacking the permission → 403.
	user := &AdminPrincipal{Role: "user", Roles: []string{"user"}, Permissions: []string{"feed:read"}}
	if code := runWithPrincipal(t, mw, user); code != http.StatusForbidden {
		t.Fatalf("unprivileged user: got %d, want 403", code)
	}
	// Admin role → allowed.
	admin := &AdminPrincipal{Role: "admin", Roles: []string{"admin"}}
	if code := runWithPrincipal(t, mw, admin); code != http.StatusOK {
		t.Fatalf("admin: got %d, want 200", code)
	}
	// Editor with the exact permission → allowed.
	editor := &AdminPrincipal{Role: "editor", Roles: []string{"editor"}, Permissions: []string{"source:write"}}
	if code := runWithPrincipal(t, mw, editor); code != http.StatusOK {
		t.Fatalf("editor with source:write: got %d, want 200", code)
	}
}

func TestRequireAdminRoleMiddleware(t *testing.T) {
	mw := RequireAdminRole("admin")

	manager := &AdminPrincipal{Role: "manager", Roles: []string{"manager"}, Permissions: []string{"*:*"}}
	if code := runWithPrincipal(t, mw, manager); code != http.StatusForbidden {
		t.Fatalf("manager (even with *:*) must be denied admin-role route: got %d, want 403", code)
	}
	admin := &AdminPrincipal{Role: "admin", Roles: []string{"admin"}}
	if code := runWithPrincipal(t, mw, admin); code != http.StatusOK {
		t.Fatalf("admin: got %d, want 200", code)
	}
}
