package controllers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
	"time"

	"content-management-system/src/utils"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

func TestFeedInspectionDBPreservesPodsResponseWithoutAccounting(t *testing.T) {
	if os.Getenv("CMS_TEST_ADMIN_URL") == "" && os.Getenv("CMS_TEST_DATABASE_URL") == "" {
		t.Skip("guarded disposable PostgreSQL required")
	}
	db := feedIntegrityTestDB(t)
	t.Setenv("DEFAULT_TENANT_ID", "default")
	integritySeedPublicFixture(t, db)
	router := gin.New()
	router.Use(func(c *gin.Context) { c.Set("db", db) })
	router.GET("/api/v1/feed/pods", GetPodsFeed)
	router.GET("/admin/feed-inspection/pods", func(c *gin.Context) {
		c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{
			UserID: uuid.NewString(), TenantID: "default", TenantClaimed: true,
		})
	}, FeedInspectionIdentity(), FeedInspectionConsumerScope, GetPodsFeed)
	fetch := func(path string) PodsResponse {
		t.Helper()
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path+"?limit=2&session_id=inspection-fixture", nil))
		var payload PodsResponse
		if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &payload) != nil || len(payload.Items) != 2 {
			t.Fatalf("feed response: %d %s", response.Code, response.Body.String())
		}
		return payload
	}

	baseline := integrityBusinessChecksum(t, db)
	inspect := fetch("/admin/feed-inspection/pods")
	time.Sleep(150 * time.Millisecond)
	if baseline != integrityBusinessChecksum(t, db) {
		t.Fatal("inspection changed feed accounting or business state")
	}
	public := fetch("/api/v1/feed/pods")
	if !reflect.DeepEqual(inspect.Items, public.Items) || (inspect.Cursor == nil) != (public.Cursor == nil) {
		t.Fatal("inspection changed the served items or paging availability")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if baseline != integrityBusinessChecksum(t, db) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the ordinary feed did not record accounting")
}

func TestFeedInspectionSeparatesOperatorFromConsumer(t *testing.T) {
	t.Setenv("DEFAULT_TENANT_ID", "tenant-a")
	t.Setenv("JWT_SECRET", "test-feed-inspection-secret")
	t.Setenv("JWT_ALLOWED_AUDIENCES", "feed-lab-test")
	consumerID := uuid.NewString()
	tokenFor := func(tenant string) string {
		t.Helper()
		token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, utils.JWTClaims{
			UserID: consumerID, TenantID: tenant, Role: "user", Email: "reader@example.test",
			RegisteredClaims: jwt.RegisteredClaims{Subject: consumerID, Issuer: "iam-authorization-service",
				Audience: jwt.ClaimStrings{"feed-lab-test"}, ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))},
		}).SignedString([]byte("test-feed-inspection-secret"))
		if err != nil {
			t.Fatal(err)
		}
		return "Bearer " + token
	}
	for _, tc := range []struct {
		name, tenant, consumer, expectedConsumer string
		validConsumer                            bool
		status                                   int
	}{
		{"anonymous", "tenant-a", "", "", false, 204},
		{"signed in", "tenant-a", tokenFor("tenant-a"), consumerID, true, 204},
		{"foreign consumer", "tenant-a", tokenFor("tenant-b"), "", true, 403},
		{"foreign operator", "tenant-b", "", "", false, 403},
		{"malformed consumer", "tenant-a", "Bearer bad-token", "", false, 401},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newMockGorm(t)
			operatorID := uuid.NewString()
			if tc.tenant == "tenant-a" {
				mock.ExpectQuery(`SELECT count\(\*\) FROM "auth_suspensions"`).
					WithArgs(operatorID).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
			}
			if tc.validConsumer {
				mock.ExpectQuery(`SELECT count\(\*\) FROM "auth_suspensions"`).
					WithArgs(consumerID).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
			}
			r := gin.New()
			r.GET("/observe", func(c *gin.Context) {
				c.Set("db", db)
				c.Set("user_id", operatorID)
				c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{
					UserID: operatorID, TenantID: tc.tenant, TenantClaimed: true,
				})
			}, FeedInspectionIdentity(), FeedInspectionConsumerScope, func(c *gin.Context) {
				if c.GetString("user_id") != tc.expectedConsumer || c.GetHeader("Authorization") != "" || !suppressFeedAccounting(c) {
					t.Fatal("operator identity or credential leaked into the consumer handler")
				}
				c.Status(http.StatusNoContent)
			})
			req := httptest.NewRequest("GET", "/observe?user_id="+operatorID, nil)
			req.Header.Set("Authorization", "Bearer operator")
			if tc.consumer != "" {
				req.Header.Set("X-Wahb-Consumer-Authorization", tc.consumer)
			}
			res := httptest.NewRecorder()
			r.ServeHTTP(res, req)
			if res.Code != tc.status {
				t.Fatalf("got %d, want %d: %s", res.Code, tc.status, res.Body.String())
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPublicFeedCannotRequestInspectionAccounting(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("GET", "/api/v1/feed/pods?inspect=true", nil)
	c.Request.Header.Set("X-Wahb-Consumer-Authorization", "Bearer fake")
	c.Request.Header.Set("X-Feed-Lab-Mode", "inspect")
	c.Request.Header.Set(feedIntegritySyntheticHdr, "fake")
	if suppressFeedAccounting(c) {
		t.Fatal("public headers suppressed feed accounting")
	}
}
