//go:build darwin || linux

package economics

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"content-management-system/src/utils"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

// This matched serving fixture includes JWT parsing and feed-sized JSON, but
// excludes a real feed query, remote database CPU and a separate load generator.
// It measures incremental middleware cost; it is not the production CPU gate.
func BenchmarkPrincipalServing(b *testing.B) {
	gin.SetMode(gin.ReleaseMode)
	b.Setenv("DEFAULT_TENANT_ID", "a")
	b.Setenv("JWT_ALLOWED_ISSUERS", "iam-authorization-service")
	b.Setenv("JWT_ALLOWED_AUDIENCES", "platform-console")
	secret := []byte("disposable-benchmark-key-never-a-service-credential")
	type item struct {
		ID, Title, Summary, PlaybackURL string
		Duration                        int
	}
	items := make([]item, 20)
	for i := range items {
		items[i] = item{fmt.Sprint(i), strings.Repeat("feed title ", 8), strings.Repeat("summary ", 100), "https://media.invalid/fixture.m3u8", 1800}
	}
	// JSON shape stays identical across both modes and every account population.
	if _, e := json.Marshal(items); e != nil {
		b.Fatal(e)
	}
	for _, population := range []int{64, 10000, 50000} {
		tokens := make([]string, population)
		for i := range tokens {
			id := fmt.Sprintf("00000000-0000-4000-8000-%012d", i)
			claims := utils.JWTClaims{UserID: id, TenantID: "a", Email: "fixture@example.invalid", Role: "user", RegisteredClaims: jwt.RegisteredClaims{Subject: id, Issuer: "iam-authorization-service", Audience: jwt.ClaimStrings{"platform-console"}, ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))}}
			token, e := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(secret)
			if e != nil {
				b.Fatal(e)
			}
			tokens[i] = "Bearer " + token
		}
		for _, enabled := range []bool{false, true} {
			b.Run(fmt.Sprintf("accounts_%d/enabled_%t", population, enabled), func(b *testing.B) {
				c := NewCollector()
				c.SetPolicies([]Policy{{TenantID: "a", CollectionEnabled: enabled}})
				app := gin.New()
				app.Use(c.Middleware())
				app.GET("/api/v1/feed/pods", func(ctx *gin.Context) {
					claims, e := utils.ParseJWT(strings.TrimPrefix(ctx.GetHeader("Authorization"), "Bearer "), secret)
					if e != nil {
						ctx.AbortWithStatus(401)
						return
					}
					ctx.Set("user_id", claims.UserID)
					ctx.Set("tenant_id", claims.TenantID)
					ctx.JSON(200, items)
				})
				var cursor atomic.Uint64
				before := servingCPU()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					req, _ := http.NewRequest("GET", "http://fixture.invalid/api/v1/feed/pods", nil)
					for pb.Next() {
						req.Header.Set("Authorization", tokens[cursor.Add(1)%uint64(len(tokens))])
						w := httptest.NewRecorder()
						app.ServeHTTP(w, req)
						if w.Code != 200 {
							b.Error("serving fixture rejected its verified token")
							return
						}
					}
				})
				b.StopTimer()
				b.ReportMetric(float64(servingCPU()-before)/float64(b.N), "process_cpu_ns/op")
				b.ReportMetric(float64(len(c.values)), "counter_keys")
				b.ReportMetric(float64(c.bytes), "counter_bytes")
				b.ReportMetric(float64(c.lost), "discarded_counters")
			})
		}
	}
}
func servingCPU() int64 {
	var r syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &r) != nil {
		return 0
	}
	return (r.Utime.Sec+r.Stime.Sec)*int64(time.Second) + (int64(r.Utime.Usec)+int64(r.Stime.Usec))*int64(time.Microsecond)
}
