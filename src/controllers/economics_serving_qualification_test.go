//go:build darwin || linux

package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"content-management-system/src/economics"
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Explicit, disposable qualification only. The external Python load generator
// keeps client CPU outside the measured Go process. This exercises real CMS
// handlers and local PostgreSQL, not remote Neon latency or the full stack.
func TestEconomicsServingQualification(t *testing.T) {
	if os.Getenv("CMS_ECONOMICS_SERVING_QUALIFICATION") != "1" {
		t.Skip("explicit isolated serving qualification required")
	}
	db := feedIntegrityTestDB(t)
	t.Setenv("DEFAULT_TENANT_ID", "default")
	t.Setenv("JWT_ALLOWED_ISSUERS", "iam-authorization-service")
	t.Setenv("JWT_ALLOWED_AUDIENCES", "platform-console")
	ids := integritySeedPublicFixture(t, db)
	var original models.ContentItem
	if e := db.Where("public_id=?", ids[0]).Take(&original).Error; e != nil {
		t.Fatal(e)
	}
	var generation string
	db.Raw("SELECT active_generation_id FROM feed_generation_heads WHERE tenant_id='default' AND lane='media'").Scan(&generation)
	for i := 0; i < 37; i++ {
		item := original
		item.ID = 0
		item.PublicID = uuid.New()
		item.IdempotencyKey = nil
		if e := db.Create(&item).Error; e != nil {
			t.Fatal(e)
		}
		if e := db.Create(&models.FeedGenerationMembership{GenerationID: uuid.MustParse(generation), MemberType: "feed_unit", MemberID: item.PublicID}).Error; e != nil {
			t.Fatal(e)
		}
	}
	var statements atomic.Int64
	for name, register := range map[string]func(string, func(*gorm.DB)) error{
		"query": func(n string, f func(*gorm.DB)) error {
			return db.Callback().Query().After("gorm:query").Register(n, f)
		},
		"raw": func(n string, f func(*gorm.DB)) error { return db.Callback().Raw().After("gorm:raw").Register(n, f) },
		"create": func(n string, f func(*gorm.DB)) error {
			return db.Callback().Create().After("gorm:create").Register(n, f)
		},
		"update": func(n string, f func(*gorm.DB)) error {
			return db.Callback().Update().After("gorm:update").Register(n, f)
		},
	} {
		if e := register("qualification:"+name, func(*gorm.DB) { statements.Add(1) }); e != nil {
			t.Fatal(e)
		}
	}
	secret := []byte("isolated-qualification-fixture-key")
	tokens := []string{}
	for i := 0; i < 64; i++ {
		id := uuid.NewString()
		claims := utils.JWTClaims{UserID: id, TenantID: "default", Email: "fixture@example.invalid", Role: "user", RegisteredClaims: jwt.RegisteredClaims{Subject: id, Issuer: "iam-authorization-service", Audience: jwt.ClaimStrings{"platform-console"}, ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))}}
		token, e := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(secret)
		if e != nil {
			t.Fatal(e)
		}
		tokens = append(tokens, token)
	}
	collector := economics.NewCollector()
	collector.SetPolicies([]economics.Policy{{TenantID: "default"}})
	var active atomic.Pointer[economics.Collector]
	active.Store(collector)
	gin.SetMode(gin.ReleaseMode)
	app := gin.New()
	app.Use(func(c *gin.Context) { c.Set("db", db); active.Load().Middleware()(c) })
	auth := func(c *gin.Context) {
		claims, e := utils.ParseJWT(strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer "), secret)
		if e != nil {
			c.AbortWithStatus(401)
			return
		}
		c.Set("user_id", claims.UserID)
		c.Set("tenant_id", claims.TenantID)
		c.Next()
	}
	app.GET("/api/v1/feed/pods", auth, GetPodsFeed)
	app.GET("/api/v1/content/:id", auth, GetContentItem)
	cpu := func() int64 {
		var r syscall.Rusage
		syscall.Getrusage(syscall.RUSAGE_SELF, &r)
		return (r.Utime.Sec+r.Stime.Sec)*1e9 + (int64(r.Utime.Usec)+int64(r.Stime.Usec))*1e3
	}
	var startCPU int64
	var startHeap uint64
	app.POST("/__phase", func(c *gin.Context) {
		enabled := c.Query("enabled") == "true"
		fresh := economics.NewCollector()
		fresh.SetPolicies([]economics.Policy{{TenantID: "default", CollectionEnabled: enabled}})
		active.Store(fresh)
		db.Model(&economics.Policy{}).Where("tenant_id='default'").Update("collection_enabled", enabled)
		statements.Store(0)
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		startHeap = m.HeapAlloc
		startCPU = cpu()
		c.Status(204)
	})
	app.GET("/__metrics", func(c *gin.Context) {
		elapsedCPU := cpu() - startCPU
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		sql := statements.Load()
		before := cpu()
		flushStart := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		err := active.Load().Flush(db.WithContext(ctx))
		flushWall := time.Since(flushStart).Seconds()
		flushCPU := cpu() - before
		flushSQL := statements.Load() - sql
		var rows int64
		db.Model(&economics.Usage{}).Count(&rows)
		message := ""
		if err != nil {
			message = err.Error()
		}
		c.JSON(200, gin.H{"server_cpu_ns": elapsedCPU, "heap_before": startHeap, "heap_after": m.HeapAlloc, "sql_statements": sql, "flush_cpu_ns": flushCPU, "flush_wall_seconds": flushWall, "flush_sql_statements": flushSQL, "flush_error": message, "persisted_usage_rows": rows})
	})
	server := httptest.NewServer(app)
	defer server.Close()
	payload, _ := json.Marshal(map[string]any{"url": server.URL, "tokens": tokens, "content": ids[0].String()})
	cmd := exec.Command("python3", "-c", economicsLoadClient)
	cmd.Stdin = strings.NewReader(string(payload))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if e := cmd.Run(); e != nil {
		t.Fatal(e)
	}
	fmt.Println("ECONOMICS_QUALIFICATION_COMPLETE: local PostgreSQL fixture; no provider calls or stable records")
}

const economicsLoadClient = `
import concurrent.futures, http.client, json, math, statistics, sys, time, urllib.parse
config=json.load(sys.stdin); target=urllib.parse.urlsplit(config['url'])
def control(path,method='GET'):
 c=http.client.HTTPConnection(target.hostname,target.port,timeout=30); c.request(method,path);r=c.getresponse();b=r.read();c.close();return json.loads(b) if b else None
# Warm real routes before measurement; every response must be useful and successful.
def request(i):
 c=http.client.HTTPConnection(target.hostname,target.port,timeout=20)
 path='/api/v1/feed/pods?limit=20' if i%5==0 else '/api/v1/content/'+config['content']
 started=time.perf_counter();c.request('GET',path,headers={'Authorization':'Bearer '+config['tokens'][i%64]});r=c.getresponse();b=r.read();elapsed=time.perf_counter()-started;c.close()
 if r.status!=200: raise RuntimeError('fixture request failed: '+str(r.status)+' '+b.decode()[:300])
 value=json.loads(b)
 if not value: raise RuntimeError('empty response')
 if i%5==0 and len(value.get('items',[]))!=20: raise RuntimeError('feed fixture did not return 20 items: '+str(len(value.get('items',[]))))
 return elapsed,len(b)
for i in range(10):request(i)
for rate in (5,25,75):
 for enabled in (False,True,True,False):
  control('/__phase?enabled='+str(enabled).lower(),'POST');samples=[];start=time.perf_counter();pending=[]
  with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
   for i in range(rate*8):
    delay=start+i/rate-time.perf_counter()
    if delay>0:time.sleep(delay)
    pending.append(pool.submit(request,i))
   samples=[f.result() for f in pending]
  wall=time.perf_counter()-start;metrics=control('/__metrics');values=sorted(x[0]*1000 for x in samples)
  result=dict(rate=rate,enabled=enabled,requests=len(samples),wall_seconds=wall,requests_per_second=len(samples)/wall,p50_ms=statistics.median(values),p95_ms=values[math.ceil(.95*len(values))-1],p99_ms=values[math.ceil(.99*len(values))-1],response_bytes=sum(x[1] for x in samples),**metrics)
  print('ECONOMICS_MEASUREMENT '+json.dumps(result),flush=True)
  if metrics['flush_error']:raise RuntimeError(metrics['flush_error'])
`
