package economics

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

var lifecycleProof = struct {
	sync.Mutex
	at    map[*sql.DB]time.Time
	begun map[*sql.DB]bool
}{at: map[*sql.DB]time.Time{}, begun: map[*sql.DB]bool{}}

func markLifecycleReady(db *gorm.DB, at time.Time) {
	pool, e := db.DB()
	if e != nil {
		return
	}
	lifecycleProof.Lock()
	lifecycleProof.at[pool] = at
	lifecycleProof.Unlock()
}
func LifecycleReady(db *gorm.DB) bool {
	pool, e := db.DB()
	if e != nil {
		return false
	}
	lifecycleProof.Lock()
	at := lifecycleProof.at[pool]
	lifecycleProof.Unlock()
	return !at.IsZero() && time.Since(at) < 15*time.Minute
}

// Restore-safe replay uses IAM's retained deletion authority, independently of the restored CMS snapshot.
func SyncLifecycle(db *gorm.DB) error {
	db = privateDB(db)
	base := strings.TrimRight(os.Getenv("IAM_BASE_URL"), "/")
	token := os.Getenv("OPERATOR_IAM_ACCESS_SNAPSHOT_TOKEN")
	if token == "" {
		token = os.Getenv("CMS_SERVICE_TOKEN")
	}
	if base == "" || token == "" {
		return fmt.Errorf("IAM lifecycle authority unavailable")
	}
	pool, e := db.DB()
	if e != nil {
		return e
	}
	lifecycleProof.Lock()
	begun := lifecycleProof.begun[pool]
	lifecycleProof.Unlock()
	if !begun {
		// A restored snapshot cannot vouch for its own cursor. Replay IAM's
		// confirmed request authority once per process before exposing identities.
		if e := db.Transaction(func(tx *gorm.DB) error {
			if e := Lock(tx); e != nil {
				return e
			}
			return tx.Exec(`UPDATE economics_lifecycle_sync SET cursor_time='1970-01-01',cursor_user='00000000-0000-0000-0000-000000000000',synced_at=NULL WHERE singleton=true`).Error
		}); e != nil {
			return e
		}
		lifecycleProof.Lock()
		lifecycleProof.begun[pool] = true
		lifecycleProof.Unlock()
	}
	for page := 0; page < 10; page++ {
		var cursor struct {
			CursorTime time.Time
			CursorUser string
		}
		if e := db.Table("economics_lifecycle_sync").Where("singleton=true").Take(&cursor).Error; e != nil {
			return e
		}
		q := url.Values{"after": {cursor.CursorTime.UTC().Format(time.RFC3339Nano)}, "user_id": {cursor.CursorUser}}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		req, e := http.NewRequestWithContext(ctx, "GET", base+"/internal/access/economics/erasures?"+q.Encode(), nil)
		if e != nil {
			cancel()
			return e
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, e := (&http.Client{Timeout: 5 * time.Second}).Do(req)
		if e != nil {
			cancel()
			return fmt.Errorf("IAM lifecycle read unavailable")
		}
		var body struct {
			Erasures []struct {
				UserID   string    `json:"user_id"`
				TenantID string    `json:"tenant_id"`
				ErasedAt time.Time `json:"erased_at"`
			} `json:"erasures"`
			HasMore bool `json:"has_more"`
		}
		if resp.StatusCode != 200 {
			resp.Body.Close()
			cancel()
			return fmt.Errorf("IAM lifecycle read unavailable")
		}
		e = json.NewDecoder(io.LimitReader(resp.Body, 64*1024)).Decode(&body)
		resp.Body.Close()
		cancel()
		if e != nil || body.Erasures == nil || len(body.Erasures) > 100 || body.HasMore && len(body.Erasures) == 0 {
			return fmt.Errorf("Invalid lifecycle proof")
		}
		e = db.Transaction(func(tx *gorm.DB) error {
			if e := Lock(tx); e != nil {
				return e
			}
			ids, subjects := []string{}, []string{}
			for _, row := range body.Erasures {
				if _, e := uuid.Parse(row.UserID); e != nil {
					return e
				}
				if row.TenantID == "" || row.ErasedAt.IsZero() || row.ErasedAt.After(time.Now().Add(time.Minute)) || row.ErasedAt.Before(cursor.CursorTime) || row.ErasedAt.Equal(cursor.CursorTime) && row.UserID <= cursor.CursorUser {
					return fmt.Errorf("Invalid lifecycle order")
				}
				ids = append(ids, row.UserID)
				subjects = append(subjects, "account:"+row.UserID)
				cursor.CursorTime, cursor.CursorUser = row.ErasedAt, row.UserID
			}
			known, e := loadSubjectBarriers(tx, subjects)
			if e != nil {
				return e
			}
			pending := []string{}
			for _, id := range ids {
				if !known["account:"+id].Erased {
					pending = append(pending, id)
				}
			}
			if e := EraseMany(tx, pending); e != nil {
				return e
			}
			return tx.Exec(`UPDATE economics_lifecycle_sync SET cursor_time=?,cursor_user=?::uuid,synced_at=? WHERE singleton=true`, cursor.CursorTime, cursor.CursorUser, time.Now().UTC()).Error
		})
		if e != nil {
			return e
		}
		if !body.HasMore {
			markLifecycleReady(db, time.Now())
			return nil
		}
	}
	return fmt.Errorf("Lifecycle replay is still catching up; account reports withheld")
}
