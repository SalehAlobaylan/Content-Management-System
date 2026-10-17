// Read-only qualification of a named environment. Never starts services,
// migrates, activates collection, replays paid work, or prints credentials.
package main

import (
	"content-management-system/src/experiencequalification"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"os"
	"path/filepath"
	"time"
)

func fail(message string) { fmt.Fprintln(os.Stderr, message); os.Exit(1) }
func main() {
	root := flag.String("root", "..", "project root")
	env := flag.String("env", "", "explicit named environment")
	flag.Parse()
	values, e := experiencequalification.LiteralProfile(*root, *env)
	if e != nil {
		fail(e.Error())
	}
	dsn, e := experiencequalification.CMSURL(values)
	if e != nil {
		fail(e.Error())
	}
	db, e := sql.Open("postgres", dsn)
	if e != nil {
		fail("CMS read connection unavailable")
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tx, e := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if e != nil {
		fail("read-only snapshot unavailable")
	}
	defer tx.Rollback()
	for _, migration := range []string{"20261016010000_platform_user_economics.sql", "20261017010000_economics_signed_invoices.sql", "20261017020000_economics_resumable_projection.sql"} {
		source, e := os.ReadFile(filepath.Join(*root, "Content-Management-System/migrations", migration))
		if e != nil {
			fail("canonical migration unavailable")
		}
		var checksum string
		if e := tx.QueryRowContext(ctx, `SELECT checksum_sha256 FROM cms_schema_migrations WHERE version=$1`, migration).Scan(&checksum); e != nil || checksum != fmt.Sprintf("%x", sha256.Sum256(source)) {
			fail("canonical economics migration has not been verified: " + migration)
		}
	}
	var finite bool
	if e := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='ai_spend_invoices'::regclass AND conname='ai_spend_invoices_amount_usd_finite')`).Scan(&finite); e != nil || !finite {
		fail("signed invoice validation unavailable")
	}
	report := map[string]any{"environment": *env, "read_only": true, "migrations": "three_applied_exact_checksum", "signed_invoice_constraint": true, "live_provider_match": false, "device_qualified": false}
	counts := map[string]int64{}
	for _, table := range []string{"economics_policies", "economics_attributions", "economics_cost_revisions", "economics_cost_summaries", "economics_usage_daily", "economics_collection_state", "economics_sources", "economics_lifecycle_barriers", "ai_spend_events", "ai_spend_rollups", "ai_spend_budgets", "ai_spend_stt_settlements"} {
		var n int64
		if e := tx.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&n); e != nil {
			fail("required owner/economics schema unavailable")
		}
		counts[table] = n
	}
	report["row_counts"] = counts
	var active struct{ Collection, Reports, Imports, Allocation int64 }
	if e := tx.QueryRowContext(ctx, `SELECT count(*) FILTER(WHERE collection_enabled),count(*) FILTER(WHERE reports_enabled),count(*) FILTER(WHERE imports_enabled),count(*) FILTER(WHERE allocation_enabled) FROM economics_policies`).Scan(&active.Collection, &active.Reports, &active.Imports, &active.Allocation); e != nil {
		fail("policy read unavailable")
	}
	report["enabled_tenant_counts"] = active
	var synced sql.NullTime
	if e := tx.QueryRowContext(ctx, `SELECT synced_at FROM economics_lifecycle_sync WHERE singleton=true`).Scan(&synced); e != nil {
		fail("lifecycle control state unavailable")
	}
	if synced.Valid {
		report["lifecycle_sync_at"] = synced.Time
	} else {
		report["lifecycle_sync_at"] = nil
	}
	var keyReady bool
	if e := tx.QueryRowContext(ctx, `SELECT octet_length(key)=32 FROM economics_lifecycle_keys WHERE singleton=true`).Scan(&keyReady); e != nil || !keyReady {
		fail("private lifecycle key unavailable")
	}
	report["lifecycle_key_ready"] = true
	rows, e := tx.QueryContext(ctx, `SELECT provider,spend_class,count(*),round(sum(cost_usd)::numeric,12)::text,sum(unpriced_events),sum(usage_unavailable_events) FROM ai_spend_rollups GROUP BY provider,spend_class ORDER BY provider,spend_class LIMIT 100`)
	if e != nil {
		fail("owner financial inventory unavailable")
	}
	providers := []map[string]any{}
	for rows.Next() {
		var provider, class, total string
		var n, unpriced, unknown int64
		if e := rows.Scan(&provider, &class, &n, &total, &unpriced, &unknown); e != nil {
			fail("owner inventory decode failed")
		}
		providers = append(providers, map[string]any{"provider": provider, "class": class, "rollup_rows": n, "priced_estimate_usd": total, "unpriced_events": unpriced, "usage_unavailable_events": unknown})
	}
	if rows.Err() != nil {
		fail("owner inventory incomplete")
	}
	rows.Close()
	report["owner_rollup_inventory"] = providers
	report["inventory_note"] = "Existing owner-priced history; not an actual provider invoice or proof of successful live qualification. No consumer identity is included."
	if e := json.NewEncoder(os.Stdout).Encode(report); e != nil {
		fail("report encoding failed")
	}
}
