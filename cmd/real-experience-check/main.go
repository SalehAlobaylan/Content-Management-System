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
	"regexp"
	"time"
)

func fail(message string) { fmt.Fprintln(os.Stderr, message); os.Exit(1) }
func main() {
	root := flag.String("root", "..", "project root")
	id := flag.String("env", "", "explicit environment ID")
	since := flag.String("since", "", "optional RFC3339 start for redacted interaction delta evidence")
	readOnly := flag.Bool("read-only", false, "read-only schema and preservation snapshot")
	disposable := flag.Bool("disposable-proof", false, "verify disposable target without writing")
	flag.Parse()
	values, err := experiencequalification.LiteralProfile(*root, *id)
	if err != nil {
		fail(err.Error())
	}
	dsn, err := experiencequalification.CMSURL(values)
	if err != nil {
		fail(err.Error())
	}
	if *disposable {
		if err := experiencequalification.Disposable(*root, *id, values); err != nil {
			fail(err.Error())
		}
		fmt.Println("Disposable database/storage separation checks passed; owner doctor still required.")
		return
	}
	if !*readOnly {
		fail("select --read-only or --disposable-proof")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		fail("CMS read connection unavailable")
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		fail("CMS read-only transaction unavailable")
	}
	defer tx.Rollback()
	report := map[string]any{"environment": *id, "read_only": true, "schema_qualified": false, "native_schema_qualified": false}
	counts := map[string]int64{}
	for _, table := range []string{"content_items", "user_interactions", "experience_events", "experience_metric_rollups", "experience_incidents", "experience_actions", "experience_evaluation_runs"} {
		var exists bool
		if err := tx.QueryRowContext(ctx, "SELECT to_regclass($1) IS NOT NULL", table).Scan(&exists); err != nil {
			fail("schema inspection failed")
		}
		if exists {
			var count int64
			if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
				fail("preservation snapshot failed")
			}
			counts[table] = count
		}
	}
	report["row_counts"] = counts
	if *since != "" {
		start, err := time.Parse(time.RFC3339, *since)
		if err != nil {
			fail("invalid inspection start")
		}
		rows, err := tx.QueryContext(ctx, `SELECT type, user_id IS NOT NULL, COUNT(*), MIN(created_at), MAX(created_at), BOOL_OR(content_item_id::text LIKE '00000000-0000-4000-8000-%') FROM user_interactions WHERE created_at >= $1 GROUP BY type,user_id IS NOT NULL ORDER BY type`, start)
		if err != nil {
			fail("interaction delta read unavailable")
		}
		groups := []map[string]any{}
		for rows.Next() {
			var kind string
			var authenticated, fixtureReference bool
			var count int64
			var first, last time.Time
			if rows.Scan(&kind, &authenticated, &count, &first, &last, &fixtureReference) != nil {
				fail("interaction delta decode unavailable")
			}
			groups = append(groups, map[string]any{"interaction_type": kind, "authenticated": authenticated, "count": count, "first_at": first, "last_at": last, "fixture_content_reference": fixtureReference})
		}
		if rows.Err() != nil {
			fail("interaction delta iteration unavailable")
		}
		rows.Close()
		report["interaction_delta"] = groups
	}

	migration := "20261013010000_real_experience_completion.sql"
	bytes, err := os.ReadFile(filepath.Join(*root, "Content-Management-System", "migrations", migration))
	if err != nil {
		fail("canonical SQL unavailable")
	}
	expected := fmt.Sprintf("%x", sha256.Sum256(bytes))
	var actual string
	err = tx.QueryRowContext(ctx, "SELECT checksum_sha256 FROM cms_schema_migrations WHERE version=$1", migration).Scan(&actual)
	if err == sql.ErrNoRows {
		report["migration_status"] = "pending"
	} else if err != nil {
		fail("canonical ledger inspection failed")
	} else if actual != expected {
		fail("canonical migration checksum differs")
	} else {
		var checks bool
		err = tx.QueryRowContext(ctx, `SELECT
   EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='experience_metric_rollups' AND column_name='histogram_version' AND column_default='1') AND
   EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='experience_metric_rollups' AND column_name='completed' AND column_default='false') AND
   EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='experience_metric_rollups' AND column_name='outcome_count' AND column_default='0') AND
   EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='experience_policies' AND column_name='evaluation_enabled' AND column_default='false') AND
   EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='experience_events' AND column_name='phase') AND
   to_regclass('experience_terminal_receipts') IS NOT NULL AND to_regclass('experience_rollup_progress') IS NOT NULL AND
   to_regclass('experience_incidents_active_fingerprint') IS NOT NULL AND to_regclass('experience_events_window_cursor') IS NOT NULL`).Scan(&checks)
		if err != nil || !checks {
			fail("canonical RUX schema postconditions missing")
		}
		report["schema_qualified"] = true
		report["migration_status"] = "applied_exact_checksum"
	}
	nativeMigration := "20261013020000_real_experience_native.sql"
	nativeSQL, err := os.ReadFile(filepath.Join(*root, "Content-Management-System", "migrations", nativeMigration))
	if err != nil {
		fail("native canonical SQL unavailable")
	}
	nativeExpected := fmt.Sprintf("%x", sha256.Sum256(nativeSQL))
	nativeActual := ""
	err = tx.QueryRowContext(ctx, "SELECT checksum_sha256 FROM cms_schema_migrations WHERE version=$1", nativeMigration).Scan(&nativeActual)
	if err == sql.ErrNoRows {
		report["native_migration_status"] = "pending"
	} else if err != nil || nativeExpected != nativeActual {
		fail("native canonical checksum unavailable or differs")
	} else {
		var ready bool
		err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='experience_events' AND column_name='client_platform') AND EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='experience_events' AND column_name='os_major') AND EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='experience_incidents' AND column_name='client_platform') AND EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='experience_suppressions' AND column_name='client_platform') AND EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='experience_policies' AND column_name='enabled_clients' AND column_default LIKE '%web%') AND EXISTS(SELECT 1 FROM pg_indexes WHERE tablename='experience_metric_rollups' AND indexname='idx_experience_rollups_identity' AND indexdef LIKE '%client_platform%')`).Scan(&ready)
		if err != nil || !ready {
			fail("native schema postconditions missing")
		}
		report["native_migration_status"] = "applied_exact_checksum"
		report["native_schema_qualified"] = true
		var clients string
		if tx.QueryRowContext(ctx, "SELECT enabled_clients FROM experience_policies WHERE tenant_id='default'").Scan(&clients) == nil {
			report["enabled_clients"] = clients
		}
	}

	var ingest, evaluation bool
	err = tx.QueryRowContext(ctx, "SELECT ingest_enabled,evaluation_enabled FROM experience_policies WHERE tenant_id='default'").Scan(&ingest, &evaluation)
	if err == nil {
		report["policy"] = map[string]bool{"ingest_enabled": ingest, "evaluation_enabled": evaluation}
	} else if err == sql.ErrNoRows {
		report["policy_status"] = "not_created"
	} else {
		fail("policy read unavailable")
	}
	release := values["PLATFORM__NEXT_PUBLIC_APP_RELEASE"]
	if release == "" {
		release = values["PLATFORM__NEXT_PUBLIC_COMMIT_SHA"]
	}
	report["release_configured"] = release != "" && release != "dev" && regexp.MustCompile(`^[A-Za-z0-9._-]{1,80}$`).MatchString(release)
	report["ingest_secret_pair_configured"] = values["CMS__RUX_INGEST_TOKEN"] != "" && values["CMS__RUX_INGEST_TOKEN"] == values["PLATFORM__RUX_INGEST_TOKEN"]
	json.NewEncoder(os.Stdout).Encode(report)
}
