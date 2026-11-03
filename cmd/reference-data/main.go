package main

import (
	rc "content-management-system/src/referencecatalog"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		os.Exit(2)
	}
	action := os.Args[1]
	switch action {
	case "build", "schema", "validate", "preview", "apply", "status":
	default:
		fmt.Fprintln(os.Stderr, "Unknown reference-data action")
		os.Exit(2)
	}
	f := flag.NewFlagSet(action, flag.ExitOnError)
	catalog := f.String("catalog", "reference-data", "catalog directory")
	tenant := f.String("tenant", "", "explicit tenant")
	env := f.String("env", "", "environment alias")
	out := f.String("out", "", "private output path")
	plan := f.String("plan", "", "reviewed plan")
	digest := f.String("digest", "", "reviewed digest")
	global := f.Bool("include-global", false, "explicit global quality scope")
	resolutions := f.String("resolutions", "", "per-key resolution JSON")
	keyMap := f.String("key-map", "", "explicit proposed-key to permanent-key JSON mapping")
	release := f.String("release", "1.0.0", "catalog release")
	f.Parse(os.Args[2:])
	if len(f.Args()) != 0 {
		fmt.Fprintln(os.Stderr, "Unexpected arguments")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	run := func() error {
		if action == "build" {
			if *tenant == "" || *out == "" {
				return fmt.Errorf("tenant and output required")
			}
			c, e := rc.Export(ctx, os.Getenv("REFERENCE_STABLE_DATABASE_URL"), os.Getenv("REFERENCE_DEV_DATABASE_URL"), *tenant, *release)
			if e != nil {
				return e
			}
			if *keyMap != "" {
				b, err := os.ReadFile(*keyMap)
				if err != nil {
					return err
				}
				mapping := map[string]string{}
				if err = rc.Decode(b, &mapping); err != nil {
					return err
				}
				if err = rc.RemapKeys(&c, mapping); err != nil {
					return err
				}
			}
			if e = rc.Write(*out, c); e != nil {
				return e
			}
			built, e := rc.Read(*out)
			if e != nil {
				return e
			}
			counts := map[string]int{}
			for _, r := range built.Records {
				counts[r.Kind]++
			}
			return json.NewEncoder(os.Stdout).Encode(map[string]any{"catalog": *out, "release": built.Manifest.ReleaseVersion, "counts": counts})
		}
		if action == "schema" {
			return json.NewEncoder(os.Stdout).Encode(rc.Schema())
		}
		c, e := rc.Read(*catalog)
		if e != nil {
			return e
		}
		if action == "validate" {
			fmt.Println("Catalog valid:", len(c.Records), "records")
			return nil
		}
		db, e := sql.Open("postgres", os.Getenv("DATABASE_URL"))
		if e != nil {
			return e
		}
		defer db.Close()
		decisions := map[string]string{}
		if *resolutions != "" {
			b, e := os.ReadFile(*resolutions)
			if e != nil {
				return e
			}
			if e = rc.Decode(b, &decisions); e != nil {
				return e
			}
		}
		switch action {
		case "preview", "status":
			p, e := rc.Preview(ctx, db, c, rc.Target(os.Getenv("DATABASE_URL")), *env, *tenant, *global, decisions)
			if e != nil {
				return e
			}
			if *out != "" {
				if e = rc.WritePlan(*out, p); e != nil {
					return e
				}
			}
			counts := map[string]int{}
			for _, a := range p.Actions {
				counts[a.State]++
			}
			json.NewEncoder(os.Stdout).Encode(map[string]any{"digest": p.Digest, "counts": counts})
			if rc.HasConflicts(p) {
				return fmt.Errorf("conflicts require explicit resolution")
			}
			return nil
		case "apply":
			p, e := rc.ReadPlan(*plan)
			if e != nil {
				return e
			}
			if p.Target != rc.Target(os.Getenv("DATABASE_URL")) || p.Environment != *env || p.Tenant != *tenant {
				return fmt.Errorf("reviewed target mismatch")
			}
			id, e := rc.Apply(ctx, db, c, p, *digest, "local-os:"+os.Getenv("USER"))
			if e == nil {
				fmt.Println("Application:", id)
			}
			return e
		default:
			return fmt.Errorf("unknown action")
		}
	}
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, "Reference data:", e)
		code := 1
		if strings.Contains(e.Error(), "conflict") || strings.Contains(e.Error(), "stale") {
			code = 3
		}
		os.Exit(code)
	}
}
