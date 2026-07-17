package testdb

import "testing"

func TestValidateDisposableDSNRejectsUnsafeTargetsBeforeConnection(t *testing.T) {
	marker := "I_UNDERSTAND_THIS_DATABASE_IS_DISPOSABLE"
	valid := "postgres://test:secret@localhost:5432/wahb_cms_test_a1b2c3d4?sslmode=disable"
	for name, value := range map[string]struct{ raw, runtime string }{
		"missing": {"", ""},
		"runtime": {valid, valid},
		"same runtime target with different credentials": {
			valid,
			"postgres://different:credentials@localhost:5432/wahb_cms_test_a1b2c3d4?connect_timeout=1",
		},
		"supabase":  {"postgres://x:y@db.supabase.co/wahb_cms_test_a1b2c3d4", ""},
		"neon":      {"postgres://x:y@ep.neon.tech/wahb_cms_test_a1b2c3d4", ""},
		"weak name": {"postgres://x:y@localhost/cms_test", ""},
		"malformed": {"://not a dsn", ""},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ValidateDisposableDSN(value.raw, value.runtime, marker); err == nil {
				t.Fatal("unsafe target was accepted")
			}
		})
	}
	target, err := ValidateDisposableDSN(valid, "", marker)
	if err != nil {
		t.Fatal(err)
	}
	if target.Host != "localhost" || target.Database != "wahb_cms_test_a1b2c3d4" {
		t.Fatalf("unsafe diagnostic target: %#v", target)
	}
}

func TestDatabaseNameRejectsAnythingOutsideGeneratedDisposableNamespace(t *testing.T) {
	if _, err := databaseName("postgres://x:y@localhost/wahb_cms_test_a1b2c3d4"); err != nil {
		t.Fatal(err)
	}
	if _, err := databaseName("postgres://x:y@localhost/wahb_cms_test_shared"); err == nil {
		t.Fatal("weak reusable database name accepted for destructive cleanup")
	}
}

func TestValidateAdminDSNRejectsUnsafeHostsBeforeConnection(t *testing.T) {
	marker := "I_UNDERSTAND_THIS_DATABASE_IS_DISPOSABLE"
	for _, dsn := range []string{
		"",
		"mysql://localhost/test",
		"postgres://x:y@db.supabase.co/postgres",
		"postgres://x:y@ep.neon.tech/postgres",
	} {
		if err := validateAdminDSN(dsn, marker); err == nil {
			t.Fatalf("unsafe admin target accepted: %q", dsn)
		}
	}
	if err := validateAdminDSN("postgres://x:y@localhost:5432/postgres?sslmode=disable", marker); err != nil {
		t.Fatal(err)
	}
}
