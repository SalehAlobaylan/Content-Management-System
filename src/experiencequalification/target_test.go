package experiencequalification

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func qualificationRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "environments", ".private"), 0700); err != nil {
		t.Fatal(err)
	}
	registry := map[string]any{"environments": []map[string]string{{"id": "test-rux", "preservation": "disposable"}, {"id": "kept-test", "preservation": "preserve"}}}
	data, _ := json.Marshal(registry)
	if err := os.WriteFile(filepath.Join(root, "environments", "registry.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestDisposableRequiresPreservationAndBothOwnerSeparation(t *testing.T) {
	root := qualificationRoot(t)
	values := map[string]string{"CMS_DATABASE_URL": "postgresql://fixture:dummy@ep-fixture.neon.tech/wahb_test_rux", "STORAGE_BUCKET": "rux-test", "STORAGE_ENDPOINT": "https://fixture.invalid"}
	for _, id := range []string{"stable", "dev", "production", "kept-test", "unregistered"} {
		if err := Disposable(root, id, values); err == nil {
			t.Fatalf("%s bypassed preservation proof", id)
		}
	}
	if err := Disposable(root, "test-rux", values); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "environments", ".private", "stable.env")
	if err := os.WriteFile(path, []byte("IAM_DATABASE_URL=postgresql://fixture:dummy@ep-fixture-pooler.neon.tech/wahb_test_rux\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := Disposable(root, "test-rux", values); err == nil {
		t.Fatal("IAM overlap through pooled alias passed")
	}
}

func TestLiteralProfileRejectsDuplicatesAndSymlinkedPrivateDirectory(t *testing.T) {
	root := qualificationRoot(t)
	private := filepath.Join(root, "environments", ".private")
	path := filepath.Join(private, "test-rux.env")
	if err := os.WriteFile(path, []byte("DATABASE_URL=first\nDATABASE_URL=second\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LiteralProfile(root, "test-rux"); err == nil {
		t.Fatal("duplicate keys accepted")
	}
	if err := os.Rename(private, private+"-original"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(private+"-original", private); err != nil {
		t.Fatal(err)
	}
	if _, err := LiteralProfile(root, "test-rux"); err == nil {
		t.Fatal("symlinked private directory accepted")
	}
}

func TestCMSURLMatchesCommonOwnerConflictRules(t *testing.T) {
	dsn := "postgresql://fixture:dummy@ep-fixture.neon.tech/wahb_test_rux"
	if _, err := CMSURL(map[string]string{"DATABASE_URL": dsn, "CMS_DATABASE_URL": dsn}); err != nil {
		t.Fatal(err)
	}
	if _, err := CMSURL(map[string]string{"DATABASE_URL": dsn, "CMS_DATABASE_URL": dsn + "-other"}); err == nil {
		t.Fatal("conflicting target accepted")
	}
}
