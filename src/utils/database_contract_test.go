package utils

import "testing"

func TestCompareCMSMigrationLedgerRequiresExactMembershipAndChecksums(t *testing.T) {
	files := map[string]string{"20260820000000_alpha.sql": "a", "20260820000001_beta.sql": "b"}
	if _, state := compareCMSMigrationLedger(files, []schemaMigrationRecord{{Version: "20260820000000_alpha.sql", Checksum: "a"}}); state != "incomplete" {
		t.Fatalf("expected incomplete, got %s", state)
	}
	if _, state := compareCMSMigrationLedger(files, []schemaMigrationRecord{{Version: "20260820000000_alpha.sql", Checksum: "wrong"}, {Version: "20260820000001_beta.sql", Checksum: "b"}}); state != "drifted" {
		t.Fatalf("expected drifted, got %s", state)
	}
	digest, state := compareCMSMigrationLedger(files, []schemaMigrationRecord{{Version: "20260820000000_alpha.sql", Checksum: "a"}, {Version: "20260820000001_beta.sql", Checksum: "b"}})
	if state != "verified" || len(digest) != 64 {
		t.Fatalf("expected verified digest, got state=%s digest=%q", state, digest)
	}
}
