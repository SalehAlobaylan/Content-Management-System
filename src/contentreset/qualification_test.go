package contentreset

import "testing"

func TestExpectedQualificationVersionIsCodeOwned(t *testing.T) {
	contract := Contract{Owner: "cms/feedstate", Effect: "publish", TargetType: "campaign", Version: "v1"}
	version, ok := ExpectedQualificationVersion(contract)
	if !ok || version != "content-reset-owner/v1" {
		t.Fatalf("unexpected expected qualification: %q %v", version, ok)
	}
	if _, ok := ExpectedQualificationVersion(Contract{Owner: "cms/unknown", Effect: "publish", TargetType: "campaign", Version: "v1"}); ok {
		t.Fatal("an unknown contract must not be qualifiable")
	}
}

func TestRecordQualificationRejectsWrongVersionBeforeDatabaseWork(t *testing.T) {
	_, err := RecordQualification(nil, QualificationInput{
		Contract:             Contract{Owner: "cms/feedstate", Effect: "publish", TargetType: "campaign", Version: "v1"},
		QualificationVersion: "content-reset-owner/v0",
		EnvironmentHash:      "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		EvidenceHash:         "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		QualifiedBy:          "reviewer",
	})
	if err == nil {
		t.Fatal("a mismatched qualification version was accepted")
	}
}

func TestRecordQualificationRejectsMissingDigestsBeforeDatabaseWork(t *testing.T) {
	_, err := RecordQualification(nil, QualificationInput{
		Contract:             Contract{Owner: "cms/feedstate", Effect: "publish", TargetType: "campaign", Version: "v1"},
		QualificationVersion: "content-reset-owner/v1",
		QualifiedBy:          "reviewer",
	})
	if err == nil {
		t.Fatal("missing evidence digests were accepted")
	}
}

func TestLoadQualificationsWithoutSchemaIsEmpty(t *testing.T) {
	qualified, err := LoadQualifications(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(qualified) != 0 {
		t.Fatalf("expected empty registry, got %d rows", len(qualified))
	}
}
