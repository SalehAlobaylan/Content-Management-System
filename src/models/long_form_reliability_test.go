package models

import (
	"sync"
	"testing"

	"gorm.io/gorm/schema"
)

func TestMediaArtifactManifestETagUsesCanonicalColumn(t *testing.T) {
	parsed, err := schema.Parse(&MediaArtifactManifest{}, &sync.Map{}, schema.NamingStrategy{})
	if err != nil {
		t.Fatal(err)
	}
	field := parsed.LookUpField("ETag")
	if field == nil {
		t.Fatal("ETag field is missing from GORM schema")
	}
	if field.DBName != "etag" {
		t.Fatalf("ETag DB column = %q, want etag", field.DBName)
	}
}
