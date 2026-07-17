// Package testdb guards destructive CMS database tests before a connection is opened.
package testdb

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

var disposableName = regexp.MustCompile(`^wahb_cms_test_[a-z0-9]{8,}$`)

// Target is deliberately safe to print in test diagnostics: it excludes
// credentials, query parameters, and the original connection string.
type Target struct {
	Host     string
	Database string
}

// ValidateDisposableDSN rejects dangerous targets without opening a connection.
// Callers must supply an explicit opt-in marker and the runtime DSN so a copied
// production/development URL cannot be used as a destructive test target.
func ValidateDisposableDSN(raw, runtimeDSN, marker string) (Target, error) {
	if marker != "I_UNDERSTAND_THIS_DATABASE_IS_DISPOSABLE" {
		return Target{}, fmt.Errorf("CMS_TEST_DISPOSABLE acknowledgement is required")
	}
	if strings.TrimSpace(raw) == "" {
		return Target{}, fmt.Errorf("CMS_TEST_DATABASE_URL is required")
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Hostname() == "" {
		return Target{}, fmt.Errorf("CMS_TEST_DATABASE_URL must be a PostgreSQL URL")
	}
	host := strings.ToLower(u.Hostname())
	if strings.Contains(host, "supabase") || strings.Contains(host, "neon") {
		return Target{}, fmt.Errorf("managed production database hosts are forbidden for tests")
	}
	database := strings.TrimPrefix(u.Path, "/")
	if !disposableName.MatchString(database) {
		return Target{}, fmt.Errorf("test database name must match wahb_cms_test_<random>")
	}
	if strings.TrimSpace(runtimeDSN) != "" {
		runtimeURL, runtimeErr := url.Parse(runtimeDSN)
		if runtimeErr == nil && runtimeURL.Hostname() != "" {
			runtimeHost := strings.ToLower(runtimeURL.Hostname())
			runtimeDatabase := strings.TrimPrefix(runtimeURL.Path, "/")
			if host == runtimeHost && database == runtimeDatabase {
				return Target{}, fmt.Errorf("test database target must not match DATABASE_URL")
			}
		}
	}
	return Target{Host: host, Database: database}, nil
}
