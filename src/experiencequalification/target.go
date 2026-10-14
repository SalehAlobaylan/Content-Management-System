// Package experiencequalification is used only by explicit qualification tools.
// It has no service startup or public HTTP integration.
package experiencequalification

import (
	"bufio"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

func LiteralProfile(root, id string) (map[string]string, error) {
	if !regexp.MustCompile(`^[a-z][a-z0-9-]{0,47}$`).MatchString(id) {
		return nil, errors.New("invalid environment ID")
	}
	path := filepath.Join(root, "environments", ".private", id+".env")
	for _, part := range []string{filepath.Join(root, "environments"), filepath.Join(root, "environments", ".private"), path} {
		info, err := os.Lstat(part)
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("symlinked or missing environment path")
		}
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("private regular environment file with mode0600 required")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("environment file unavailable")
	}
	defer f.Close()
	values := map[string]string{}
	scan := bufio.NewScanner(f)
	for scan.Scan() {
		line := strings.TrimSpace(scan.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return nil, errors.New("invalid literal environment entry")
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if len(value) >= 2 && (value[0] == '\'' && value[len(value)-1] == '\'' || value[0] == '"' && value[len(value)-1] == '"') {
			value = value[1 : len(value)-1]
		}
		if !regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`).MatchString(key) {
			return nil, errors.New("invalid literal environment key")
		}
		if _, exists := values[key]; exists {
			return nil, errors.New("duplicate literal environment key")
		}
		values[key] = value
	}
	if scan.Err() != nil {
		return nil, errors.New("environment read failed")
	}
	return values, nil
}
func CMSURL(values map[string]string) (string, error) {
	if common := values["DATABASE_URL"]; common != "" {
		for _, key := range []string{"CMS_DATABASE_URL", "IAM_DATABASE_URL"} {
			if value := values[key]; value != "" && value != common {
				return "", errors.New("conflicting common and owner database settings")
			}
		}
	}
	dsn := values["CMS_DATABASE_URL"]
	if dsn == "" {
		dsn = values["DATABASE_URL"]
	}
	u, e := url.Parse(dsn)
	if e != nil || u.Scheme != "postgresql" && u.Scheme != "postgres" || !strings.HasSuffix(u.Hostname(), ".neon.tech") || u.User == nil || u.Path == "" {
		return "", errors.New("explicit named Neon CMS target required")
	}
	return dsn, nil
}
func Disposable(root, id string, values map[string]string) error {
	if id == "stable" || id == "dev" || id == "production" {
		return errors.New("retained environments cannot run fixture writes")
	}
	registryBytes, err := os.ReadFile(filepath.Join(root, "environments", "registry.json"))
	if err != nil {
		return errors.New("disposable preservation proof unavailable")
	}
	var registry struct {
		Environments []struct {
			ID           string `json:"id"`
			Preservation string `json:"preservation"`
		} `json:"environments"`
	}
	if json.Unmarshal(registryBytes, &registry) != nil {
		return errors.New("invalid preservation proof")
	}
	proven := false
	for _, entry := range registry.Environments {
		if entry.ID == id && entry.Preservation == "disposable" {
			proven = true
		}
	}
	if !proven {
		return errors.New("named environment is not proven disposable")
	}
	if values["STORAGE_BUCKET"] == "" || values["STORAGE_ENDPOINT"] == "" {
		return errors.New("separated storage target required")
	}
	dsn, err := CMSURL(values)
	if err != nil {
		return err
	}
	target, _ := url.Parse(dsn)
	if !regexp.MustCompile(`^wahb_(cms_)?(test|disposable)_[a-z0-9_]+$`).MatchString(strings.TrimPrefix(target.Path, "/")) {
		return errors.New("database name lacks disposable qualification proof")
	}
	entries, err := os.ReadDir(filepath.Join(root, "environments", ".private"))
	if err != nil {
		return errors.New("separation proof unavailable")
	}
	for _, entry := range entries {
		other := strings.TrimSuffix(entry.Name(), ".env")
		if other == id || !strings.HasSuffix(entry.Name(), ".env") {
			continue
		}
		v, e := LiteralProfile(root, other)
		if e != nil {
			return errors.New("other environment identity unreadable")
		}
		for _, key := range []string{"CMS_DATABASE_URL", "IAM_DATABASE_URL", "DATABASE_URL"} {
			otherDSN := v[key]
			if otherDSN == "" {
				continue
			}
			u, e := url.Parse(otherDSN)
			if e != nil {
				return errors.New("other owner database identity unreadable")
			}
			if normalizedNeonHost(u.Hostname()) == normalizedNeonHost(target.Hostname()) && u.Path == target.Path {
				return errors.New("database target overlaps another CMS/IAM environment")
			}
		}
		if values["STORAGE_BUCKET"] != "" && values["STORAGE_BUCKET"] == v["STORAGE_BUCKET"] && values["STORAGE_ENDPOINT"] == v["STORAGE_ENDPOINT"] {
			return errors.New("storage target overlaps another environment")
		}
	}
	return nil
}

func normalizedNeonHost(host string) string {
	first, rest, _ := strings.Cut(strings.ToLower(host), ".")
	return strings.TrimSuffix(first, "-pooler") + "." + rest
}
