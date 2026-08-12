// Package accesssnapshot owns the typed, fail-closed IAM observation used by
// CMS decision and recovery paths. It deliberately uses the existing CMS
// machine identity; Supply actions do not introduce an Operator-only key.
package accesssnapshot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

var ErrUnavailable = errors.New("current IAM access is unavailable")

type Snapshot struct {
	UserID        string    `json:"user_id"`
	TenantID      string    `json:"tenant_id"`
	Active        bool      `json:"active"`
	Roles         []string  `json:"roles"`
	Permissions   []string  `json:"permissions"`
	IsAdmin       bool      `json:"is_admin"`
	AccessVersion string    `json:"access_version"`
	ObservedAt    time.Time `json:"observed_at"`
}

func (snapshot Snapshot) ValidateFor(userID, tenantID string) error {
	if snapshot.UserID != userID || snapshot.TenantID != tenantID || strings.TrimSpace(snapshot.AccessVersion) == "" {
		return fmt.Errorf("%w: IAM returned mismatched access identity", ErrUnavailable)
	}
	if !snapshot.Active {
		return fmt.Errorf("%w: account is inactive", ErrUnavailable)
	}
	return nil
}

func (snapshot Snapshot) HasPermission(permission string) bool {
	if snapshot.IsAdmin {
		return true
	}
	permission = strings.ToLower(strings.TrimSpace(permission))
	for _, item := range snapshot.Permissions {
		if strings.EqualFold(strings.TrimSpace(item), permission) || strings.EqualFold(strings.TrimSpace(item), "*:*") {
			return true
		}
		parts := strings.SplitN(permission, ":", 2)
		if len(parts) == 2 && strings.EqualFold(strings.TrimSpace(item), parts[0]+":*") {
			return true
		}
	}
	return false
}

type Provider interface {
	Snapshot(ctx context.Context, userID, tenantID string) (Snapshot, error)
}

type IAMClient struct {
	baseURL string
	token   string
	client  *http.Client
}

func NewIAMClient(baseURL, token string, client *http.Client) (*IAMClient, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, fmt.Errorf("%w: IAM base URL is invalid", ErrUnavailable)
	}
	if strings.TrimSpace(token) == "" {
		return nil, fmt.Errorf("%w: CMS machine identity is not configured", ErrUnavailable)
	}
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	return &IAMClient{baseURL: strings.TrimRight(parsed.String(), "/"), token: token, client: client}, nil
}

func NewIAMClientFromEnv() (*IAMClient, error) {
	// The legacy override stays readable for existing deployments. New Supply
	// recovery always falls back to the established CMS machine identity.
	token := strings.TrimSpace(os.Getenv("OPERATOR_IAM_ACCESS_SNAPSHOT_TOKEN"))
	if token == "" {
		token = strings.TrimSpace(os.Getenv("CMS_SERVICE_TOKEN"))
	}
	return NewIAMClient(os.Getenv("IAM_BASE_URL"), token, nil)
}

func (client *IAMClient) Snapshot(ctx context.Context, userID, tenantID string) (Snapshot, error) {
	endpoint, err := url.Parse(client.baseURL + "/internal/access/users/" + url.PathEscape(strings.TrimSpace(userID)))
	if err != nil {
		return Snapshot{}, fmt.Errorf("%w: create snapshot endpoint", ErrUnavailable)
	}
	query := endpoint.Query()
	query.Set("tenant_id", strings.TrimSpace(tenantID))
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return Snapshot{}, fmt.Errorf("%w: create snapshot request", ErrUnavailable)
	}
	request.Header.Set("Authorization", "Bearer "+client.token)
	response, err := client.client.Do(request)
	if err != nil {
		return Snapshot{}, fmt.Errorf("%w: IAM request failed", ErrUnavailable)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return Snapshot{}, fmt.Errorf("%w: IAM response status %d", ErrUnavailable, response.StatusCode)
	}
	decoder := json.NewDecoder(response.Body)
	decoder.DisallowUnknownFields()
	var snapshot Snapshot
	if err := decoder.Decode(&snapshot); err != nil {
		return Snapshot{}, fmt.Errorf("%w: decode IAM response", ErrUnavailable)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return Snapshot{}, fmt.Errorf("%w: IAM response contains trailing data", ErrUnavailable)
	}
	if snapshot.ObservedAt.IsZero() {
		return Snapshot{}, fmt.Errorf("%w: IAM observation time is missing", ErrUnavailable)
	}
	sort.Strings(snapshot.Roles)
	sort.Strings(snapshot.Permissions)
	if err := snapshot.ValidateFor(userID, tenantID); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}
