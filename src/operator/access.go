package operator

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

var ErrAccessUnavailable = errors.New("current IAM access is unavailable")

type AccessSnapshot struct {
	UserID        string    `json:"user_id"`
	TenantID      string    `json:"tenant_id"`
	Active        bool      `json:"active"`
	Roles         []string  `json:"roles"`
	Permissions   []string  `json:"permissions"`
	IsAdmin       bool      `json:"is_admin"`
	AccessVersion string    `json:"access_version"`
	ObservedAt    time.Time `json:"observed_at"`
}

func (snapshot AccessSnapshot) ValidateFor(userID, tenantID string) error {
	if snapshot.UserID != userID || snapshot.TenantID != tenantID || strings.TrimSpace(snapshot.AccessVersion) == "" {
		return fmt.Errorf("%w: IAM returned mismatched access identity", ErrAccessUnavailable)
	}
	if !snapshot.Active {
		return fmt.Errorf("%w: account is inactive", ErrAccessUnavailable)
	}
	return nil
}

func (snapshot AccessSnapshot) HasPermission(permission string) bool {
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

type IAMAccessClient struct {
	baseURL string
	token   string
	client  *http.Client
}

func NewIAMAccessClient(baseURL, token string, client *http.Client) (*IAMAccessClient, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, fmt.Errorf("%w: IAM base URL is invalid", ErrAccessUnavailable)
	}
	if strings.TrimSpace(token) == "" {
		return nil, fmt.Errorf("%w: IAM access capability is not configured", ErrAccessUnavailable)
	}
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	return &IAMAccessClient{baseURL: strings.TrimRight(parsed.String(), "/"), token: token, client: client}, nil
}

func NewIAMAccessClientFromEnv() (*IAMAccessClient, error) {
	// Operator uses the established CMS machine identity. The old dedicated
	// override remains readable for deployed installations, but absence of that
	// optional setting must never create a second Operator-specific boot gate.
	token := strings.TrimSpace(os.Getenv("OPERATOR_IAM_ACCESS_SNAPSHOT_TOKEN"))
	if token == "" {
		token = strings.TrimSpace(os.Getenv("CMS_SERVICE_TOKEN"))
	}
	return NewIAMAccessClient(os.Getenv("IAM_BASE_URL"), token, nil)
}

func (client *IAMAccessClient) Snapshot(ctx context.Context, userID, tenantID string) (AccessSnapshot, error) {
	endpoint, err := url.Parse(client.baseURL + "/internal/access/users/" + url.PathEscape(strings.TrimSpace(userID)))
	if err != nil {
		return AccessSnapshot{}, fmt.Errorf("%w: create snapshot endpoint", ErrAccessUnavailable)
	}
	query := endpoint.Query()
	query.Set("tenant_id", strings.TrimSpace(tenantID))
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return AccessSnapshot{}, fmt.Errorf("%w: create snapshot request", ErrAccessUnavailable)
	}
	request.Header.Set("Authorization", "Bearer "+client.token)
	response, err := client.client.Do(request)
	if err != nil {
		return AccessSnapshot{}, fmt.Errorf("%w: IAM request failed", ErrAccessUnavailable)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return AccessSnapshot{}, fmt.Errorf("%w: IAM response status %d", ErrAccessUnavailable, response.StatusCode)
	}
	decoder := json.NewDecoder(response.Body)
	decoder.DisallowUnknownFields()
	var snapshot AccessSnapshot
	if err := decoder.Decode(&snapshot); err != nil {
		return AccessSnapshot{}, fmt.Errorf("%w: decode IAM response", ErrAccessUnavailable)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return AccessSnapshot{}, fmt.Errorf("%w: IAM response contains trailing data", ErrAccessUnavailable)
	}
	if snapshot.ObservedAt.IsZero() {
		return AccessSnapshot{}, fmt.Errorf("%w: IAM observation time is missing", ErrAccessUnavailable)
	}
	sort.Strings(snapshot.Roles)
	sort.Strings(snapshot.Permissions)
	if err := snapshot.ValidateFor(userID, tenantID); err != nil {
		return AccessSnapshot{}, err
	}
	return snapshot, nil
}
