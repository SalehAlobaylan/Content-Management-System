package operator

import (
	"net/http"

	"content-management-system/src/accesssnapshot"
)

// Operator retains these aliases for its public package contract. The shared
// implementation also serves native Supply approvals and workers, preventing
// a second environment gate or divergent IAM parsing behavior.
var ErrAccessUnavailable = accesssnapshot.ErrUnavailable

type AccessSnapshot = accesssnapshot.Snapshot
type IAMAccessClient = accesssnapshot.IAMClient

func NewIAMAccessClient(baseURL, token string, client *http.Client) (*IAMAccessClient, error) {
	return accesssnapshot.NewIAMClient(baseURL, token, client)
}

func NewIAMAccessClientFromEnv() (*IAMAccessClient, error) {
	return accesssnapshot.NewIAMClientFromEnv()
}
