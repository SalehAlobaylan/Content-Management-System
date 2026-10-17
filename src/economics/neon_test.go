package economics

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNeonAdapterUsesExplicitReadScopeAndNeverCallsItABill(t *testing.T) {
	from := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -1)
	to := from.AddDate(0, 0, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Query().Get("project_ids") != "project-one" || r.URL.Query().Get("org_id") != "org-one" || r.Header.Get("Authorization") != "Bearer fixture-only" {
			t.Error("scope/auth contract")
		}
		fmt.Fprintf(w, `{"projects":[{"project_id":"project-one","periods":[{"period_id":"period","period_plan":"launch","consumption":[{"timeframe_start":%q,"timeframe_end":%q,"metrics":[{"metric_name":"compute_unit_seconds","value":15}]}]}]}],"pagination":{}}`, from.Format(time.RFC3339), to.Format(time.RFC3339))
	}))
	defer server.Close()
	result, e := FetchNeon(context.Background(), server.Client(), server.URL, "fixture-only", NeonConfig{OrgID: "org-one", ProjectID: "project-one"}, from, to)
	if e != nil || len(result.Projects) != 1 {
		t.Fatal(result, e)
	}
}
func TestNeonUnavailablePlanDoesNotBecomeZero(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(403) }))
	defer server.Close()
	_, e := FetchNeon(context.Background(), server.Client(), server.URL, "fixture-only", NeonConfig{OrgID: "org-one", ProjectID: "project-one"}, time.Now().Add(-time.Hour), time.Now())
	if e == nil {
		t.Fatal("unsupported provider API became zero usage")
	}
}
