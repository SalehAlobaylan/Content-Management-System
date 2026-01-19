package integration

import (
	"content-management-system/src/models"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestInteractionIntegration(t *testing.T) {
	fmt.Println("💬 Starting Interaction Integration Tests")
	clearLumenTables()
	seedTestContent()

	var testContentID string
	var testInteractionID string
	testSessionID := "test-session-123"

	// Get a test content item
	var item models.ContentItem
	if err := testDB.Where("type = ?", models.ContentTypeVideo).First(&item).Error; err != nil {
		t.Fatalf("failed to get test content: %v", err)
	}
	testContentID = item.PublicID.String()

	t.Run("Create Like Interaction", func(t *testing.T) {
		fmt.Println("  ❤️  Testing create like...")
		body := fmt.Sprintf(`{"content_item_id":"%s","interaction_type":"like","session_id":"%s"}`, testContentID, testSessionID)
		req := httptest.NewRequest("POST", "/api/v1/interactions", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		fmt.Printf("  📊 Create like response: %d\n", w.Code)
		if w.Code != http.StatusCreated {
			t.Fatalf("Expected 201, got %d: %s", w.Code, w.Body.String())
		}

		var response struct {
			Data struct {
				ID string `json:"id"`
			} `json:"data"`
		}
		json.Unmarshal(w.Body.Bytes(), &response)
		testInteractionID = response.Data.ID
		fmt.Printf("  📊 Created interaction ID: %s\n", testInteractionID)
		fmt.Println("  ✅ Create like test passed")
	})

	t.Run("Create Duplicate Like (Idempotent)", func(t *testing.T) {
		fmt.Println("  🔄 Testing duplicate like (should be idempotent)...")
		body := fmt.Sprintf(`{"content_item_id":"%s","interaction_type":"like","session_id":"%s"}`, testContentID, testSessionID)
		req := httptest.NewRequest("POST", "/api/v1/interactions", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		// Should return 200 (already exists) not error
		if w.Code != http.StatusOK && w.Code != http.StatusCreated {
			t.Fatalf("Expected 200 or 201, got %d: %s", w.Code, w.Body.String())
		}
		fmt.Println("  ✅ Idempotent like test passed")
	})

	t.Run("Create Bookmark Interaction", func(t *testing.T) {
		fmt.Println("  🔖 Testing create bookmark...")
		body := fmt.Sprintf(`{"content_item_id":"%s","interaction_type":"bookmark","session_id":"%s"}`, testContentID, testSessionID)
		req := httptest.NewRequest("POST", "/api/v1/interactions", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		fmt.Printf("  📊 Create bookmark response: %d\n", w.Code)
		if w.Code != http.StatusCreated {
			t.Fatalf("Expected 201, got %d: %s", w.Code, w.Body.String())
		}
		fmt.Println("  ✅ Create bookmark test passed")
	})

	t.Run("Get Bookmarks", func(t *testing.T) {
		fmt.Println("  📚 Testing get bookmarks...")
		req := httptest.NewRequest("GET", "/api/v1/interactions/bookmarks?session_id="+testSessionID, nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		fmt.Printf("  📊 Get bookmarks response: %d\n", w.Code)
		if w.Code != http.StatusOK {
			t.Fatalf("Expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var response struct {
			Items []struct {
				ID string `json:"id"`
			} `json:"items"`
		}
		json.Unmarshal(w.Body.Bytes(), &response)

		fmt.Printf("  📊 Found %d bookmarked items\n", len(response.Items))
		if len(response.Items) == 0 {
			t.Fatalf("expected at least one bookmarked item")
		}
		fmt.Println("  ✅ Get bookmarks test passed")
	})

	t.Run("Create View Interaction", func(t *testing.T) {
		fmt.Println("  👁️  Testing create view...")
		body := fmt.Sprintf(`{"content_item_id":"%s","interaction_type":"view","session_id":"%s"}`, testContentID, testSessionID)
		req := httptest.NewRequest("POST", "/api/v1/interactions", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		fmt.Printf("  📊 Create view response: %d\n", w.Code)
		if w.Code != http.StatusCreated {
			t.Fatalf("Expected 201, got %d: %s", w.Code, w.Body.String())
		}
		fmt.Println("  ✅ Create view test passed")
	})

	t.Run("Delete Interaction", func(t *testing.T) {
		if testInteractionID == "" {
			t.Skip("No interaction ID to delete")
		}
		fmt.Println("  🗑️  Testing delete interaction...")
		req := httptest.NewRequest("DELETE", "/api/v1/interactions/"+testInteractionID, nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		fmt.Printf("  📊 Delete interaction response: %d\n", w.Code)
		if w.Code != http.StatusOK {
			t.Fatalf("Expected 200, got %d: %s", w.Code, w.Body.String())
		}
		fmt.Println("  ✅ Delete interaction test passed")
	})

	t.Run("Invalid Content ID", func(t *testing.T) {
		fmt.Println("  ❌ Testing invalid content ID...")
		body := `{"content_item_id":"not-a-uuid","interaction_type":"like","session_id":"test"}`
		req := httptest.NewRequest("POST", "/api/v1/interactions", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("Expected 400, got %d", w.Code)
		}
		fmt.Println("  ✅ Invalid content ID test passed")
	})

	t.Run("Missing Session/User ID for Bookmarks", func(t *testing.T) {
		fmt.Println("  ❌ Testing missing session ID...")
		req := httptest.NewRequest("GET", "/api/v1/interactions/bookmarks", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("Expected 400, got %d", w.Code)
		}
		fmt.Println("  ✅ Missing session ID test passed")
	})
}
