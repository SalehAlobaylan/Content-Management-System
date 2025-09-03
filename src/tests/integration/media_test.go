package integration

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestMediaIntegration(t *testing.T) {
	fmt.Println("📸 Starting Media Integration Tests")
	clearTables()

	t.Run("Create Media", func(t *testing.T) {
		fmt.Println("  🔨 Testing media creation...")
		body := `{"url":"http://example.com/test.jpg","type":"image"}`
		req := httptest.NewRequest("POST", "/api/v1/media", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		fmt.Printf("  📊 Create media response: %d\n", w.Code)
		if w.Code != http.StatusCreated {
			t.Fatalf("Expected status 201, got %d: %s", w.Code, w.Body.String())
		}

		var wrapper utils.ResponseMessage
		if err := json.Unmarshal(w.Body.Bytes(), &wrapper); err != nil {
			t.Fatalf("Failed to unmarshal response wrapper: %v", err)
		}
		var response models.Media
		b, _ := json.Marshal(wrapper.Data)
		_ = json.Unmarshal(b, &response)
		fmt.Printf("  ✅ Created media with URL: %s\n", response.URL)
		if response.URL != "http://example.com/test.jpg" {
			t.Errorf("Expected URL 'http://example.com/test.jpg', got %s", response.URL)
		}
		// PublicID may not be populated in create response because DB default is set server-side.
		// We'll verify PublicID presence in the list endpoint below instead.
	})

	t.Run("Get All Media", func(t *testing.T) {
		fmt.Println("  📋 Testing media list retrieval...")
		req := httptest.NewRequest("GET", "/api/v1/media", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		fmt.Printf("  📊 List media response: %d\n", w.Code)
		if w.Code != http.StatusOK {
			t.Fatalf("Expected status 200, got %d: %s", w.Code, w.Body.String())
		}

		var wrapper utils.ResponseMessage
		if err := json.Unmarshal(w.Body.Bytes(), &wrapper); err != nil {
			t.Fatalf("Failed to unmarshal list wrapper: %v", err)
		}
		var list []models.Media
		b, _ := json.Marshal(wrapper.Data)
		_ = json.Unmarshal(b, &list)
		fmt.Printf("  📊 Found %d media items\n", len(list))
		if len(list) == 0 {
			t.Errorf("Expected non-empty media list")
		}
		if len(list) > 0 && list[0].PublicID == uuid.Nil {
			t.Errorf("Expected listed media to have PublicID")
		}
		if len(list) > 0 {
			fmt.Printf("  ✅ First media PublicID: %s\n", list[0].PublicID.String())
		}
	})
}

// Helper function to create test media
// createTestMedia is implemented in post_test.go for package integration

/*
TESTING HINTS:
1. Request Creation:
   - Use proper JSON formatting for relationships
   - Handle URL encoding for query parameters
   - Set appropriate headers

2. Response Validation:
   - Check both status codes and response content
   - Verify relationship data is correct
   - Validate filtered results carefully

3. Test Data:
   - Create meaningful test data
   - Handle relationships properly
   - Clean up between tests

4. Error Cases to Consider:
   - Invalid media IDs
   - Missing required fields
   - Invalid filter parameters
   - Non-existent relationships
*/
