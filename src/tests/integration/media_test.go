package integration

import (
	"content-management-system/src/models"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TODO: Import required packages for:
// - JSON handling
// - HTTP testing
// - Your application models
// - Testing package

/*
MEDIA INTEGRATION TESTS

These tests verify the complete flow of media operations through the API.
Each test should:
1. Start with a clean database state
2. Perform API operations
3. Verify the responses
4. Check database state if needed
*/

func TestMediaIntegration(test *testing.T) {
	clearTables()

	test.Run("Create Media", func(test *testing.T) {
		body := `{
			"url": "http://example.com/test.jpg",
			"type": "image"
		}`

		req := httptest.NewRequest("POST", "/api/v1/media", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusCreated {
			test.Fatalf("Expected status 201, got %d: %s", w.Code, w.Body.String())
		}

		var response models.Media
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			test.Fatalf("Failed to unmarshal response: %v", err)
		}

		if response.URL != "http://example.com/test.jpg" {
			test.Errorf("Expected URL 'http://example.com/test.jpg', got %s", response.URL)
		}
		if response.ID == 0 {
			test.Errorf("Expected created media to have an ID")
		}
	})

	test.Run("Get All Media", func(test *testing.T) {
		req := httptest.NewRequest("GET", "/api/v1/media", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			test.Fatalf("Expected status 200, got %d: %s", w.Code, w.Body.String())
		}

		var list []models.Media
		if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil {
			test.Fatalf("Failed to unmarshal list: %v", err)
		}
		if len(list) == 0 {
			test.Errorf("Expected non-empty media list")
		}
	})
}

/*
TESTING HINTS:
1. Request Creation:
   - Use httptest.NewRequest for creating requests
   - Remember to set Content-Type for POST requests
   - Use strings.NewReader for request bodies

2. Response Handling:
   - Use httptest.NewRecorder for capturing responses
   - Parse JSON responses carefully
   - Check both status codes and response bodies

3. Test Data:
   - Use meaningful test data
   - Clean up between tests
   - Consider edge cases

4. Error Cases:
   - Test invalid inputs
   - Test missing required fields
   - Test invalid content types
*/
