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

// TODO: Import required packages for:
// - JSON handling
// - HTTP testing
// - URL manipulation
// - String formatting
// - Your application models
// - Testing package

/*
POST INTEGRATION TESTS

These tests verify the complete flow of post operations through the API,
including relationships with media items.
Each test should:
1. Start with a clean database
2. Set up required relationships (media)
3. Perform post operations
4. Verify responses and relationships
*/

func TestPostIntegration(test *testing.T) {
	clearTables()

	mediaID := createTestMedia(test)

	test.Run("Create Post with Media", func(test *testing.T) {
		postBody := `{
			"title": "Post A",
			"content": "Body",
			"author": "Alice",
			"media": [{"id": ` + fmt.Sprintf("%d", mediaID) + `}]
		}`

		req := httptest.NewRequest("POST", "/api/v1/posts", strings.NewReader(postBody))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusCreated {
			test.Fatalf("Expected 201, got %d: %s", w.Code, w.Body.String())
		}

		var created models.Post
		if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
			test.Fatalf("unmarshal created post: %v", err)
		}
		if created.ID == 0 {
			test.Fatalf("expected post id")
		}
		if len(created.Media) != 1 || created.Media[0].ID != mediaID {
			test.Fatalf("expected post to have media id %d, got %+v", mediaID, created.Media)
		}
	})

	test.Run("Get Posts with Filter", func(test *testing.T) {
		req := httptest.NewRequest("GET", "/api/v1/posts?author=Alice&title=Post", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			test.Fatalf("Expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var list []models.Post
		if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil {
			test.Fatalf("unmarshal list: %v", err)
		}
		if len(list) == 0 {
			test.Fatalf("expected at least one post")
		}
		if list[0].Author != "Alice" {
			test.Fatalf("expected author Alice, got %s", list[0].Author)
		}
	})
}

// Helper function to create test media
func createTestMedia(t *testing.T) uint {
	body := `{
		"url": "http://example.com/test.jpg",
		"type": "image"
	}`

	req := httptest.NewRequest("POST", "/api/v1/media", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("Failed to create test media, status: %d, body: %s", w.Code, w.Body.String())
	}

	var response models.Media
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to create test media: %v", err)
	}

	return response.ID
}

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
