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

func TestPostIntegration(t *testing.T) {
	fmt.Println("📝 Starting Post Integration Tests")
	clearTables()

	t.Run("Create Post", func(t *testing.T) {
		fmt.Println("  🔨 Testing post creation...")
		postBody := fmt.Sprintf(`{"title":"Post A","content":"Body","author":"Alice"}`)
		req := httptest.NewRequest("POST", "/api/v1/posts", strings.NewReader(postBody))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		fmt.Printf("  📊 Create post response: %d\n", w.Code)
		if w.Code != http.StatusCreated {
			t.Fatalf("Expected 201, got %d: %s", w.Code, w.Body.String())
		}

		var wrapper utils.ResponseMessage
		if err := json.Unmarshal(w.Body.Bytes(), &wrapper); err != nil {
			t.Fatalf("unmarshal wrapper: %v", err)
		}
		var created models.Post
		b, _ := json.Marshal(wrapper.Data)
		_ = json.Unmarshal(b, &created)
		fmt.Printf("  ✅ Created post: %s by %s\n", created.Title, created.Author)
		fmt.Printf("  📊 Post PublicID: %s\n", created.PublicID.String())
		if created.PublicID == uuid.Nil {
			t.Fatalf("expected non-empty post PublicID")
		}
		if created.Author != "Alice" || created.Title != "Post A" {
			t.Fatalf("unexpected post data: %+v", created)
		}
		if len(created.Media) != 0 {
			t.Fatalf("expected no media on created post, got %+v", created.Media)
		}
	})

	t.Run("Get Posts with Filter", func(t *testing.T) {
		fmt.Println("  🔍 Testing post filtering...")
		req := httptest.NewRequest("GET", "/api/v1/posts?author=Alice&title=Post", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		fmt.Printf("  📊 Filter posts response: %d\n", w.Code)
		if w.Code != http.StatusOK {
			t.Fatalf("Expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var wrapper utils.ResponseMessage
		if err := json.Unmarshal(w.Body.Bytes(), &wrapper); err != nil {
			t.Fatalf("unmarshal list wrapper: %v", err)
		}
		var list []models.Post
		b, _ := json.Marshal(wrapper.Data)
		_ = json.Unmarshal(b, &list)
		fmt.Printf("  📊 Found %d posts matching filter\n", len(list))
		if len(list) == 0 {
			t.Fatalf("expected at least one post")
		}
		if list[0].Author != "Alice" {
			t.Fatalf("expected author Alice, got %s", list[0].Author)
		}
		fmt.Printf("  ✅ Filtered post: %s by %s\n", list[0].Title, list[0].Author)
	})
}
