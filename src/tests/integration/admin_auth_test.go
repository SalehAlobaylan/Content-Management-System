package integration

import (
	"bytes"
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
)

func seedAdminUser(t *testing.T, email string, password string) models.AdminUser {
	t.Helper()
	hash, err := utils.HashPassword(password)
	if err != nil {
		t.Fatalf("failed to hash password: %v", err)
	}

	user := models.AdminUser{
		PublicID:     uuid.New(),
		Email:        email,
		Role:         "admin",
		PasswordHash: hash,
		IsActive:     true,
	}

	if err := testDB.Create(&user).Error; err != nil {
		t.Fatalf("failed to create admin user: %v", err)
	}

	return user
}

func TestAdminLoginSuccess(t *testing.T) {
	clearTables()
	email := "admin@login.test"
	password := "ChangeMe123!"
	seedAdminUser(t, email, password)

	payload := map[string]string{
		"email":    email,
		"password": password,
	}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/admin/login", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.Code)
	}

	var result map[string]any
	if err := json.Unmarshal(resp.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if result["token"] == nil {
		t.Fatalf("expected token in response")
	}
}

func TestAdminLoginInvalidPassword(t *testing.T) {
	clearTables()
	email := "admin@invalid.test"
	password := "ChangeMe123!"
	seedAdminUser(t, email, password)

	payload := map[string]string{
		"email":    email,
		"password": "wrong",
	}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/admin/login", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.Code)
	}
}

func TestAdminMeUnauthorized(t *testing.T) {
	clearTables()
	req := httptest.NewRequest(http.MethodGet, "/admin/me", nil)
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.Code)
	}
}

func TestAdminMeSuccess(t *testing.T) {
	clearTables()
	email := "admin@me.test"
	password := "ChangeMe123!"
	user := seedAdminUser(t, email, password)

	token, err := utils.GenerateJWT(user.PublicID.String(), user.Email, user.TenantID, user.Role, []string(user.Permissions))
	if err != nil {
		t.Fatalf("failed to generate token: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/admin/me", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.Code)
	}
}
