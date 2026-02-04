package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"net/http"
	"net/mail"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/lib/pq"
	"gorm.io/gorm"
)

type adminUserRequest struct {
	Email       string   `json:"email"`
	Password    string   `json:"password"`
	Role        string   `json:"role"`
	Permissions []string `json:"permissions"`
	IsActive    *bool    `json:"is_active"`
}

type adminUserResponse struct {
	ID          string   `json:"id"`
	Email       string   `json:"email"`
	Role        string   `json:"role"`
	Permissions []string `json:"permissions"`
	IsActive    bool     `json:"is_active"`
	CreatedAt   string   `json:"created_at"`
	UpdatedAt   string   `json:"updated_at"`
}

type adminUserListResponse struct {
	Data       []adminUserResponse `json:"data"`
	Total      int64               `json:"total"`
	Page       int                 `json:"page"`
	Limit      int                 `json:"limit"`
	TotalPages int                 `json:"total_pages"`
}

type adminPasswordResetRequest struct {
	Password string `json:"password"`
}

func mapAdminUserResponse(user models.AdminUser) adminUserResponse {
	permissions := []string{}
	if user.Permissions != nil {
		permissions = []string(user.Permissions)
	}

	return adminUserResponse{
		ID:          user.PublicID.String(),
		Email:       user.Email,
		Role:        user.Role,
		Permissions: permissions,
		IsActive:    user.IsActive,
		CreatedAt:   user.CreatedAt.Format(time.RFC3339),
		UpdatedAt:   user.UpdatedAt.Format(time.RFC3339),
	}
}

func requireAdminRole(c *gin.Context) (*models.AdminUser, bool) {
	value, exists := c.Get("admin_user")
	if !exists {
		c.JSON(http.StatusUnauthorized, authErrorResponse{
			Message: "Unauthorized",
			Code:    "UNAUTHORIZED",
		})
		return nil, false
	}

	user, ok := value.(models.AdminUser)
	if !ok {
		c.JSON(http.StatusUnauthorized, authErrorResponse{
			Message: "Unauthorized",
			Code:    "UNAUTHORIZED",
		})
		return nil, false
	}

	if strings.ToLower(user.Role) != "admin" {
		c.JSON(http.StatusForbidden, authErrorResponse{
			Message: "Forbidden",
			Code:    "FORBIDDEN",
		})
		return nil, false
	}

	return &user, true
}

// ListAdminUsers handles GET /admin/users
func ListAdminUsers(c *gin.Context) {
	if _, ok := requireAdminRole(c); !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)

	query := db.Model(&models.AdminUser{})

	params, err := utils.ParseQueryParams(c, adminUserQueryConfig)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: err.Error(),
			Code:    "INVALID_QUERY",
		})
		return
	}

	query = utils.ApplyQuery(query, params, adminUserQueryConfig)

	var users []models.AdminUser
	meta, err := utils.FetchWithPagination(query, params, &users)
	if err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to fetch admin users",
			Code:    "FETCH_FAILED",
		})
		return
	}

	data := make([]adminUserResponse, 0, len(users))
	for _, user := range users {
		data = append(data, mapAdminUserResponse(user))
	}

	c.JSON(http.StatusOK, adminUserListResponse{
		Data:       data,
		Total:      meta.Total,
		Page:       meta.Page,
		Limit:      meta.Limit,
		TotalPages: meta.TotalPages,
	})
}

// GetAdminUser handles GET /admin/users/:id
func GetAdminUser(c *gin.Context) {
	if _, ok := requireAdminRole(c); !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")

	var user models.AdminUser
	if err := db.Where("public_id = ?", publicID).First(&user).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{
			Message: "Admin user not found",
			Code:    "NOT_FOUND",
		})
		return
	}

	c.JSON(http.StatusOK, mapAdminUserResponse(user))
}

// CreateAdminUser handles POST /admin/users
func CreateAdminUser(c *gin.Context) {
	if _, ok := requireAdminRole(c); !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)
	var req adminUserRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid request",
			Code:    "INVALID_REQUEST",
		})
		return
	}

	email := strings.ToLower(strings.TrimSpace(req.Email))
	if email == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Email is required",
			Code:    "EMAIL_REQUIRED",
		})
		return
	}
	if _, err := mail.ParseAddress(email); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid email format",
			Code:    "INVALID_EMAIL_FORMAT",
		})
		return
	}
	if strings.TrimSpace(req.Role) == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Role is required",
			Code:    "ROLE_REQUIRED",
		})
		return
	}

	// Require password for new user
	password := strings.TrimSpace(req.Password)
	if password == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Password is required",
			Code:    "PASSWORD_REQUIRED",
		})
		return
	}

	hash, err := utils.HashPassword(password)
	if err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to hash password",
			Code:    "HASH_FAILED",
		})
		return
	}

	user := models.AdminUser{
		Email:        email,
		Role:         strings.ToLower(req.Role),
		PasswordHash: hash,
		Permissions:  pq.StringArray(req.Permissions),
		IsActive:     true,
	}

	if req.IsActive != nil {
		user.IsActive = *req.IsActive
	}

	if err := db.Create(&user).Error; err != nil {
		message := "Failed to create admin user"
		if strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "unique") {
			message = "Email already exists"
		}
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: message,
			Code:    "CREATE_FAILED",
		})
		return
	}

	c.JSON(http.StatusCreated, mapAdminUserResponse(user))
}

// UpdateAdminUser handles PUT /admin/users/:id
func UpdateAdminUser(c *gin.Context) {
	if _, ok := requireAdminRole(c); !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	var req adminUserRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid request",
			Code:    "INVALID_REQUEST",
		})
		return
	}

	var user models.AdminUser
	if err := db.Where("public_id = ?", publicID).First(&user).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{
			Message: "Admin user not found",
			Code:    "NOT_FOUND",
		})
		return
	}

	if strings.TrimSpace(req.Email) != "" {
		email := strings.ToLower(strings.TrimSpace(req.Email))
		if _, err := mail.ParseAddress(email); err != nil {
			c.JSON(http.StatusBadRequest, authErrorResponse{
				Message: "Invalid email format",
				Code:    "INVALID_EMAIL_FORMAT",
			})
			return
		}
		user.Email = email
	}
	if strings.TrimSpace(req.Role) != "" {
		user.Role = strings.ToLower(strings.TrimSpace(req.Role))
	}
	if req.Permissions != nil {
		user.Permissions = pq.StringArray(req.Permissions)
	}
	if req.IsActive != nil {
		user.IsActive = *req.IsActive
	}

	if err := db.Save(&user).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to update admin user",
			Code:    "UPDATE_FAILED",
		})
		return
	}

	c.JSON(http.StatusOK, mapAdminUserResponse(user))
}

// DeleteAdminUser handles DELETE /admin/users/:id
func DeleteAdminUser(c *gin.Context) {
	if _, ok := requireAdminRole(c); !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")

	var user models.AdminUser
	if err := db.Where("public_id = ?", publicID).First(&user).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{
			Message: "Admin user not found",
			Code:    "NOT_FOUND",
		})
		return
	}

	if err := db.Delete(&user).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to delete admin user",
			Code:    "DELETE_FAILED",
		})
		return
	}

	c.Status(http.StatusNoContent)
}

// ResetAdminUserPassword handles POST /admin/users/:id/password
func ResetAdminUserPassword(c *gin.Context) {
	if _, ok := requireAdminRole(c); !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	var req adminPasswordResetRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid request",
			Code:    "INVALID_REQUEST",
		})
		return
	}
	if strings.TrimSpace(req.Password) == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Password is required",
			Code:    "PASSWORD_REQUIRED",
		})
		return
	}

	var user models.AdminUser
	if err := db.Where("public_id = ?", publicID).First(&user).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{
			Message: "Admin user not found",
			Code:    "NOT_FOUND",
		})
		return
	}

	hash, err := utils.HashPassword(req.Password)
	if err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to hash password",
			Code:    "HASH_FAILED",
		})
		return
	}

	user.PasswordHash = hash

	if err := db.Save(&user).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to reset password",
			Code:    "RESET_FAILED",
		})
		return
	}

	c.Status(http.StatusNoContent)
}

var adminUserQueryConfig = utils.QueryConfig{
	DefaultLimit: 20,
	MaxLimit:     100,
	DefaultSort: []utils.SortParam{{
		Field:     "created_at",
		Direction: "desc",
	}},
	SortableFields: map[string]string{
		"created_at": "admin_users.created_at",
		"updated_at": "admin_users.updated_at",
		"email":      "admin_users.email",
		"role":       "admin_users.role",
	},
	FilterableFields: map[string]string{
		"role":      "admin_users.role",
		"is_active": "admin_users.is_active",
		"email":     "admin_users.email",
	},
	SearchableFields: map[string]string{
		"email": "admin_users.email",
	},
	DefaultSearchFields: []string{"email"},
	FieldDefaultOperators: map[string]string{
		"email": "contains",
	},
}
