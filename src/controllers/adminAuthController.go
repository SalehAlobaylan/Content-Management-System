package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"net/http"
	"net/mail"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type adminLoginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type loginResponse struct {
	Token string       `json:"token"`
	User  loginUserDTO `json:"user"`
}

type loginUserDTO struct {
	ID    string `json:"id"`
	Email string `json:"email"`
	Role  string `json:"role"`
}

type meResponse struct {
	ID          string   `json:"id"`
	Email       string   `json:"email"`
	Role        string   `json:"role"`
	Permissions []string `json:"permissions"`
}

type authErrorResponse struct {
	Message string `json:"message"`
	Code    string `json:"code,omitempty"`
	Details any    `json:"details,omitempty"`
}

var adminLoginLimiter = utils.NewLoginRateLimiter(5, 15*time.Minute)

func respondAuthError(c *gin.Context, status int, message string, code string) {
	c.JSON(status, authErrorResponse{
		Message: message,
		Code:    code,
	})
}

// AdminLogin handles POST /admin/login
func AdminLogin(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var req adminLoginRequest

	ip := c.ClientIP()
	if ok, retryAfter := adminLoginLimiter.Allow(ip); !ok {
		c.Header("Retry-After", strconv.Itoa(retryAfter))
		respondAuthError(c, http.StatusTooManyRequests, "Too many requests", "RATE_LIMITED")
		return
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		respondAuthError(c, http.StatusBadRequest, "Invalid request", "INVALID_REQUEST")
		return
	}

	if strings.TrimSpace(req.Email) == "" && strings.TrimSpace(req.Password) == "" {
		respondAuthError(c, http.StatusBadRequest, "Email and password are required", "MISSING_CREDENTIALS")
		return
	}
	if strings.TrimSpace(req.Email) == "" {
		respondAuthError(c, http.StatusBadRequest, "Email is required", "EMAIL_REQUIRED")
		return
	}
	if strings.TrimSpace(req.Password) == "" {
		respondAuthError(c, http.StatusBadRequest, "Password is required", "PASSWORD_REQUIRED")
		return
	}

	req.Email = strings.TrimSpace(req.Email)
	req.Password = strings.TrimSpace(req.Password)

	if _, err := mail.ParseAddress(req.Email); err != nil {
		respondAuthError(c, http.StatusBadRequest, "Invalid email format", "INVALID_EMAIL_FORMAT")
		return
	}

	email := strings.ToLower(req.Email)
	var user models.AdminUser
	if err := db.Where("email = ?", email).First(&user).Error; err != nil {
		respondAuthError(c, http.StatusUnauthorized, "Invalid email or password", "INVALID_CREDENTIALS")
		return
	}
	if !user.IsActive {
		respondAuthError(c, http.StatusUnauthorized, "Account is disabled", "ACCOUNT_DISABLED")
		return
	}
	if !utils.CheckPassword(user.PasswordHash, req.Password) {
		respondAuthError(c, http.StatusUnauthorized, "Invalid email or password", "INVALID_CREDENTIALS")
		return
	}

	token, err := utils.GenerateJWT(user.PublicID.String(), user.Email, user.Role, []string(user.Permissions))
	if err != nil {
		respondAuthError(c, http.StatusInternalServerError, "Internal server error", "INTERNAL_SERVER_ERROR")
		return
	}

	c.JSON(http.StatusOK, loginResponse{
		Token: token,
		User: loginUserDTO{
			ID:    user.PublicID.String(),
			Email: user.Email,
			Role:  user.Role,
		},
	})
}

// AdminMe handles GET /admin/me
func AdminMe(c *gin.Context) {
	value, exists := c.Get("admin_user")
	if !exists {
		respondAuthError(c, http.StatusUnauthorized, "Unauthorized", "UNAUTHORIZED")
		return
	}

	user, ok := value.(models.AdminUser)
	if !ok {
		respondAuthError(c, http.StatusUnauthorized, "Unauthorized", "UNAUTHORIZED")
		return
	}

	permissions := []string{}
	if user.Permissions != nil {
		permissions = []string(user.Permissions)
	}

	c.JSON(http.StatusOK, meResponse{
		ID:          user.PublicID.String(),
		Email:       user.Email,
		Role:        user.Role,
		Permissions: permissions,
	})
}
