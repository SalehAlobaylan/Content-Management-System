package utils

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

// JWTClaims defines the token payload
type JWTClaims struct {
	Email       string   `json:"email"`
	Role        string   `json:"role"`
	Permissions []string `json:"permissions,omitempty"`
	jwt.RegisteredClaims
}

var (
	ErrTokenExpired          = errors.New("token expired")
	ErrTokenInvalid          = errors.New("token invalid")
	ErrTokenSignatureInvalid = errors.New("token signature invalid")
)

// HashPassword hashes a plaintext password using bcrypt
func HashPassword(password string) (string, error) {
	hashed, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hashed), nil
}

// CheckPassword compares a bcrypt hash with a plaintext password
func CheckPassword(hash string, password string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
}

// GetJWTSecret returns the JWT secret from environment variables
func GetJWTSecret() ([]byte, error) {
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		return nil, fmt.Errorf("JWT_SECRET is not set")
	}
	return []byte(secret), nil
}

// GetJWTExpiration returns the JWT expiration duration
func GetJWTExpiration() time.Duration {
	hoursValue := os.Getenv("JWT_EXPIRATION_HOURS")
	if hoursValue == "" {
		return 24 * time.Hour
	}
	hours, err := strconv.Atoi(hoursValue)
	if err != nil || hours <= 0 {
		return 24 * time.Hour
	}
	return time.Duration(hours) * time.Hour
}

// GenerateJWT generates a signed JWT for the given user data
func GenerateJWT(userID string, email string, role string, permissions []string) (string, error) {
	secret, err := GetJWTSecret()
	if err != nil {
		return "", err
	}

	now := time.Now()
	claims := JWTClaims{
		Email:       email,
		Role:        role,
		Permissions: permissions,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID,
			Issuer:    "cms-service",
			Audience:  []string{"platform-console"},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(GetJWTExpiration())),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(secret)
}

// ParseJWT validates and parses a JWT token string
func ParseJWT(tokenString string, secret []byte) (*JWTClaims, error) {
	claims := &JWTClaims{}
	token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return secret, nil
	})

	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, ErrTokenExpired
		}
		if errors.Is(err, jwt.ErrTokenSignatureInvalid) {
			return nil, ErrTokenSignatureInvalid
		}
		return nil, ErrTokenInvalid
	}
	if !token.Valid {
		return nil, ErrTokenInvalid
	}
	if claims.RegisteredClaims.Subject == "" || claims.Email == "" || claims.Role == "" {
		return nil, ErrTokenInvalid
	}
	return claims, nil
}
