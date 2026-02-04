# CMS Auth API Contract for Platform Console

**Document Version:** 1.0  
**Date:** 2026-02-02  
**Purpose:** Define authentication API contract between CMS Service and Platform Console  
**Consumer:** Platform Console (Next.js Frontend)  
**Provider:** CMS Service (Go/Gin Backend)

---

## Overview

The Platform Console is a unified admin dashboard that serves both Platform (CMS-backed) and CRM operations. It requires a single sign-on experience where:

1. CMS Service acts as the **JWT Issuer**
2. Platform Console authenticates users via CMS
3. The same JWT token is used for both CMS `/admin/*` and CRM `/admin/*` APIs
4. CRM Service validates the JWT (verifier only, doesn't issue tokens)

---

## Authentication Flow

```
┌─────────────────┐     POST /admin/login      ┌─────────────────┐
│  Platform       │ ────────────────────────▶  │  CMS Service    │
│  Console        │                            │  (JWT Issuer)   │
│  (Next.js)      │    { email, password }     │                 │
└─────────────────┘                            └─────────────────┘
        │                                               │
        │          JWT Token (HS256)                    │
        ◀───────────────────────────────────────────────┘
        │
        │  Store in localStorage
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│  All subsequent requests                                         │
│  Authorization: Bearer <token>                                   │
│                                                                  │
│  CMS /admin/*  ─────▶ Validates JWT (issuer)                    │
│  CRM /admin/*  ─────▶ Validates JWT (verifier only)             │
└─────────────────────────────────────────────────────────────────┘
```

---

## Required Endpoints

### 1. Login

**Endpoint:** `POST /admin/login`

**Purpose:** Authenticate user and issue JWT token

**Request Body:**

```json
{
  "email": "string (required)",
  "password": "string (required)"
}
```

**Success Response (200):**

```json
{
  "token": "eyJhbGciOiJIUzI1NiIs...",
  "user": {
    "id": "uuid-string",
    "email": "user@example.com",
    "role": "admin"
  }
}
```

**Error Responses:**

- `401 Unauthorized` - Invalid credentials
  ```json
  {
    "message": "Invalid email or password"
  }
  ```
- `400 Bad Request` - Missing fields
  ```json
  {
    "message": "Email and password are required"
  }
  ```
- `500 Internal Server Error` - Server error
  ```json
  {
    "message": "Internal server error"
  }
  ```

---

### 2. Get Current User (Token Validation)

**Endpoint:** `GET /admin/me`

**Purpose:** Validate token and get current user info

**Headers:**

```
Authorization: Bearer <jwt_token>
```

**Success Response (200):**

```json
{
  "id": "uuid-string",
  "email": "user@example.com",
  "role": "admin",
  "permissions": [
    "read:sources",
    "write:sources",
    "read:customers",
    "write:customers"
  ]
}
```

**Error Responses:**

- `401 Unauthorized` - Missing or invalid token
  ```json
  {
    "message": "Unauthorized"
  }
  ```
- `403 Forbidden` - Valid token but insufficient permissions
  ```json
  {
    "message": "Forbidden"
  }
  ```

---

## JWT Token Specification

### Algorithm

- **HS256** (HMAC with SHA-256)

### Secret

- Shared secret between CMS and CRM services
- Must be securely stored and rotated periodically

### Payload Structure

```json
{
  "sub": "user-uuid", // User ID (required)
  "email": "user@example.com", // User email (required)
  "role": "admin", // User role (required)
  "permissions": ["read:sources"], // Array of permissions (optional)
  "exp": 1700000000, // Expiration timestamp (required)
  "iat": 1699996400, // Issued at timestamp (required)
  "iss": "cms-service", // Issuer (optional)
  "aud": "platform-console" // Audience (optional)
}
```

### Roles

The Platform Console supports three roles:

| Role      | Description        | Platform Access              | CRM Access                       |
| --------- | ------------------ | ---------------------------- | -------------------------------- |
| `admin`   | Full system access | Full CRUD, trigger ingestion | Full access, manage pipelines    |
| `manager` | Team management    | View sources, content        | Manage team, deals, customers    |
| `agent`   | Limited access     | View content only            | Own customers, deals, activities |

### Token Expiration

- **Recommended:** 24 hours (86400 seconds)
- Console handles expiration by redirecting to login

---

## Error Handling Requirements

### HTTP Status Codes

- `200` - Success
- `400` - Bad Request (invalid input)
- `401` - Unauthorized (invalid/missing token)
- `403` - Forbidden (valid token, insufficient permissions)
- `500` - Internal Server Error

### Error Response Format

All errors should follow this format:

```json
{
  "message": "Human-readable error message",
  "code": "ERROR_CODE_OPTIONAL",
  "details": {} // Optional additional details
}
```

### Specific Error Messages

**Login Errors:**

- Invalid email format: `"Invalid email format"`
- Missing email: `"Email is required"`
- Missing password: `"Password is required"`
- Invalid credentials: `"Invalid email or password"`
- Account disabled: `"Account is disabled"`

**Token Errors:**

- Missing token: `"Authentication required"`
- Invalid token: `"Invalid authentication token"`
- Expired token: `"Token has expired"`
- Invalid signature: `"Invalid token signature"`

---

## CORS Configuration

The CMS service must allow CORS requests from the Platform Console:

```go
// Example Gin CORS middleware
func CORSMiddleware() gin.HandlerFunc {
    return func(c *gin.Context) {
        c.Writer.Header().Set("Access-Control-Allow-Origin", "https://console.turfa.com")
        c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
        c.Writer.Header().Set("Access-Control-Allow-Headers", "Origin, Content-Type, Accept, Authorization")
        c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")

        if c.Request.Method == "OPTIONS" {
            c.AbortWithStatus(204)
            return
        }

        c.Next()
    }
}
```

**Required Headers:**

- `Access-Control-Allow-Origin`: Console domain(s)
- `Access-Control-Allow-Methods`: `GET, POST, PUT, PATCH, DELETE, OPTIONS`
- `Access-Control-Allow-Headers`: `Origin, Content-Type, Accept, Authorization`

---

## Security Requirements

### Password Requirements

- Minimum 8 characters
- At least one uppercase letter
- At least one lowercase letter
- At least one number
- At least one special character (optional but recommended)

### Rate Limiting

Implement rate limiting on `/admin/login`:

- 5 attempts per IP per 15 minutes
- Return `429 Too Many Requests` when exceeded
- Include `Retry-After` header

### Token Security

- Use secure, randomly generated secret (min 256 bits)
- Store secret in environment variable, not code
- Rotate secrets periodically
- Use HTTPS only (no HTTP)

### Password Storage

- Never store plaintext passwords
- Use bcrypt, Argon2, or PBKDF2 for hashing
- Minimum cost factor: 10 (bcrypt)

---

## Testing Checklist

Before handing off to the Platform Console team, verify:

- [ ] `POST /admin/login` returns valid JWT on correct credentials
- [ ] `POST /admin/login` returns 401 on incorrect credentials
- [ ] `GET /admin/me` returns user data with valid token
- [ ] `GET /admin/me` returns 401 with invalid/expired token
- [ ] JWT payload contains all required fields (sub, email, role, exp, iat)
- [ ] Token expiration is reasonable (recommend 24 hours)
- [ ] CORS is configured for Console domain
- [ ] HTTPS is enforced in production
- [ ] Rate limiting is implemented on login
- [ ] Error messages are clear and helpful

---

## Example Implementation (Go/Gin)

```go
package main

import (
    "net/http"
    "time"

    "github.com/gin-gonic/gin"
    "github.com/golang-jwt/jwt/v5"
    "golang.org/x/crypto/bcrypt"
)

var jwtSecret = []byte(os.Getenv("JWT_SECRET"))

type LoginRequest struct {
    Email    string `json:"email" binding:"required,email"`
    Password string `json:"password" binding:"required"`
}

type User struct {
    ID       string   `json:"id"`
    Email    string   `json:"email"`
    Role     string   `json:"role"`
    Password string   `json:"-"` // hashed password
}

func loginHandler(c *gin.Context) {
    var req LoginRequest
    if err := c.ShouldBindJSON(&req); err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid request"})
        return
    }

    // Look up user in database
    user, err := findUserByEmail(req.Email)
    if err != nil {
        c.JSON(http.StatusUnauthorized, gin.H{"message": "Invalid email or password"})
        return
    }

    // Verify password
    if err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(req.Password)); err != nil {
        c.JSON(http.StatusUnauthorized, gin.H{"message": "Invalid email or password"})
        return
    }

    // Generate JWT
    token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
        "sub":  user.ID,
        "email": user.Email,
        "role": user.Role,
        "exp":  time.Now().Add(24 * time.Hour).Unix(),
        "iat":  time.Now().Unix(),
    })

    tokenString, err := token.SignedString(jwtSecret)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"message": "Failed to generate token"})
        return
    }

    c.JSON(http.StatusOK, gin.H{
        "token": tokenString,
        "user": gin.H{
            "id":    user.ID,
            "email": user.Email,
            "role":  user.Role,
        },
    })
}

func meHandler(c *gin.Context) {
    // Get user from context (set by auth middleware)
    user, exists := c.Get("user")
    if !exists {
        c.JSON(http.StatusUnauthorized, gin.H{"message": "Unauthorized"})
        return
    }

    c.JSON(http.StatusOK, user)
}

func authMiddleware() gin.HandlerFunc {
    return func(c *gin.Context) {
        authHeader := c.GetHeader("Authorization")
        if authHeader == "" {
            c.JSON(http.StatusUnauthorized, gin.H{"message": "Authentication required"})
            c.Abort()
            return
        }

        // Extract Bearer token
        tokenString := strings.TrimPrefix(authHeader, "Bearer ")
        if tokenString == authHeader {
            c.JSON(http.StatusUnauthorized, gin.H{"message": "Invalid authorization header"})
            c.Abort()
            return
        }

        // Parse and validate token
        token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
            if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
                return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
            }
            return jwtSecret, nil
        })

        if err != nil || !token.Valid {
            c.JSON(http.StatusUnauthorized, gin.H{"message": "Invalid token"})
            c.Abort()
            return
        }

        // Extract claims
        if claims, ok := token.Claims.(jwt.MapClaims); ok {
            c.Set("user", User{
                ID:    claims["sub"].(string),
                Email: claims["email"].(string),
                Role:  claims["role"].(string),
            })
        }

        c.Next()
    }
}

func main() {
    r := gin.Default()
    r.Use(CORSMiddleware())

    // Public routes
    r.POST("/admin/login", loginHandler)

    // Protected routes
    authorized := r.Group("/admin")
    authorized.Use(authMiddleware())
    {
        authorized.GET("/me", meHandler)
        // ... other protected routes
    }

    r.Run(":8080")
}
```

---

## Contact & Support

**Platform Console Team:**

- Frontend Repository: `/Users/salehalobaylan/Desktop/Wahb-Project/Platform-Console`
- Auth Store: `src/lib/stores/auth.ts`
- API Client: `src/lib/api/client.ts`
- Login Form: `src/components/auth/login-form.tsx`

**Integration Notes:**

1. Ensure JWT secret is shared between CMS and CRM services
2. Coordinate deployment to avoid auth disruptions
3. Test token validation across both services before production

---

**End of Document**
