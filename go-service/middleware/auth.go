package middleware

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// AuthMiddleware validates Bearer token authentication.
func AuthMiddleware(validToken string) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")

		if authHeader == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"detail": "Not authenticated",
			})
			return
		}

		// Parse "Bearer <token>" format
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"detail": "Invalid authorization format. Use: Bearer <token>",
			})
			return
		}

		token := parts[1]
		if token != validToken {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"detail": "Invalid or expired token",
			})
			return
		}

		// Token is valid, continue to next handler
		c.Next()
	}
}
