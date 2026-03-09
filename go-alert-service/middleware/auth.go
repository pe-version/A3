package middleware

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"iot-alert-service/models"
)

// AuthMiddleware validates Bearer token authentication.
func AuthMiddleware(validToken string) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")

		if authHeader == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, models.ErrorResponse{
				Detail: "Not authenticated",
			})
			return
		}

		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, models.ErrorResponse{
				Detail: "Invalid authorization format. Use: Bearer <token>",
			})
			return
		}

		if parts[1] != validToken {
			c.AbortWithStatusJSON(http.StatusUnauthorized, models.ErrorResponse{
				Detail: "Invalid or expired token",
			})
			return
		}

		c.Next()
	}
}
