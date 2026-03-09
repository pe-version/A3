package middleware

import (
	"log/slog"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const CorrelationIDKey = "correlation_id"
const CorrelationIDHeader = "X-Correlation-ID"

// LoggingMiddleware adds correlation IDs and logs requests with structured logging.
func LoggingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		correlationID := c.GetHeader(CorrelationIDHeader)
		if correlationID == "" {
			correlationID = uuid.New().String()
		}

		c.Set(CorrelationIDKey, correlationID)

		start := time.Now()
		c.Next()

		slog.Info("Request completed",
			"correlation_id", correlationID,
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"duration_ms", time.Since(start).Milliseconds(),
		)

		c.Header(CorrelationIDHeader, correlationID)
	}
}

// GetCorrelationID retrieves the correlation ID from the Gin context.
func GetCorrelationID(c *gin.Context) string {
	if id, exists := c.Get(CorrelationIDKey); exists {
		return id.(string)
	}
	return ""
}
