package middleware

import (
	"log/slog"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// CorrelationIDKey is the context key for the correlation ID.
const CorrelationIDKey = "correlation_id"

// CorrelationIDHeader is the HTTP header name for the correlation ID.
const CorrelationIDHeader = "X-Correlation-ID"

// LoggingMiddleware adds correlation IDs and logs requests with structured logging.
func LoggingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Get or generate correlation ID
		correlationID := c.GetHeader(CorrelationIDHeader)
		if correlationID == "" {
			correlationID = uuid.New().String()
		}

		// Store in context for access by handlers
		c.Set(CorrelationIDKey, correlationID)

		// Record start time
		start := time.Now()

		// Process request
		c.Next()

		// Calculate duration
		duration := time.Since(start)

		// Log request completion
		slog.Info("Request completed",
			"correlation_id", correlationID,
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"duration_ms", duration.Milliseconds(),
		)

		// Add correlation ID to response header
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
