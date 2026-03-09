package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"iot-sensor-service/models"
)

// HealthHandler handles the health check endpoint.
type HealthHandler struct{}

// NewHealthHandler creates a new health handler.
func NewHealthHandler() *HealthHandler {
	return &HealthHandler{}
}

// Health returns the service health status.
func (h *HealthHandler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, models.HealthResponse{
		Status:  "ok",
		Service: "go",
	})
}
