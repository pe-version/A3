package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"iot-alert-service/models"
	"iot-alert-service/repositories"
)

// TriggeredAlertHandler handles triggered alert operations.
type TriggeredAlertHandler struct {
	repo repositories.TriggeredAlertRepository
}

// NewTriggeredAlertHandler creates a new triggered alert handler.
func NewTriggeredAlertHandler(repo repositories.TriggeredAlertRepository) *TriggeredAlertHandler {
	return &TriggeredAlertHandler{repo: repo}
}

// ListAlerts returns all triggered alerts.
func (h *TriggeredAlertHandler) ListAlerts(c *gin.Context) {
	alerts, err := h.repo.GetAll()
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Detail: "Failed to retrieve triggered alerts",
		})
		return
	}

	c.JSON(http.StatusOK, models.TriggeredAlertList{
		Alerts: alerts,
		Count:  len(alerts),
	})
}

// GetAlert returns a single triggered alert by ID.
func (h *TriggeredAlertHandler) GetAlert(c *gin.Context) {
	alertID := c.Param("id")

	alert, err := h.repo.GetByID(alertID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Detail: "Failed to retrieve triggered alert",
		})
		return
	}

	if alert == nil {
		c.JSON(http.StatusNotFound, models.ErrorResponse{
			Detail: "No triggered alert with id '" + alertID + "'",
		})
		return
	}

	c.JSON(http.StatusOK, alert)
}

// UpdateAlert updates the status of a triggered alert.
func (h *TriggeredAlertHandler) UpdateAlert(c *gin.Context) {
	alertID := c.Param("id")

	var input models.TriggeredAlertUpdate
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Detail: "Invalid request body: " + err.Error(),
		})
		return
	}

	if err := input.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Detail: err.Error(),
		})
		return
	}

	alert, err := h.repo.UpdateStatus(alertID, *input.Status)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Detail: "Failed to update triggered alert",
		})
		return
	}

	if alert == nil {
		c.JSON(http.StatusNotFound, models.ErrorResponse{
			Detail: "No triggered alert with id '" + alertID + "'",
		})
		return
	}

	c.JSON(http.StatusOK, alert)
}
