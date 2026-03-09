package handlers

import (
	"database/sql"
	"net/http"

	"github.com/gin-gonic/gin"

	"iot-alert-service/clients"
	"iot-alert-service/models"
	"iot-alert-service/repositories"
)

// AlertRuleHandler handles alert rule CRUD operations.
type AlertRuleHandler struct {
	repo         repositories.AlertRuleRepository
	sensorClient *clients.SensorClient
}

// NewAlertRuleHandler creates a new alert rule handler.
func NewAlertRuleHandler(repo repositories.AlertRuleRepository, sensorClient *clients.SensorClient) *AlertRuleHandler {
	return &AlertRuleHandler{repo: repo, sensorClient: sensorClient}
}

// ListRules returns all alert rules.
func (h *AlertRuleHandler) ListRules(c *gin.Context) {
	rules, err := h.repo.GetAll()
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Detail: "Failed to retrieve alert rules",
		})
		return
	}

	c.JSON(http.StatusOK, models.AlertRuleList{
		Rules: rules,
		Count: len(rules),
	})
}

// GetRule returns a single alert rule by ID.
func (h *AlertRuleHandler) GetRule(c *gin.Context) {
	ruleID := c.Param("id")

	rule, err := h.repo.GetByID(ruleID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Detail: "Failed to retrieve alert rule",
		})
		return
	}

	if rule == nil {
		c.JSON(http.StatusNotFound, models.ErrorResponse{
			Detail: "No alert rule with id '" + ruleID + "'",
		})
		return
	}

	c.JSON(http.StatusOK, rule)
}

// CreateRule creates a new alert rule, validating the sensor exists via the sensor service.
func (h *AlertRuleHandler) CreateRule(c *gin.Context) {
	var input models.AlertRuleCreate
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

	// Validate sensor exists via circuit breaker client
	var warning string
	_, validated, err := h.sensorClient.GetSensor(input.SensorID)
	if err != nil {
		// Sensor confirmed not found (404)
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Detail: "Sensor '" + input.SensorID + "' not found",
		})
		return
	}
	if !validated {
		// Sensor service unavailable — allow creation with warning
		warning = "Sensor service unavailable; sensor_id not validated"
	}

	rule, err := h.repo.Create(&input)
	if err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Detail: err.Error(),
		})
		return
	}

	resp := models.AlertRuleResponse{
		ID:        rule.ID,
		SensorID:  rule.SensorID,
		Metric:    rule.Metric,
		Operator:  rule.Operator,
		Threshold: rule.Threshold,
		Name:      rule.Name,
		Status:    rule.Status,
		CreatedAt: rule.CreatedAt,
		UpdatedAt: rule.UpdatedAt,
		Warning:   warning,
	}

	c.JSON(http.StatusCreated, resp)
}

// UpdateRule updates an existing alert rule.
func (h *AlertRuleHandler) UpdateRule(c *gin.Context) {
	ruleID := c.Param("id")

	var input models.AlertRuleUpdate
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Detail: "Invalid request body: " + err.Error(),
		})
		return
	}

	rule, err := h.repo.Update(ruleID, &input)
	if err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Detail: err.Error(),
		})
		return
	}

	if rule == nil {
		c.JSON(http.StatusNotFound, models.ErrorResponse{
			Detail: "No alert rule with id '" + ruleID + "'",
		})
		return
	}

	c.JSON(http.StatusOK, rule)
}

// DeleteRule removes an alert rule.
func (h *AlertRuleHandler) DeleteRule(c *gin.Context) {
	ruleID := c.Param("id")

	err := h.repo.Delete(ruleID)
	if err != nil {
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, models.ErrorResponse{
				Detail: "No alert rule with id '" + ruleID + "'",
			})
			return
		}
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Detail: "Failed to delete alert rule",
		})
		return
	}

	c.Status(http.StatusNoContent)
}
