package handlers

import (
	"database/sql"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"iot-sensor-service/messaging"
	"iot-sensor-service/models"
	"iot-sensor-service/repositories"
)

// SensorHandler handles sensor CRUD operations.
type SensorHandler struct {
	repo      repositories.SensorRepository
	publisher *messaging.EventPublisher
}

// NewSensorHandler creates a new sensor handler with the given repository and event publisher.
func NewSensorHandler(repo repositories.SensorRepository, publisher *messaging.EventPublisher) *SensorHandler {
	return &SensorHandler{repo: repo, publisher: publisher}
}

// ListSensors returns all sensors.
func (h *SensorHandler) ListSensors(c *gin.Context) {
	sensors, err := h.repo.GetAll()
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Detail: "Failed to retrieve sensors",
		})
		return
	}

	c.JSON(http.StatusOK, models.SensorList{
		Sensors: sensors,
		Count:   len(sensors),
	})
}

// GetSensor returns a single sensor by ID.
func (h *SensorHandler) GetSensor(c *gin.Context) {
	sensorID := c.Param("id")

	sensor, err := h.repo.GetByID(sensorID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Detail: "Failed to retrieve sensor",
		})
		return
	}

	if sensor == nil {
		c.JSON(http.StatusNotFound, models.ErrorResponse{
			Detail: "No sensor with id '" + sensorID + "'",
		})
		return
	}

	c.JSON(http.StatusOK, sensor)
}

// CreateSensor creates a new sensor.
func (h *SensorHandler) CreateSensor(c *gin.Context) {
	var input models.SensorCreate
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Detail: "Invalid request body: " + err.Error(),
		})
		return
	}

	sensor, err := h.repo.Create(&input)
	if err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Detail: err.Error(),
		})
		return
	}

	c.JSON(http.StatusCreated, sensor)
}

// UpdateSensor updates an existing sensor.
func (h *SensorHandler) UpdateSensor(c *gin.Context) {
	sensorID := c.Param("id")

	var input models.SensorUpdate
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Detail: "Invalid request body: " + err.Error(),
		})
		return
	}

	sensor, err := h.repo.Update(sensorID, &input)
	if err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Detail: err.Error(),
		})
		return
	}

	if sensor == nil {
		c.JSON(http.StatusNotFound, models.ErrorResponse{
			Detail: "No sensor with id '" + sensorID + "'",
		})
		return
	}

	// Generate trace ID to follow this event through the pipeline
	traceID := uuid.New().String()
	slog.Info("Sensor updated, publishing event", "sensor_id", sensor.ID, "trace_id", traceID)

	// Publish sensor.updated event for alert services to consume
	if h.publisher != nil {
		go h.publisher.PublishSensorUpdated(sensor.ID, sensor.Value, sensor.Type, sensor.Unit, traceID)
	}

	c.JSON(http.StatusOK, sensor)
}

// DeleteSensor removes a sensor.
func (h *SensorHandler) DeleteSensor(c *gin.Context) {
	sensorID := c.Param("id")

	err := h.repo.Delete(sensorID)
	if err != nil {
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, models.ErrorResponse{
				Detail: "No sensor with id '" + sensorID + "'",
			})
			return
		}
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Detail: "Failed to delete sensor",
		})
		return
	}

	c.Status(http.StatusNoContent)
}
