package models

import (
	"fmt"
	"time"
)

// ValidSensorTypes contains all allowed sensor types.
var ValidSensorTypes = map[string]bool{
	"temperature": true,
	"motion":      true,
	"humidity":    true,
	"light":       true,
	"air_quality": true,
	"co2":         true,
	"contact":     true,
	"pressure":    true,
}

// ValidSensorUnits contains all allowed units of measurement.
var ValidSensorUnits = map[string]bool{
	"fahrenheit": true,
	"celsius":    true,
	"boolean":    true,
	"percent":    true,
	"aqi":        true,
	"ppm":        true,
	"lux":        true,
	"pascal":     true,
}

// ValidSensorStatuses contains all allowed sensor statuses.
var ValidSensorStatuses = map[string]bool{
	"active":   true,
	"inactive": true,
	"error":    true,
}

// Sensor represents a complete sensor record from the database.
type Sensor struct {
	ID          string  `json:"id"`
	Name        string  `json:"name"`
	Type        string  `json:"type"`
	Location    string  `json:"location"`
	Value       float64 `json:"value"`
	Unit        string  `json:"unit"`
	Status      string  `json:"status"`
	LastReading string  `json:"last_reading"`
	CreatedAt   string  `json:"created_at,omitempty"`
	UpdatedAt   string  `json:"updated_at,omitempty"`
}

// SensorCreate represents the request body for creating a new sensor.
type SensorCreate struct {
	Name     string  `json:"name" binding:"required,min=1,max=100"`
	Type     string  `json:"type" binding:"required"`
	Location string  `json:"location" binding:"required,min=1,max=100"`
	Value    float64 `json:"value"`
	Unit     string  `json:"unit" binding:"required,min=1,max=50"`
	Status   string  `json:"status" binding:"required"`
}

// Validate checks if the SensorCreate fields are valid.
func (s *SensorCreate) Validate() error {
	if !ValidSensorTypes[s.Type] {
		return fmt.Errorf("invalid sensor type: %s", s.Type)
	}
	if !ValidSensorUnits[s.Unit] {
		return fmt.Errorf("invalid sensor unit: %s", s.Unit)
	}
	if !ValidSensorStatuses[s.Status] {
		return fmt.Errorf("invalid sensor status: %s", s.Status)
	}
	return nil
}

// SensorUpdate represents the request body for updating a sensor.
// All fields are optional (pointers).
type SensorUpdate struct {
	Name     *string  `json:"name,omitempty"`
	Type     *string  `json:"type,omitempty"`
	Location *string  `json:"location,omitempty"`
	Value    *float64 `json:"value,omitempty"`
	Unit     *string  `json:"unit,omitempty"`
	Status   *string  `json:"status,omitempty"`
}

// Validate checks if the SensorUpdate fields are valid.
func (s *SensorUpdate) Validate() error {
	if s.Type != nil && !ValidSensorTypes[*s.Type] {
		return fmt.Errorf("invalid sensor type: %s", *s.Type)
	}
	if s.Unit != nil && !ValidSensorUnits[*s.Unit] {
		return fmt.Errorf("invalid sensor unit: %s", *s.Unit)
	}
	if s.Status != nil && !ValidSensorStatuses[*s.Status] {
		return fmt.Errorf("invalid sensor status: %s", *s.Status)
	}
	return nil
}

// SensorList represents the response for listing sensors.
type SensorList struct {
	Sensors []Sensor `json:"sensors"`
	Count   int      `json:"count"`
}

// HealthResponse represents the health check response.
type HealthResponse struct {
	Status  string `json:"status"`
	Service string `json:"service"`
}

// ErrorResponse represents an error response (RFC 7807 compatible).
type ErrorResponse struct {
	Detail string `json:"detail"`
}

// Now returns the current UTC time as an ISO 8601 string.
func Now() string {
	return time.Now().UTC().Format(time.RFC3339)
}
