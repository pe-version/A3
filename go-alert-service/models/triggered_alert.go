package models

import "fmt"

// ValidAlertStatuses contains all allowed triggered alert statuses.
var ValidAlertStatuses = map[string]bool{
	"open":         true,
	"acknowledged": true,
	"resolved":     true,
}

// TriggeredAlert represents a complete triggered alert record from the database.
type TriggeredAlert struct {
	ID          string  `json:"id"`
	RuleID      string  `json:"rule_id"`
	SensorID    string  `json:"sensor_id"`
	SensorValue float64 `json:"sensor_value"`
	Threshold   float64 `json:"threshold"`
	Message     string  `json:"message"`
	Status      string  `json:"status"`
	CreatedAt   string  `json:"created_at"`
	ResolvedAt  *string `json:"resolved_at"`
}

// TriggeredAlertUpdate represents the request body for updating a triggered alert.
type TriggeredAlertUpdate struct {
	Status *string `json:"status" binding:"required"`
}

// Validate checks if the TriggeredAlertUpdate fields are valid.
func (u *TriggeredAlertUpdate) Validate() error {
	if u.Status != nil && !ValidAlertStatuses[*u.Status] {
		return fmt.Errorf("invalid alert status: %s", *u.Status)
	}
	return nil
}

// TriggeredAlertList represents the response for listing triggered alerts.
type TriggeredAlertList struct {
	Alerts []TriggeredAlert `json:"alerts"`
	Count  int              `json:"count"`
}

// HealthResponse represents the health check response.
type HealthResponse struct {
	Status  string `json:"status"`
	Service string `json:"service"`
}

// ErrorResponse represents an error response.
type ErrorResponse struct {
	Detail string `json:"detail"`
}
