package models

import (
	"fmt"
	"time"
)

// ValidOperators contains all allowed comparison operators.
var ValidOperators = map[string]bool{
	"gt":  true,
	"lt":  true,
	"gte": true,
	"lte": true,
	"eq":  true,
}

// ValidRuleStatuses contains all allowed rule statuses.
var ValidRuleStatuses = map[string]bool{
	"active":   true,
	"inactive": true,
}

// AlertRule represents a complete alert rule record from the database.
type AlertRule struct {
	ID        string  `json:"id"`
	SensorID  string  `json:"sensor_id"`
	Metric    string  `json:"metric"`
	Operator  string  `json:"operator"`
	Threshold float64 `json:"threshold"`
	Name      string  `json:"name"`
	Status    string  `json:"status"`
	CreatedAt string  `json:"created_at"`
	UpdatedAt string  `json:"updated_at"`
}

// AlertRuleCreate represents the request body for creating a new alert rule.
type AlertRuleCreate struct {
	SensorID  string  `json:"sensor_id" binding:"required,min=1"`
	Metric    string  `json:"metric"`
	Operator  string  `json:"operator" binding:"required"`
	Threshold float64 `json:"threshold"`
	Name      string  `json:"name" binding:"required,min=1,max=200"`
	Status    string  `json:"status"`
}

// Validate checks if the AlertRuleCreate fields are valid.
func (r *AlertRuleCreate) Validate() error {
	if !ValidOperators[r.Operator] {
		return fmt.Errorf("invalid operator: %s", r.Operator)
	}
	if r.Status == "" {
		r.Status = "active"
	}
	if !ValidRuleStatuses[r.Status] {
		return fmt.Errorf("invalid rule status: %s", r.Status)
	}
	if r.Metric == "" {
		r.Metric = "value"
	}
	return nil
}

// AlertRuleUpdate represents the request body for updating an alert rule.
// All fields are optional (pointers).
type AlertRuleUpdate struct {
	SensorID  *string  `json:"sensor_id,omitempty"`
	Metric    *string  `json:"metric,omitempty"`
	Operator  *string  `json:"operator,omitempty"`
	Threshold *float64 `json:"threshold,omitempty"`
	Name      *string  `json:"name,omitempty"`
	Status    *string  `json:"status,omitempty"`
}

// Validate checks if the AlertRuleUpdate fields are valid.
func (r *AlertRuleUpdate) Validate() error {
	if r.Operator != nil && !ValidOperators[*r.Operator] {
		return fmt.Errorf("invalid operator: %s", *r.Operator)
	}
	if r.Status != nil && !ValidRuleStatuses[*r.Status] {
		return fmt.Errorf("invalid rule status: %s", *r.Status)
	}
	return nil
}

// AlertRuleResponse extends AlertRule with an optional warning field.
// The warning is populated when the sensor service is unavailable during
// rule creation (circuit breaker fallback).
type AlertRuleResponse struct {
	ID        string  `json:"id"`
	SensorID  string  `json:"sensor_id"`
	Metric    string  `json:"metric"`
	Operator  string  `json:"operator"`
	Threshold float64 `json:"threshold"`
	Name      string  `json:"name"`
	Status    string  `json:"status"`
	CreatedAt string  `json:"created_at"`
	UpdatedAt string  `json:"updated_at"`
	Warning   string  `json:"warning,omitempty"`
}

// AlertRuleList represents the response for listing alert rules.
type AlertRuleList struct {
	Rules []AlertRule `json:"rules"`
	Count int         `json:"count"`
}

// Now returns the current UTC time as an ISO 8601 string.
func Now() string {
	return time.Now().UTC().Format(time.RFC3339)
}
