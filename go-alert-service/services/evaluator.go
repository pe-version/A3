package services

import (
	"fmt"
	"log/slog"
	"time"

	"iot-alert-service/messaging"
	"iot-alert-service/metrics"
	"iot-alert-service/repositories"
)

// AlertEvaluator evaluates sensor update events against active alert rules.
type AlertEvaluator struct {
	ruleRepo  repositories.AlertRuleRepository
	alertRepo repositories.TriggeredAlertRepository
}

// NewAlertEvaluator creates a new alert evaluator.
func NewAlertEvaluator(ruleRepo repositories.AlertRuleRepository, alertRepo repositories.TriggeredAlertRepository) *AlertEvaluator {
	return &AlertEvaluator{ruleRepo: ruleRepo, alertRepo: alertRepo}
}

// Evaluate checks a sensor event against all active rules for that sensor.
func (e *AlertEvaluator) Evaluate(event messaging.SensorEvent) {
	start := time.Now()
	defer func() {
		metrics.RecordProcessingDuration(start)
		metrics.EventsProcessed.Add(1)
	}()

	rules, err := e.ruleRepo.GetActiveRulesForSensor(event.SensorID)
	if err != nil {
		slog.Error("Failed to get active rules", "sensor_id", event.SensorID, "trace_id", event.TraceID, "error", err.Error())
		return
	}

	for _, rule := range rules {
		if thresholdCrossed(event.Value, rule.Operator, rule.Threshold) {
			message := fmt.Sprintf("Sensor %s value %.2f %s threshold %.2f (rule: %s)",
				event.SensorID, event.Value, rule.Operator, rule.Threshold, rule.Name)

			alert, err := e.alertRepo.Create(rule.ID, event.SensorID, event.Value, rule.Threshold, message)
			if err != nil {
				slog.Error("Failed to create triggered alert", "rule_id", rule.ID, "trace_id", event.TraceID, "error", err.Error())
				continue
			}

			metrics.AlertsTriggered.Add(1)
			slog.Info("Alert triggered", "alert_id", alert.ID, "rule_id", rule.ID, "trace_id", event.TraceID, "message", message)
		}
	}
}

func thresholdCrossed(value float64, operator string, threshold float64) bool {
	switch operator {
	case "gt":
		return value > threshold
	case "lt":
		return value < threshold
	case "gte":
		return value >= threshold
	case "lte":
		return value <= threshold
	case "eq":
		return value == threshold
	default:
		return false
	}
}
