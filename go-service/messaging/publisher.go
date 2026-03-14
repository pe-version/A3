package messaging

import (
	"encoding/json"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// SensorEvent represents the sensor.updated event published to RabbitMQ.
type SensorEvent struct {
	Event     string  `json:"event"`
	SensorID  string  `json:"sensor_id"`
	Value     float64 `json:"value"`
	Type      string  `json:"type"`
	Unit      string  `json:"unit"`
	Timestamp string  `json:"timestamp"`
	TraceID   string  `json:"trace_id"`
}

// EventPublisher publishes sensor events to RabbitMQ.
// Uses a fanout exchange so all bound queues (e.g. alert services) receive every event.
// Reconnects automatically on failure — publish errors are logged and swallowed
// so a RabbitMQ outage never breaks the HTTP response.
type EventPublisher struct {
	url     string
	conn    *amqp.Connection
	channel *amqp.Channel
}

// NewEventPublisher creates a new publisher. It attempts an initial connection
// but does not fail if RabbitMQ is not yet available.
func NewEventPublisher(url string) *EventPublisher {
	p := &EventPublisher{url: url}
	if err := p.connect(); err != nil {
		slog.Warn("RabbitMQ not available at startup — will retry on first publish", "error", err)
	}
	return p
}

func (p *EventPublisher) connect() error {
	conn, err := amqp.Dial(p.url)
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return err
	}

	if err := ch.ExchangeDeclare("sensor_events", "fanout", true, false, false, false, nil); err != nil {
		ch.Close()
		conn.Close()
		return err
	}

	p.conn = conn
	p.channel = ch
	slog.Info("Connected to RabbitMQ for publishing")
	return nil
}

// PublishSensorUpdated publishes a sensor.updated event to the sensor_events exchange.
// If RabbitMQ is unavailable the error is logged and the call returns silently.
func (p *EventPublisher) PublishSensorUpdated(sensorID string, value float64, sensorType, unit, traceID string) {
	event := SensorEvent{
		Event:     "sensor.updated",
		SensorID:  sensorID,
		Value:     value,
		Type:      sensorType,
		Unit:      unit,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		TraceID:   traceID,
	}

	body, err := json.Marshal(event)
	if err != nil {
		slog.Error("Failed to marshal sensor event", "error", err)
		return
	}

	if err := p.publish(body); err != nil {
		slog.Warn("Failed to publish sensor event, attempting reconnect", "error", err)
		p.conn = nil
		p.channel = nil

		if reconnErr := p.connect(); reconnErr != nil {
			slog.Warn("RabbitMQ reconnect failed — event dropped", "sensor_id", sensorID, "error", reconnErr)
			return
		}

		if err := p.publish(body); err != nil {
			slog.Warn("Publish failed after reconnect — event dropped", "sensor_id", sensorID, "error", err)
			return
		}
	}

	slog.Info("Published sensor.updated event", "sensor_id", sensorID, "value", value, "trace_id", traceID)
}

func (p *EventPublisher) publish(body []byte) error {
	if p.channel == nil {
		return amqp.ErrClosed
	}
	return p.channel.Publish(
		"sensor_events", // exchange
		"",              // routing key (fanout ignores it)
		false,           // mandatory
		false,           // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
		},
	)
}
