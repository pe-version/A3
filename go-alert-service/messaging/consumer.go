package messaging

import (
	"encoding/json"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"iot-alert-service/metrics"
)

// SensorEvent represents a sensor.updated event from RabbitMQ.
type SensorEvent struct {
	Event     string  `json:"event"`
	SensorID  string  `json:"sensor_id"`
	Value     float64 `json:"value"`
	Type      string  `json:"type"`
	Unit      string  `json:"unit"`
	Timestamp string  `json:"timestamp"`
	TraceID   string  `json:"trace_id"`
}

// AlertConsumer consumes sensor.updated events from RabbitMQ.
type AlertConsumer struct {
	url      string
	callback func(SensorEvent)
	// async pipeline fields; nil when running in blocking mode
	eventCh chan SensorEvent
}

// NewAlertConsumer creates a new alert consumer in blocking mode.
func NewAlertConsumer(url string, callback func(SensorEvent)) *AlertConsumer {
	return &AlertConsumer{url: url, callback: callback}
}

// NewAsyncAlertConsumer creates a consumer that dispatches events to a
// buffered worker pool. workerCount goroutines drain the channel; when the
// channel is full the consumer blocks (backpressure).
func NewAsyncAlertConsumer(url string, callback func(SensorEvent), workerCount int) *AlertConsumer {
	// Buffer = workerCount * 10 gives workers headroom without unbounded queuing.
	ch := make(chan SensorEvent, workerCount*10)
	c := &AlertConsumer{url: url, callback: callback, eventCh: ch}
	for i := 0; i < workerCount; i++ {
		go c.worker()
	}
	return c
}

func (c *AlertConsumer) worker() {
	for event := range c.eventCh {
		func() {
			defer func() {
				if r := recover(); r != nil {
					slog.Error("Worker panic recovered", "error", r, "sensor_id", event.SensorID)
				}
			}()
			c.callback(event)
		}()
	}
}

// Start begins consuming in a background goroutine with reconnect logic.
func (c *AlertConsumer) Start() {
	mode := "blocking"
	if c.eventCh != nil {
		mode = "async"
	}
	go c.consumeLoop()
	slog.Info("Alert consumer started", "mode", mode)
}

func (c *AlertConsumer) consumeLoop() {
	for {
		err := c.consume()
		if err != nil {
			slog.Error("Consumer connection lost — reconnecting in 5s", "error", err.Error())
			time.Sleep(5 * time.Second)
		}
	}
}

func (c *AlertConsumer) consume() error {
	conn, err := amqp.Dial(c.url)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	err = ch.ExchangeDeclare("sensor_events", "fanout", true, false, false, false, nil)
	if err != nil {
		return err
	}

	q, err := ch.QueueDeclare("alert_service_go", true, false, false, false, nil)
	if err != nil {
		return err
	}

	err = ch.QueueBind(q.Name, "", "sensor_events", false, nil)
	if err != nil {
		return err
	}

	// Prefetch limits how many unacked messages RabbitMQ sends at once.
	err = ch.Qos(10, 0, false)
	if err != nil {
		return err
	}

	// autoAck=false: ack only after successful processing so messages are
	// not lost if the evaluator crashes mid-flight.
	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	slog.Info("Connected to RabbitMQ, waiting for sensor events")

	for msg := range msgs {
		var event SensorEvent
		if err := json.Unmarshal(msg.Body, &event); err != nil {
			slog.Warn("Received invalid JSON message", "error", err.Error())
			// Nack without requeue — malformed messages cannot be retried usefully.
			msg.Nack(false, false)
			continue
		}
		if event.Event == "sensor.updated" {
			metrics.EventsReceived.Add(1)
			slog.Info("Received sensor.updated event",
				"sensor_id", event.SensorID, "value", event.Value, "trace_id", event.TraceID)
			if c.eventCh != nil {
				// Async: send to worker pool; blocks if channel is full (backpressure).
				c.eventCh <- event
			} else {
				// Blocking: evaluate inline before acking.
				c.callback(event)
			}
		}
		msg.Ack(false)
	}

	return nil
}
