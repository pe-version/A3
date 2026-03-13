package clients

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"time"

	"github.com/sony/gobreaker"
)

// SensorClient is an HTTP client for the sensor service with circuit breaker
// and retry logic for resilient inter-service communication.
type SensorClient struct {
	baseURL  string
	apiToken string
	cb       *gobreaker.CircuitBreaker
	client   *http.Client
}

// NewSensorClient creates a new sensor client with circuit breaker.
func NewSensorClient(baseURL, apiToken string, failMax int, resetTimeout int) *SensorClient {
	settings := gobreaker.Settings{
		Name:        "sensor-service",
		MaxRequests: 1,
		Interval:    time.Duration(resetTimeout) * time.Second,
		Timeout:     time.Duration(resetTimeout) * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= uint32(failMax)
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			slog.Warn("Circuit breaker state changed",
				"name", name, "from", from.String(), "to", to.String())
		},
	}

	return &SensorClient{
		baseURL:  baseURL,
		apiToken: apiToken,
		cb:       gobreaker.NewCircuitBreaker(settings),
		client:   &http.Client{Timeout: 2 * time.Second},
	}
}

// GetSensor retrieves sensor data via the circuit breaker.
// Returns (sensor_data, is_validated, error):
//   - (data, true, nil): Sensor found and validated
//   - (nil, true, error): Sensor confirmed not found (404)
//   - (nil, false, nil): Sensor service unavailable (fallback)
func (sc *SensorClient) GetSensor(sensorID string) (map[string]interface{}, bool, error) {
	result, err := sc.cb.Execute(func() (interface{}, error) {
		return sc.makeRequestWithRetry(sensorID, 3)
	})

	if err != nil {
		slog.Warn("Sensor service unavailable",
			"sensor_id", sensorID, "error", err.Error())
		return nil, false, nil
	}

	if result == nil {
		return nil, true, fmt.Errorf("sensor not found")
	}

	return result.(map[string]interface{}), true, nil
}

// makeRequestWithRetry makes an HTTP request with retry and exponential backoff.
func (sc *SensorClient) makeRequestWithRetry(sensorID string, maxRetries int) (interface{}, error) {
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		var backoff time.Duration
		if attempt > 0 {
			backoff = time.Duration(math.Pow(2, float64(attempt-1))) * time.Second
			time.Sleep(backoff)
		}

		result, err := sc.makeRequest(sensorID)
		if err == nil {
			return result, nil
		}
		lastErr = err
		slog.Warn("Sensor service request failed, retrying",
			"sensor_id", sensorID, "attempt", attempt+1, "max_retries", maxRetries,
			"backoff_ms", backoff.Milliseconds(), "error", err.Error())
	}

	slog.Warn("All retries exhausted for sensor service",
		"sensor_id", sensorID, "max_retries", maxRetries, "error", lastErr.Error())
	return nil, lastErr
}

// makeRequest makes a single HTTP request to the sensor service.
func (sc *SensorClient) makeRequest(sensorID string) (interface{}, error) {
	url := fmt.Sprintf("%s/sensors/%s", sc.baseURL, sensorID)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+sc.apiToken)

	resp, err := sc.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("sensor service returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	return result, nil
}
