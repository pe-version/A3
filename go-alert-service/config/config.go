package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config holds application configuration loaded from environment variables.
type Config struct {
	Port             int
	DatabasePath     string
	APIToken         string
	LogLevel         string
	LogFormat        string
	SeedDataPath     string
	SensorServiceURL string
	RabbitMQURL      string
	CBFailMax        int
	CBResetTimeout   int
}

// Load reads configuration from environment variables.
func Load() (*Config, error) {
	port, err := strconv.Atoi(getEnv("PORT", "8081"))
	if err != nil {
		port = 8081
	}

	apiToken := os.Getenv("API_TOKEN")
	if apiToken == "" {
		return nil, fmt.Errorf("API_TOKEN environment variable is required")
	}

	cbFailMax, err := strconv.Atoi(getEnv("CB_FAIL_MAX", "5"))
	if err != nil {
		cbFailMax = 5
	}

	cbResetTimeout, err := strconv.Atoi(getEnv("CB_RESET_TIMEOUT", "30"))
	if err != nil {
		cbResetTimeout = 30
	}

	return &Config{
		Port:             port,
		DatabasePath:     getEnv("DATABASE_PATH", "/app/data/alerts-go.db"),
		APIToken:         apiToken,
		LogLevel:         getEnv("LOG_LEVEL", "INFO"),
		LogFormat:        getEnv("LOG_FORMAT", "json"),
		SeedDataPath:     getEnv("SEED_DATA_PATH", "/app/data/alert_rules.json"),
		SensorServiceURL: getEnv("SENSOR_SERVICE_URL", "http://go-service:8080"),
		RabbitMQURL:      getEnv("RABBITMQ_URL", "amqp://iot_service:iot_secret@rabbitmq:5672/"),
		CBFailMax:        cbFailMax,
		CBResetTimeout:   cbResetTimeout,
	}, nil
}

// getEnv returns the value of an environment variable or a default value.
func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
