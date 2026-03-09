/*
IoT Sensor Service - Go (Gin)

A RESTful API for managing IoT sensor devices with SQLite persistence
and Bearer token authentication.
*/
package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/gin-gonic/gin"

	"iot-sensor-service/config"
	"iot-sensor-service/database"
	"iot-sensor-service/handlers"
	"iot-sensor-service/messaging"
	"iot-sensor-service/middleware"
	"iot-sensor-service/repositories"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		slog.Error("Failed to load configuration", "error", err)
		os.Exit(1)
	}

	// Connect to database
	db, err := database.Connect(cfg.DatabasePath)
	if err != nil {
		slog.Error("Failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	// Initialize database schema
	if err := database.InitSchema(db); err != nil {
		slog.Error("Failed to initialize database schema", "error", err)
		os.Exit(1)
	}

	// Seed data from JSON if table is empty
	if err := database.SeedFromJSON(db, cfg.SeedDataPath); err != nil {
		slog.Error("Failed to seed database", "error", err)
		os.Exit(1)
	}

	// Create repository
	sensorRepo := repositories.NewSQLiteSensorRepository(db)

	// Create event publisher (connects to RabbitMQ; tolerates unavailability)
	publisher := messaging.NewEventPublisher(cfg.RabbitMQURL)

	// Create handlers
	healthHandler := handlers.NewHealthHandler()
	sensorHandler := handlers.NewSensorHandler(sensorRepo, publisher)

	// Set up router
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()

	// Add global middleware
	router.Use(gin.Recovery())
	router.Use(middleware.LoggingMiddleware())

	// Health endpoint - unauthenticated for load balancer/orchestrator probes
	router.GET("/health", healthHandler.Health)

	// Protected routes - require Bearer token authentication
	protected := router.Group("/")
	protected.Use(middleware.AuthMiddleware(cfg.APIToken))
	protected.GET("/sensors", sensorHandler.ListSensors)
	protected.GET("/sensors/:id", sensorHandler.GetSensor)
	protected.POST("/sensors", sensorHandler.CreateSensor)
	protected.PUT("/sensors/:id", sensorHandler.UpdateSensor)
	protected.DELETE("/sensors/:id", sensorHandler.DeleteSensor)

	// Start server
	addr := fmt.Sprintf(":%d", cfg.Port)
	slog.Info("Starting Go IoT Sensor Service", "port", cfg.Port)
	if err := router.Run(addr); err != nil {
		slog.Error("Failed to start server", "error", err)
		os.Exit(1)
	}
}
