/*
IoT Alert Service - Go (Gin)

A RESTful API for managing alert rules and triggered alerts with SQLite persistence,
Bearer token authentication, circuit breaker resilience, and RabbitMQ event consumption.
*/
package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/gin-gonic/gin"

	"iot-alert-service/clients"
	"iot-alert-service/config"
	"iot-alert-service/database"
	"iot-alert-service/handlers"
	"iot-alert-service/messaging"
	"iot-alert-service/metrics"
	"iot-alert-service/middleware"
	"iot-alert-service/repositories"
	"iot-alert-service/services"
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

	// Create repositories
	ruleRepo := repositories.NewSQLiteAlertRuleRepository(db)
	alertRepo := repositories.NewSQLiteTriggeredAlertRepository(db)

	// Create sensor client with circuit breaker
	sensorClient := clients.NewSensorClient(
		cfg.SensorServiceURL,
		cfg.APIToken,
		cfg.CBFailMax,
		cfg.CBResetTimeout,
	)

	// Start metrics server on :9090
	metrics.PipelineMode = cfg.PipelineMode
	metrics.WorkerCount = cfg.WorkerCount
	metrics.Serve(":9090")

	// Start RabbitMQ consumer in background
	evaluator := services.NewAlertEvaluator(ruleRepo, alertRepo)
	var consumer *messaging.AlertConsumer
	if cfg.PipelineMode == "async" {
		slog.Info("Pipeline mode: async", "worker_count", cfg.WorkerCount)
		consumer = messaging.NewAsyncAlertConsumer(cfg.RabbitMQURL, evaluator.Evaluate, cfg.WorkerCount)
	} else {
		slog.Info("Pipeline mode: blocking")
		consumer = messaging.NewAlertConsumer(cfg.RabbitMQURL, evaluator.Evaluate)
	}
	consumer.Start()

	// Create handlers
	healthHandler := handlers.NewHealthHandler()
	ruleHandler := handlers.NewAlertRuleHandler(ruleRepo, sensorClient)
	alertHandler := handlers.NewTriggeredAlertHandler(alertRepo)

	// Set up router
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()

	// Global middleware
	router.Use(gin.Recovery())
	router.Use(middleware.LoggingMiddleware())

	// Health endpoint — unauthenticated
	router.GET("/health", healthHandler.Health)

	// Protected routes — require Bearer token
	protected := router.Group("/")
	protected.Use(middleware.AuthMiddleware(cfg.APIToken))

	protected.GET("/rules", ruleHandler.ListRules)
	protected.GET("/rules/:id", ruleHandler.GetRule)
	protected.POST("/rules", ruleHandler.CreateRule)
	protected.PUT("/rules/:id", ruleHandler.UpdateRule)
	protected.DELETE("/rules/:id", ruleHandler.DeleteRule)

	protected.GET("/alerts", alertHandler.ListAlerts)
	protected.GET("/alerts/:id", alertHandler.GetAlert)
	protected.PUT("/alerts/:id", alertHandler.UpdateAlert)

	// Start server
	addr := fmt.Sprintf(":%d", cfg.Port)
	slog.Info("Starting Go IoT Alert Service", "port", cfg.Port)
	if err := router.Run(addr); err != nil {
		slog.Error("Failed to start server", "error", err)
		os.Exit(1)
	}
}
