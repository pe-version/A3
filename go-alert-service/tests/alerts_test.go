package tests

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"iot-alert-service/clients"
	"iot-alert-service/database"
	"iot-alert-service/handlers"
	"iot-alert-service/middleware"
	"iot-alert-service/repositories"
)

const testToken = "test-secret-token"

// setupTestRouter creates a test router with a temporary in-memory database.
// The sensor client is nil — CreateRule tests that need sensor validation
// use a mock HTTP server instead (see setupTestRouterWithSensorServer).
func setupTestRouter(t *testing.T) (*gin.Engine, func()) {
	t.Helper()

	tmpFile, err := os.CreateTemp("", "test-alerts-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	dbPath := tmpFile.Name()

	db, err := database.Connect(dbPath)
	if err != nil {
		os.Remove(dbPath)
		t.Fatalf("Failed to connect to database: %v", err)
	}

	if err := database.InitSchema(db); err != nil {
		db.Close()
		os.Remove(dbPath)
		t.Fatalf("Failed to initialize schema: %v", err)
	}

	ruleRepo := repositories.NewSQLiteAlertRuleRepository(db)
	alertRepo := repositories.NewSQLiteTriggeredAlertRepository(db)

	// Sensor client pointing at a non-existent server to simulate unavailability
	sensorClient := clients.NewSensorClient("http://localhost:0", testToken, 1, 1)

	healthHandler := handlers.NewHealthHandler()
	ruleHandler := handlers.NewAlertRuleHandler(ruleRepo, sensorClient)
	alertHandler := handlers.NewTriggeredAlertHandler(alertRepo)

	gin.SetMode(gin.TestMode)
	router := gin.New()

	router.GET("/health", healthHandler.Health)

	protected := router.Group("/")
	protected.Use(middleware.AuthMiddleware(testToken))
	protected.GET("/rules", ruleHandler.ListRules)
	protected.GET("/rules/:id", ruleHandler.GetRule)
	protected.POST("/rules", ruleHandler.CreateRule)
	protected.PUT("/rules/:id", ruleHandler.UpdateRule)
	protected.DELETE("/rules/:id", ruleHandler.DeleteRule)
	protected.GET("/alerts", alertHandler.ListAlerts)
	protected.GET("/alerts/:id", alertHandler.GetAlert)
	protected.PUT("/alerts/:id", alertHandler.UpdateAlert)

	cleanup := func() {
		db.Close()
		os.Remove(dbPath)
	}

	return router, cleanup
}

// setupTestRouterWithSensorServer creates a router with a mock sensor HTTP server
// that responds to /sensors/:id lookups.
func setupTestRouterWithSensorServer(t *testing.T, sensorExists bool) (*gin.Engine, func()) {
	t.Helper()

	// Spin up a mock sensor service
	mockSensor := gin.New()
	mockSensor.GET("/sensors/:id", func(c *gin.Context) {
		if !sensorExists {
			c.JSON(http.StatusNotFound, gin.H{"detail": "not found"})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"id": c.Param("id"), "name": "Test Sensor",
			"type": "temperature", "unit": "fahrenheit",
		})
	})
	mockServer := httptest.NewServer(mockSensor)

	tmpFile, err := os.CreateTemp("", "test-alerts-*.db")
	if err != nil {
		mockServer.Close()
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	dbPath := tmpFile.Name()

	db, err := database.Connect(dbPath)
	if err != nil {
		mockServer.Close()
		os.Remove(dbPath)
		t.Fatalf("Failed to connect to database: %v", err)
	}

	if err := database.InitSchema(db); err != nil {
		db.Close()
		mockServer.Close()
		os.Remove(dbPath)
		t.Fatalf("Failed to initialize schema: %v", err)
	}

	ruleRepo := repositories.NewSQLiteAlertRuleRepository(db)
	alertRepo := repositories.NewSQLiteTriggeredAlertRepository(db)
	sensorClient := clients.NewSensorClient(mockServer.URL, testToken, 5, 30)

	healthHandler := handlers.NewHealthHandler()
	ruleHandler := handlers.NewAlertRuleHandler(ruleRepo, sensorClient)
	alertHandler := handlers.NewTriggeredAlertHandler(alertRepo)

	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/health", healthHandler.Health)
	protected := router.Group("/")
	protected.Use(middleware.AuthMiddleware(testToken))
	protected.GET("/rules", ruleHandler.ListRules)
	protected.GET("/rules/:id", ruleHandler.GetRule)
	protected.POST("/rules", ruleHandler.CreateRule)
	protected.PUT("/rules/:id", ruleHandler.UpdateRule)
	protected.DELETE("/rules/:id", ruleHandler.DeleteRule)
	protected.GET("/alerts", alertHandler.ListAlerts)
	protected.GET("/alerts/:id", alertHandler.GetAlert)
	protected.PUT("/alerts/:id", alertHandler.UpdateAlert)

	cleanup := func() {
		db.Close()
		os.Remove(dbPath)
		mockServer.Close()
	}

	return router, cleanup
}

// --- Auth tests ---

func TestUnauthorizedWithoutToken(t *testing.T) {
	router, cleanup := setupTestRouter(t)
	defer cleanup()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/rules", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401, got %d", w.Code)
	}
}

func TestUnauthorizedWithInvalidToken(t *testing.T) {
	router, cleanup := setupTestRouter(t)
	defer cleanup()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/rules", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401, got %d", w.Code)
	}
}

func TestUnauthorizedMalformedHeader(t *testing.T) {
	router, cleanup := setupTestRouter(t)
	defer cleanup()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/rules", nil)
	req.Header.Set("Authorization", "Token some-token")
	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401, got %d", w.Code)
	}
}

func TestUnauthorizedResponseBody(t *testing.T) {
	router, cleanup := setupTestRouter(t)
	defer cleanup()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/rules", nil)
	router.ServeHTTP(w, req)

	var response map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &response)

	if _, ok := response["detail"]; !ok {
		t.Error("Expected 'detail' field in 401 response body")
	}
}

// --- Health ---

func TestHealthEndpointNoAuthRequired(t *testing.T) {
	router, cleanup := setupTestRouter(t)
	defer cleanup()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/health", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", w.Code)
	}

	var response map[string]string
	json.Unmarshal(w.Body.Bytes(), &response)

	if response["status"] != "ok" {
		t.Errorf("Expected status 'ok', got '%s'", response["status"])
	}
	if response["service"] != "go-alert" {
		t.Errorf("Expected service 'go-alert', got '%s'", response["service"])
	}
}

// --- Alert Rules ---

func TestListRulesEmpty(t *testing.T) {
	router, cleanup := setupTestRouter(t)
	defer cleanup()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/rules", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", w.Code)
	}

	var response map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &response)

	rules := response["rules"].([]interface{})
	if len(rules) != 0 {
		t.Errorf("Expected empty rules list, got %d", len(rules))
	}
	if int(response["count"].(float64)) != 0 {
		t.Errorf("Expected count 0")
	}
}

func TestCreateRuleWithValidSensor(t *testing.T) {
	router, cleanup := setupTestRouterWithSensorServer(t, true)
	defer cleanup()

	rule := map[string]interface{}{
		"sensor_id": "sensor-001",
		"name":      "High Temp Alert",
		"operator":  "gt",
		"threshold": 80.0,
	}
	body, _ := json.Marshal(rule)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/rules", bytes.NewBuffer(body))
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected 201, got %d: %s", w.Code, w.Body.String())
	}

	var created map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &created)

	if created["id"] == nil {
		t.Error("Expected id in response")
	}
	if created["name"] != "High Temp Alert" {
		t.Errorf("Expected name 'High Temp Alert', got '%s'", created["name"])
	}
	if created["operator"] != "gt" {
		t.Errorf("Expected operator 'gt', got '%s'", created["operator"])
	}
}

func TestCreateRuleWithNonexistentSensor(t *testing.T) {
	router, cleanup := setupTestRouterWithSensorServer(t, false)
	defer cleanup()

	rule := map[string]interface{}{
		"sensor_id": "sensor-999",
		"name":      "Bad Rule",
		"operator":  "gt",
		"threshold": 50.0,
	}
	body, _ := json.Marshal(rule)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/rules", bytes.NewBuffer(body))
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateRuleSensorUnavailableAllowsWithWarning(t *testing.T) {
	// Use the router where sensor client points at localhost:0 (unreachable)
	router, cleanup := setupTestRouter(t)
	defer cleanup()

	rule := map[string]interface{}{
		"sensor_id": "sensor-001",
		"name":      "Fallback Rule",
		"operator":  "lt",
		"threshold": 10.0,
	}
	body, _ := json.Marshal(rule)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/rules", bytes.NewBuffer(body))
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected 201 (fallback), got %d: %s", w.Code, w.Body.String())
	}

	var created map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &created)

	if created["warning"] == nil || created["warning"] == "" {
		t.Error("Expected warning field when sensor service is unavailable")
	}
}

func TestCreateRuleInvalidOperator(t *testing.T) {
	router, cleanup := setupTestRouter(t)
	defer cleanup()

	rule := map[string]interface{}{
		"sensor_id": "sensor-001",
		"name":      "Bad Op",
		"operator":  "invalid",
		"threshold": 50.0,
	}
	body, _ := json.Marshal(rule)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/rules", bytes.NewBuffer(body))
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateRuleMissingRequired(t *testing.T) {
	router, cleanup := setupTestRouter(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{"name": "No SensorID"})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/rules", bytes.NewBuffer(body))
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d", w.Code)
	}
}

func TestGetNonexistentRule(t *testing.T) {
	router, cleanup := setupTestRouter(t)
	defer cleanup()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/rules/rule-999", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected 404, got %d", w.Code)
	}

	var response map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &response)
	if _, ok := response["detail"]; !ok {
		t.Error("Expected 'detail' field in 404 response")
	}
	if !strings.Contains(response["detail"].(string), "rule-999") {
		t.Errorf("Expected detail to reference rule-999, got '%s'", response["detail"])
	}
}

func TestCreateAndFetchRule(t *testing.T) {
	router, cleanup := setupTestRouterWithSensorServer(t, true)
	defer cleanup()

	rule := map[string]interface{}{
		"sensor_id": "sensor-001",
		"name":      "Fetch Me",
		"operator":  "gte",
		"threshold": 90.0,
	}
	body, _ := json.Marshal(rule)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/rules", bytes.NewBuffer(body))
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("Create failed: %d %s", w.Code, w.Body.String())
	}

	var created map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &created)
	ruleID := created["id"].(string)

	w = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/rules/"+ruleID, nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", w.Code)
	}

	var fetched map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &fetched)
	if fetched["id"] != ruleID {
		t.Errorf("Expected id '%s', got '%s'", ruleID, fetched["id"])
	}
}

func TestUpdateRule(t *testing.T) {
	router, cleanup := setupTestRouterWithSensorServer(t, true)
	defer cleanup()

	// Create
	rule := map[string]interface{}{
		"sensor_id": "sensor-001", "name": "Update Me",
		"operator": "gt", "threshold": 70.0,
	}
	body, _ := json.Marshal(rule)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/rules", bytes.NewBuffer(body))
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	var created map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &created)
	ruleID := created["id"].(string)

	// Update
	threshold := 99.0
	updateData := map[string]interface{}{"threshold": threshold, "status": "inactive"}
	body, _ = json.Marshal(updateData)

	w = httptest.NewRecorder()
	req, _ = http.NewRequest("PUT", "/rules/"+ruleID, bytes.NewBuffer(body))
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var updated map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &updated)

	if updated["threshold"].(float64) != 99.0 {
		t.Errorf("Expected threshold 99.0, got %v", updated["threshold"])
	}
	if updated["status"] != "inactive" {
		t.Errorf("Expected status 'inactive', got '%s'", updated["status"])
	}
	if updated["name"] != "Update Me" {
		t.Errorf("Expected name unchanged 'Update Me', got '%s'", updated["name"])
	}
}

func TestUpdateRuleInvalidStatus(t *testing.T) {
	router, cleanup := setupTestRouterWithSensorServer(t, true)
	defer cleanup()

	// Create
	rule := map[string]interface{}{
		"sensor_id": "sensor-001", "name": "Validate Status",
		"operator": "gt", "threshold": 50.0,
	}
	body, _ := json.Marshal(rule)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/rules", bytes.NewBuffer(body))
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	var created map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &created)
	ruleID := created["id"].(string)

	// Update with bad status
	body, _ = json.Marshal(map[string]interface{}{"status": "broken"})

	w = httptest.NewRecorder()
	req, _ = http.NewRequest("PUT", "/rules/"+ruleID, bytes.NewBuffer(body))
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d", w.Code)
	}
}

func TestDeleteRule(t *testing.T) {
	router, cleanup := setupTestRouterWithSensorServer(t, true)
	defer cleanup()

	// Create
	rule := map[string]interface{}{
		"sensor_id": "sensor-001", "name": "Delete Me",
		"operator": "eq", "threshold": 0.0,
	}
	body, _ := json.Marshal(rule)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/rules", bytes.NewBuffer(body))
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	var created map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &created)
	ruleID := created["id"].(string)

	// Delete
	w = httptest.NewRecorder()
	req, _ = http.NewRequest("DELETE", "/rules/"+ruleID, nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("Expected 204, got %d", w.Code)
	}

	// Verify gone
	w = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/rules/"+ruleID, nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected 404 after delete, got %d", w.Code)
	}
}

func TestDeleteNonexistentRule(t *testing.T) {
	router, cleanup := setupTestRouter(t)
	defer cleanup()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("DELETE", "/rules/rule-999", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected 404, got %d", w.Code)
	}
}

func TestUpdateNonexistentRule(t *testing.T) {
	router, cleanup := setupTestRouter(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{"threshold": 99.0})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PUT", "/rules/rule-999", bytes.NewBuffer(body))
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected 404, got %d", w.Code)
	}
}

func TestListRulesAfterCreate(t *testing.T) {
	router, cleanup := setupTestRouterWithSensorServer(t, true)
	defer cleanup()

	operators := []string{"gt", "lt", "gte"}
	for i, op := range operators {
		rule := map[string]interface{}{
			"sensor_id": "sensor-001", "name": "Rule",
			"operator": op, "threshold": float64(i * 10),
		}
		body, _ := json.Marshal(rule)
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("POST", "/rules", bytes.NewBuffer(body))
		req.Header.Set("Authorization", "Bearer "+testToken)
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, req)
	}

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/rules", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", w.Code)
	}

	var response map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &response)

	if int(response["count"].(float64)) != 3 {
		t.Errorf("Expected count 3, got %v", response["count"])
	}
}

// --- Triggered Alerts ---

func TestListAlertsEmpty(t *testing.T) {
	router, cleanup := setupTestRouter(t)
	defer cleanup()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/alerts", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", w.Code)
	}

	var response map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &response)

	alerts := response["alerts"].([]interface{})
	if len(alerts) != 0 {
		t.Errorf("Expected empty alerts, got %d", len(alerts))
	}
}

func TestGetNonexistentAlert(t *testing.T) {
	router, cleanup := setupTestRouter(t)
	defer cleanup()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/alerts/alert-999", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected 404, got %d", w.Code)
	}
}

func TestUpdateNonexistentAlert(t *testing.T) {
	router, cleanup := setupTestRouter(t)
	defer cleanup()

	status := "acknowledged"
	body, _ := json.Marshal(map[string]interface{}{"status": &status})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PUT", "/alerts/alert-999", bytes.NewBuffer(body))
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected 404, got %d", w.Code)
	}
}

func TestUpdateAlertInvalidStatus(t *testing.T) {
	router, cleanup := setupTestRouter(t)
	defer cleanup()

	// input.Validate() runs before any DB lookup, so invalid status returns
	// 400 regardless of whether the alert ID exists.
	body, _ := json.Marshal(map[string]interface{}{"status": "invalid_status"})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PUT", "/alerts/alert-any", bytes.NewBuffer(body))
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400 for invalid status, got %d: %s", w.Code, w.Body.String())
	}
}
