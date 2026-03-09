package database

import (
	"database/sql"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// Schema for alert_rules and triggered_alerts tables.
const schema = `
CREATE TABLE IF NOT EXISTS alert_rules (
    id TEXT PRIMARY KEY,
    sensor_id TEXT NOT NULL,
    metric TEXT NOT NULL DEFAULT 'value',
    operator TEXT NOT NULL CHECK(operator IN ('gt', 'lt', 'gte', 'lte', 'eq')),
    threshold REAL NOT NULL,
    name TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('active', 'inactive')),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS triggered_alerts (
    id TEXT PRIMARY KEY,
    rule_id TEXT NOT NULL,
    sensor_id TEXT NOT NULL,
    sensor_value REAL NOT NULL,
    threshold REAL NOT NULL,
    message TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('open', 'acknowledged', 'resolved')),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at TEXT,
    FOREIGN KEY (rule_id) REFERENCES alert_rules(id)
);

CREATE INDEX IF NOT EXISTS idx_alert_rules_sensor_id ON alert_rules(sensor_id);
CREATE INDEX IF NOT EXISTS idx_alert_rules_status ON alert_rules(status);
CREATE INDEX IF NOT EXISTS idx_triggered_alerts_rule_id ON triggered_alerts(rule_id);
CREATE INDEX IF NOT EXISTS idx_triggered_alerts_status ON triggered_alerts(status);
`

// AlertRuleJSON represents an alert rule from the JSON seed file.
type AlertRuleJSON struct {
	ID        string  `json:"id"`
	SensorID  string  `json:"sensor_id"`
	Metric    string  `json:"metric"`
	Operator  string  `json:"operator"`
	Threshold float64 `json:"threshold"`
	Name      string  `json:"name"`
	Status    string  `json:"status"`
}

// Connect establishes a connection to the SQLite database.
func Connect(dbPath string) (*sql.DB, error) {
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

// InitSchema creates the database schema.
func InitSchema(db *sql.DB) error {
	_, err := db.Exec(schema)
	return err
}

// SeedFromJSON seeds the database from a JSON file if the table is empty.
func SeedFromJSON(db *sql.DB, jsonPath string) error {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM alert_rules").Scan(&count)
	if err != nil {
		return err
	}

	if count > 0 {
		slog.Info("Database already has data, skipping seed")
		return nil
	}

	data, err := os.ReadFile(jsonPath)
	if err != nil {
		if os.IsNotExist(err) {
			slog.Warn("Seed file not found", "path", jsonPath)
			return nil
		}
		return err
	}

	var rules []AlertRuleJSON
	if err := json.Unmarshal(data, &rules); err != nil {
		return err
	}

	now := time.Now().UTC().Format(time.RFC3339)

	stmt, err := db.Prepare(`
		INSERT INTO alert_rules (id, sensor_id, metric, operator, threshold, name, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, rule := range rules {
		_, err := stmt.Exec(
			rule.ID,
			rule.SensorID,
			rule.Metric,
			rule.Operator,
			rule.Threshold,
			rule.Name,
			rule.Status,
			now,
			now,
		)
		if err != nil {
			return err
		}
	}

	slog.Info("Seeded database from JSON", "count", len(rules))
	return nil
}
