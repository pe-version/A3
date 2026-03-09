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

// Schema for sensors table
const schema = `
CREATE TABLE IF NOT EXISTS sensors (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT NOT NULL CHECK(type IN ('temperature', 'motion', 'humidity', 'light', 'air_quality', 'co2', 'contact', 'pressure')),
    location TEXT NOT NULL,
    value REAL NOT NULL,
    unit TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('active', 'inactive', 'error')),
    last_reading TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sensors_type ON sensors(type);
CREATE INDEX IF NOT EXISTS idx_sensors_location ON sensors(location);
CREATE INDEX IF NOT EXISTS idx_sensors_status ON sensors(status);
`

// SensorJSON represents a sensor from the JSON seed file.
type SensorJSON struct {
	ID          string      `json:"id"`
	Name        string      `json:"name"`
	Type        string      `json:"type"`
	Location    string      `json:"location"`
	Value       interface{} `json:"value"`
	Unit        string      `json:"unit"`
	Status      string      `json:"status"`
	LastReading string      `json:"last_reading"`
}

// Connect establishes a connection to the SQLite database.
func Connect(dbPath string) (*sql.DB, error) {
	// Ensure directory exists
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	// Test connection
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
	// Check if table has data
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM sensors").Scan(&count)
	if err != nil {
		return err
	}

	if count > 0 {
		slog.Info("Database already has data, skipping seed")
		return nil
	}

	// Read JSON file
	data, err := os.ReadFile(jsonPath)
	if err != nil {
		if os.IsNotExist(err) {
			slog.Warn("Seed file not found", "path", jsonPath)
			return nil
		}
		return err
	}

	var sensors []SensorJSON
	if err := json.Unmarshal(data, &sensors); err != nil {
		return err
	}

	now := time.Now().UTC().Format(time.RFC3339)

	// Insert sensors
	stmt, err := db.Prepare(`
		INSERT INTO sensors (id, name, type, location, value, unit, status, last_reading, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, sensor := range sensors {
		// Convert value to float64
		var value float64
		switch v := sensor.Value.(type) {
		case float64:
			value = v
		case bool:
			if v {
				value = 1.0
			} else {
				value = 0.0
			}
		case int:
			value = float64(v)
		}

		_, err := stmt.Exec(
			sensor.ID,
			sensor.Name,
			sensor.Type,
			sensor.Location,
			value,
			sensor.Unit,
			sensor.Status,
			sensor.LastReading,
			now,
			now,
		)
		if err != nil {
			return err
		}
	}

	slog.Info("Seeded database from JSON", "count", len(sensors))
	return nil
}
