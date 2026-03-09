package repositories

import (
	"database/sql"
	"fmt"

	"iot-sensor-service/models"
)

// SensorRepository defines the interface for sensor data access.
type SensorRepository interface {
	GetAll() ([]models.Sensor, error)
	GetByID(id string) (*models.Sensor, error)
	Create(sensor *models.SensorCreate) (*models.Sensor, error)
	Update(id string, updates *models.SensorUpdate) (*models.Sensor, error)
	Delete(id string) error
}

// SQLiteSensorRepository implements SensorRepository using SQLite.
type SQLiteSensorRepository struct {
	db *sql.DB
}

// NewSQLiteSensorRepository creates a new SQLite-backed sensor repository.
func NewSQLiteSensorRepository(db *sql.DB) *SQLiteSensorRepository {
	return &SQLiteSensorRepository{db: db}
}

// GetAll retrieves all sensors from the database.
func (r *SQLiteSensorRepository) GetAll() ([]models.Sensor, error) {
	rows, err := r.db.Query(`
		SELECT id, name, type, location, value, unit, status, last_reading, created_at, updated_at
		FROM sensors ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sensors []models.Sensor
	for rows.Next() {
		var s models.Sensor
		err := rows.Scan(&s.ID, &s.Name, &s.Type, &s.Location, &s.Value, &s.Unit, &s.Status, &s.LastReading, &s.CreatedAt, &s.UpdatedAt)
		if err != nil {
			return nil, err
		}
		sensors = append(sensors, s)
	}

	if sensors == nil {
		sensors = []models.Sensor{}
	}

	return sensors, rows.Err()
}

// GetByID retrieves a sensor by its ID.
func (r *SQLiteSensorRepository) GetByID(id string) (*models.Sensor, error) {
	var s models.Sensor
	err := r.db.QueryRow(`
		SELECT id, name, type, location, value, unit, status, last_reading, created_at, updated_at
		FROM sensors WHERE id = ?
	`, id).Scan(&s.ID, &s.Name, &s.Type, &s.Location, &s.Value, &s.Unit, &s.Status, &s.LastReading, &s.CreatedAt, &s.UpdatedAt)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &s, nil
}

// Create inserts a new sensor into the database.
func (r *SQLiteSensorRepository) Create(sensor *models.SensorCreate) (*models.Sensor, error) {
	// Validate input
	if err := sensor.Validate(); err != nil {
		return nil, err
	}

	// Generate new ID and insert atomically to prevent duplicate IDs under concurrency.
	tx, err := r.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var maxNum sql.NullInt64
	err = tx.QueryRow("SELECT MAX(CAST(SUBSTR(id, 8) AS INTEGER)) FROM sensors WHERE id LIKE 'sensor-%'").Scan(&maxNum)
	if err != nil {
		return nil, err
	}

	nextNum := int64(1)
	if maxNum.Valid {
		nextNum = maxNum.Int64 + 1
	}
	newID := fmt.Sprintf("sensor-%03d", nextNum)

	now := models.Now()

	_, err = tx.Exec(`
		INSERT INTO sensors (id, name, type, location, value, unit, status, last_reading, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, newID, sensor.Name, sensor.Type, sensor.Location, sensor.Value, sensor.Unit, sensor.Status, now, now, now)

	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return r.GetByID(newID)
}

// Update modifies an existing sensor.
func (r *SQLiteSensorRepository) Update(id string, updates *models.SensorUpdate) (*models.Sensor, error) {
	// Check if sensor exists
	existing, err := r.GetByID(id)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, nil
	}

	// Validate input
	if err := updates.Validate(); err != nil {
		return nil, err
	}

	// Build update query
	query := "UPDATE sensors SET updated_at = ?, last_reading = ?"
	args := []interface{}{models.Now(), models.Now()}

	if updates.Name != nil {
		query += ", name = ?"
		args = append(args, *updates.Name)
	}
	if updates.Type != nil {
		query += ", type = ?"
		args = append(args, *updates.Type)
	}
	if updates.Location != nil {
		query += ", location = ?"
		args = append(args, *updates.Location)
	}
	if updates.Value != nil {
		query += ", value = ?"
		args = append(args, *updates.Value)
	}
	if updates.Unit != nil {
		query += ", unit = ?"
		args = append(args, *updates.Unit)
	}
	if updates.Status != nil {
		query += ", status = ?"
		args = append(args, *updates.Status)
	}

	query += " WHERE id = ?"
	args = append(args, id)

	_, err = r.db.Exec(query, args...)
	if err != nil {
		return nil, err
	}

	return r.GetByID(id)
}

// Delete removes a sensor from the database.
func (r *SQLiteSensorRepository) Delete(id string) error {
	result, err := r.db.Exec("DELETE FROM sensors WHERE id = ?", id)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	return nil
}
