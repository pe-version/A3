package repositories

import (
	"database/sql"
	"fmt"

	"iot-alert-service/models"
)

// TriggeredAlertRepository defines the interface for triggered alert data access.
type TriggeredAlertRepository interface {
	GetAll() ([]models.TriggeredAlert, error)
	GetByID(id string) (*models.TriggeredAlert, error)
	Create(ruleID, sensorID string, sensorValue, threshold float64, message string) (*models.TriggeredAlert, error)
	UpdateStatus(id string, status string) (*models.TriggeredAlert, error)
}

// SQLiteTriggeredAlertRepository implements TriggeredAlertRepository using SQLite.
type SQLiteTriggeredAlertRepository struct {
	db *sql.DB
}

// NewSQLiteTriggeredAlertRepository creates a new SQLite-backed triggered alert repository.
func NewSQLiteTriggeredAlertRepository(db *sql.DB) *SQLiteTriggeredAlertRepository {
	return &SQLiteTriggeredAlertRepository{db: db}
}

// GetAll retrieves all triggered alerts from the database.
func (r *SQLiteTriggeredAlertRepository) GetAll() ([]models.TriggeredAlert, error) {
	rows, err := r.db.Query(`
		SELECT id, rule_id, sensor_id, sensor_value, threshold, message, status, created_at, resolved_at
		FROM triggered_alerts ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var alerts []models.TriggeredAlert
	for rows.Next() {
		var alert models.TriggeredAlert
		err := rows.Scan(&alert.ID, &alert.RuleID, &alert.SensorID, &alert.SensorValue,
			&alert.Threshold, &alert.Message, &alert.Status, &alert.CreatedAt, &alert.ResolvedAt)
		if err != nil {
			return nil, err
		}
		alerts = append(alerts, alert)
	}

	if alerts == nil {
		alerts = []models.TriggeredAlert{}
	}

	return alerts, rows.Err()
}

// GetByID retrieves a triggered alert by its ID.
func (r *SQLiteTriggeredAlertRepository) GetByID(id string) (*models.TriggeredAlert, error) {
	var alert models.TriggeredAlert
	err := r.db.QueryRow(`
		SELECT id, rule_id, sensor_id, sensor_value, threshold, message, status, created_at, resolved_at
		FROM triggered_alerts WHERE id = ?
	`, id).Scan(&alert.ID, &alert.RuleID, &alert.SensorID, &alert.SensorValue,
		&alert.Threshold, &alert.Message, &alert.Status, &alert.CreatedAt, &alert.ResolvedAt)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &alert, nil
}

// Create inserts a new triggered alert into the database.
// Uses a transaction to ensure atomic ID generation and insertion.
func (r *SQLiteTriggeredAlertRepository) Create(ruleID, sensorID string, sensorValue, threshold float64, message string) (*models.TriggeredAlert, error) {
	tx, err := r.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var maxNum sql.NullInt64
	err = tx.QueryRow("SELECT MAX(CAST(SUBSTR(id, 7) AS INTEGER)) FROM triggered_alerts WHERE id LIKE 'alert-%'").Scan(&maxNum)
	if err != nil {
		return nil, err
	}

	nextNum := int64(1)
	if maxNum.Valid {
		nextNum = maxNum.Int64 + 1
	}
	newID := fmt.Sprintf("alert-%03d", nextNum)

	now := models.Now()

	_, err = tx.Exec(`
		INSERT INTO triggered_alerts (id, rule_id, sensor_id, sensor_value, threshold, message, status, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, newID, ruleID, sensorID, sensorValue, threshold, message, "open", now)

	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return r.GetByID(newID)
}

// UpdateStatus updates the status of a triggered alert.
func (r *SQLiteTriggeredAlertRepository) UpdateStatus(id string, status string) (*models.TriggeredAlert, error) {
	existing, err := r.GetByID(id)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, nil
	}

	var resolvedAt *string
	if status == "resolved" {
		now := models.Now()
		resolvedAt = &now
	}

	_, err = r.db.Exec(
		"UPDATE triggered_alerts SET status = ?, resolved_at = ? WHERE id = ?",
		status, resolvedAt, id,
	)
	if err != nil {
		return nil, err
	}

	return r.GetByID(id)
}
