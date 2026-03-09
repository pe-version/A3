package repositories

import (
	"database/sql"
	"fmt"

	"iot-alert-service/models"
)

// AlertRuleRepository defines the interface for alert rule data access.
type AlertRuleRepository interface {
	GetAll() ([]models.AlertRule, error)
	GetByID(id string) (*models.AlertRule, error)
	GetActiveRulesForSensor(sensorID string) ([]models.AlertRule, error)
	Create(rule *models.AlertRuleCreate) (*models.AlertRule, error)
	Update(id string, updates *models.AlertRuleUpdate) (*models.AlertRule, error)
	Delete(id string) error
}

// SQLiteAlertRuleRepository implements AlertRuleRepository using SQLite.
type SQLiteAlertRuleRepository struct {
	db *sql.DB
}

// NewSQLiteAlertRuleRepository creates a new SQLite-backed alert rule repository.
func NewSQLiteAlertRuleRepository(db *sql.DB) *SQLiteAlertRuleRepository {
	return &SQLiteAlertRuleRepository{db: db}
}

// GetAll retrieves all alert rules from the database.
func (r *SQLiteAlertRuleRepository) GetAll() ([]models.AlertRule, error) {
	rows, err := r.db.Query(`
		SELECT id, sensor_id, metric, operator, threshold, name, status, created_at, updated_at
		FROM alert_rules ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rules []models.AlertRule
	for rows.Next() {
		var rule models.AlertRule
		err := rows.Scan(&rule.ID, &rule.SensorID, &rule.Metric, &rule.Operator,
			&rule.Threshold, &rule.Name, &rule.Status, &rule.CreatedAt, &rule.UpdatedAt)
		if err != nil {
			return nil, err
		}
		rules = append(rules, rule)
	}

	if rules == nil {
		rules = []models.AlertRule{}
	}

	return rules, rows.Err()
}

// GetByID retrieves an alert rule by its ID.
func (r *SQLiteAlertRuleRepository) GetByID(id string) (*models.AlertRule, error) {
	var rule models.AlertRule
	err := r.db.QueryRow(`
		SELECT id, sensor_id, metric, operator, threshold, name, status, created_at, updated_at
		FROM alert_rules WHERE id = ?
	`, id).Scan(&rule.ID, &rule.SensorID, &rule.Metric, &rule.Operator,
		&rule.Threshold, &rule.Name, &rule.Status, &rule.CreatedAt, &rule.UpdatedAt)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &rule, nil
}

// GetActiveRulesForSensor retrieves all active rules for a given sensor.
func (r *SQLiteAlertRuleRepository) GetActiveRulesForSensor(sensorID string) ([]models.AlertRule, error) {
	rows, err := r.db.Query(`
		SELECT id, sensor_id, metric, operator, threshold, name, status, created_at, updated_at
		FROM alert_rules WHERE sensor_id = ? AND status = 'active' ORDER BY id
	`, sensorID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rules []models.AlertRule
	for rows.Next() {
		var rule models.AlertRule
		err := rows.Scan(&rule.ID, &rule.SensorID, &rule.Metric, &rule.Operator,
			&rule.Threshold, &rule.Name, &rule.Status, &rule.CreatedAt, &rule.UpdatedAt)
		if err != nil {
			return nil, err
		}
		rules = append(rules, rule)
	}

	if rules == nil {
		rules = []models.AlertRule{}
	}

	return rules, rows.Err()
}

// Create inserts a new alert rule into the database.
// Uses a transaction to ensure atomic ID generation and insertion,
// preventing duplicate IDs under concurrent access.
func (r *SQLiteAlertRuleRepository) Create(rule *models.AlertRuleCreate) (*models.AlertRule, error) {
	if err := rule.Validate(); err != nil {
		return nil, err
	}

	tx, err := r.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var maxNum sql.NullInt64
	err = tx.QueryRow("SELECT MAX(CAST(SUBSTR(id, 6) AS INTEGER)) FROM alert_rules WHERE id LIKE 'rule-%'").Scan(&maxNum)
	if err != nil {
		return nil, err
	}

	nextNum := int64(1)
	if maxNum.Valid {
		nextNum = maxNum.Int64 + 1
	}
	newID := fmt.Sprintf("rule-%03d", nextNum)

	now := models.Now()

	_, err = tx.Exec(`
		INSERT INTO alert_rules (id, sensor_id, metric, operator, threshold, name, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, newID, rule.SensorID, rule.Metric, rule.Operator, rule.Threshold, rule.Name, rule.Status, now, now)

	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return r.GetByID(newID)
}

// Update modifies an existing alert rule.
func (r *SQLiteAlertRuleRepository) Update(id string, updates *models.AlertRuleUpdate) (*models.AlertRule, error) {
	existing, err := r.GetByID(id)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, nil
	}

	if err := updates.Validate(); err != nil {
		return nil, err
	}

	query := "UPDATE alert_rules SET updated_at = ?"
	args := []interface{}{models.Now()}

	if updates.SensorID != nil {
		query += ", sensor_id = ?"
		args = append(args, *updates.SensorID)
	}
	if updates.Metric != nil {
		query += ", metric = ?"
		args = append(args, *updates.Metric)
	}
	if updates.Operator != nil {
		query += ", operator = ?"
		args = append(args, *updates.Operator)
	}
	if updates.Threshold != nil {
		query += ", threshold = ?"
		args = append(args, *updates.Threshold)
	}
	if updates.Name != nil {
		query += ", name = ?"
		args = append(args, *updates.Name)
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

// Delete removes an alert rule from the database.
func (r *SQLiteAlertRuleRepository) Delete(id string) error {
	result, err := r.db.Exec("DELETE FROM alert_rules WHERE id = ?", id)
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
