package history

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"go.uber.org/zap"
)

// QueryHistory represents a stored query with metadata
type QueryHistory struct {
	ID        string        `json:"id"`
	Timestamp time.Time     `json:"timestamp"`
	Query     string        `json:"query"`
	Duration  time.Duration `json:"duration"`
	Rows      int           `json:"rows"`
	Profile   string        `json:"profile"`
}

var (
	db     *sql.DB
	logger *zap.Logger
)

// Initialize sets up the history database
func Initialize() error {
	var err error
	logger, err = zap.NewProduction()
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}

	// Create history directory in user's home directory
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("failed to get home directory: %w", err)
	}

	historyDir := filepath.Join(homeDir, ".trino-cli", "history")
	if err := os.MkdirAll(historyDir, 0755); err != nil {
		return fmt.Errorf("failed to create history directory: %w", err)
	}

	dbPath := filepath.Join(historyDir, "history.db")
	db, err = sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open history database: %w", err)
	}

	// Create tables if they don't exist
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS query_history (
		id TEXT PRIMARY KEY,
		timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
		query TEXT NOT NULL,
		duration INTEGER DEFAULT 0,
		rows INTEGER DEFAULT 0,
		profile TEXT NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_query_history_timestamp ON query_history(timestamp);
	`

	if _, err := db.Exec(createTableSQL); err != nil {
		return fmt.Errorf("failed to create history table: %w", err)
	}

	logger.Info("History database initialized", zap.String("path", dbPath))
	return nil
}

// Close closes the database connection
func Close() error {
	if db != nil {
		return db.Close()
	}
	return nil
}

// AddQuery adds a query to the history database
func AddQuery(query string, duration time.Duration, rows int, profile string) (string, error) {
	if db == nil {
		return "", fmt.Errorf("history database not initialized")
	}

	// Generate a unique ID based on timestamp
	id := fmt.Sprintf("%d", time.Now().UnixNano())

	// Insert the query into the database
	stmt, err := db.Prepare(`
		INSERT INTO query_history (id, query, duration, rows, profile)
		VALUES (?, ?, ?, ?, ?)
	`)
	if err != nil {
		return "", fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(id, query, duration.Milliseconds(), rows, profile)
	if err != nil {
		return "", fmt.Errorf("failed to insert query: %w", err)
	}

	logger.Info("Query added to history", zap.String("id", id))
	return id, nil
}

// GetQueries retrieves query history entries
func GetQueries(limit int, offset int) ([]QueryHistory, error) {
	if db == nil {
		return nil, fmt.Errorf("history database not initialized")
	}

	rows, err := db.Query(`
		SELECT id, timestamp, query, duration, rows, profile
		FROM query_history
		ORDER BY timestamp DESC
		LIMIT ? OFFSET ?
	`, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to query history: %w", err)
	}
	defer rows.Close()

	var queries []QueryHistory
	for rows.Next() {
		var q QueryHistory
		var timestamp string
		var durationMs int64

		if err := rows.Scan(&q.ID, &timestamp, &q.Query, &durationMs, &q.Rows, &q.Profile); err != nil {
			return nil, fmt.Errorf("failed to scan query: %w", err)
		}

		// Parse timestamp
		t, err := time.Parse("2006-01-02 15:04:05", timestamp)
		if err != nil {
			logger.Warn("Failed to parse timestamp", zap.Error(err), zap.String("timestamp", timestamp))
			t = time.Now() // Fallback to current time
		}
		q.Timestamp = t
		q.Duration = time.Duration(durationMs) * time.Millisecond

		queries = append(queries, q)
	}

	return queries, nil
}

// SearchQueries searches query history with a search term
func SearchQueries(searchTerm string, limit int) ([]QueryHistory, error) {
	if db == nil {
		return nil, fmt.Errorf("history database not initialized")
	}

	// Use LIKE for simple search
	searchPattern := "%" + searchTerm + "%"
	rows, err := db.Query(`
		SELECT id, timestamp, query, duration, rows, profile
		FROM query_history
		WHERE query LIKE ?
		ORDER BY timestamp DESC
		LIMIT ?
	`, searchPattern, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to search history: %w", err)
	}
	defer rows.Close()

	var queries []QueryHistory
	for rows.Next() {
		var q QueryHistory
		var timestamp string
		var durationMs int64

		if err := rows.Scan(&q.ID, &timestamp, &q.Query, &durationMs, &q.Rows, &q.Profile); err != nil {
			return nil, fmt.Errorf("failed to scan query: %w", err)
		}

		// Parse timestamp
		t, err := time.Parse("2006-01-02 15:04:05", timestamp)
		if err != nil {
			logger.Warn("Failed to parse timestamp", zap.Error(err), zap.String("timestamp", timestamp))
			t = time.Now() // Fallback to current time
		}
		q.Timestamp = t
		q.Duration = time.Duration(durationMs) * time.Millisecond

		queries = append(queries, q)
	}

	return queries, nil
}

// GetQueryByID retrieves a specific query by ID
func GetQueryByID(id string) (*QueryHistory, error) {
	if db == nil {
		return nil, fmt.Errorf("history database not initialized")
	}

	var q QueryHistory
	var timestamp string
	var durationMs int64

	err := db.QueryRow(`
		SELECT id, timestamp, query, duration, rows, profile
		FROM query_history
		WHERE id = ?
	`, id).Scan(&q.ID, &timestamp, &q.Query, &durationMs, &q.Rows, &q.Profile)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("query not found: %s", id)
		}
		return nil, fmt.Errorf("failed to get query: %w", err)
	}

	// Parse timestamp
	t, err := time.Parse("2006-01-02 15:04:05", timestamp)
	if err != nil {
		logger.Warn("Failed to parse timestamp", zap.Error(err), zap.String("timestamp", timestamp))
		t = time.Now() // Fallback to current time
	}
	q.Timestamp = t
	q.Duration = time.Duration(durationMs) * time.Millisecond

	return &q, nil
}

// ClearHistory clears all or part of the query history
func ClearHistory(olderThan time.Time) (int64, error) {
	if db == nil {
		return 0, fmt.Errorf("history database not initialized")
	}

	var result sql.Result
	var err error

	if olderThan.IsZero() {
		// Clear all history
		result, err = db.Exec("DELETE FROM query_history")
	} else {
		// Clear history older than specified time
		timeStr := olderThan.Format("2006-01-02 15:04:05")
		result, err = db.Exec("DELETE FROM query_history WHERE timestamp < ?", timeStr)
	}

	if err != nil {
		return 0, fmt.Errorf("failed to clear history: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return rowsAffected, nil
}

// FuzzySearchQueries performs a fuzzy search on the query history
func FuzzySearchQueries(searchTerm string, limit int) ([]QueryHistory, error) {
	// Get all queries first (with a reasonable limit)
	queries, err := GetQueries(1000, 0)
	if err != nil {
		return nil, err
	}

	// Simple fuzzy matching - split search term into words and check if each word
	// is contained in the query (case insensitive)
	var results []QueryHistory
	searchWords := strings.Fields(strings.ToLower(searchTerm))

	for _, q := range queries {
		queryLower := strings.ToLower(q.Query)
		match := true

		for _, word := range searchWords {
			if !strings.Contains(queryLower, word) {
				match = false
				break
			}
		}

		if match {
			results = append(results, q)
		}

		// Limit the results
		if len(results) >= limit {
			break
		}
	}

	return results, nil
}
