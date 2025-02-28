package autocomplete

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"go.uber.org/zap"
)

// SchemaIntrospector fetches schema metadata from Trino in real-time
type SchemaIntrospector struct {
	db                *sql.DB
	cache             *SchemaCache
	logger            *zap.Logger
	refreshInterval   time.Duration
	lastRefresh       time.Time
	stopRefresh       chan struct{}
	backgroundRefresh bool
	mu                sync.Mutex
}

// NewSchemaIntrospector creates a new schema introspector
func NewSchemaIntrospector(db *sql.DB, cache *SchemaCache, logger *zap.Logger) *SchemaIntrospector {
	if logger == nil {
		var err error
		logger, err = zap.NewProduction()
		if err != nil {
			// Fallback to empty logger if we can't create one
			logger = zap.NewNop()
		}
	}

	return &SchemaIntrospector{
		db:              db,
		cache:           cache,
		logger:          logger,
		refreshInterval: 30 * time.Minute, // Default refresh every 30 minutes
		stopRefresh:     make(chan struct{}),
	}
}

// SetRefreshInterval sets how often the background refresh occurs
func (si *SchemaIntrospector) SetRefreshInterval(interval time.Duration) {
	si.mu.Lock()
	defer si.mu.Unlock()

	// Only restart if the interval has changed
	if si.refreshInterval != interval {
		si.refreshInterval = interval

		// Restart background refresh if it's running
		if si.backgroundRefresh {
			si.logger.Info("Restarting background refresh with new interval",
				zap.Duration("interval", interval))

			// Stop the current refresh goroutine
			si.stopRefresh <- struct{}{}

			// Create a new channel for the new goroutine
			si.stopRefresh = make(chan struct{})

			// Start a new refresh goroutine
			go si.runBackgroundRefresh()
		}
	}
}

// StartBackgroundRefresh begins a background goroutine that refreshes schema metadata
func (si *SchemaIntrospector) StartBackgroundRefresh() {
	si.mu.Lock()
	defer si.mu.Unlock()

	if si.backgroundRefresh {
		si.logger.Info("Background refresh already running")
		return
	}

	si.backgroundRefresh = true
	si.logger.Info("Starting background schema refresh",
		zap.Duration("interval", si.refreshInterval))

	go si.runBackgroundRefresh()
}

// runBackgroundRefresh is the goroutine that periodically refreshes schema metadata
func (si *SchemaIntrospector) runBackgroundRefresh() {
	ticker := time.NewTicker(si.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := si.RefreshAll(); err != nil {
				si.logger.Error("Background refresh failed", zap.Error(err))
			}
		case <-si.stopRefresh:
			si.logger.Info("Background refresh stopped")
			return
		}
	}
}

// StopBackgroundRefresh stops the background refresh goroutine
func (si *SchemaIntrospector) StopBackgroundRefresh() {
	si.mu.Lock()
	defer si.mu.Unlock()

	if !si.backgroundRefresh {
		return
	}

	si.backgroundRefresh = false
	si.stopRefresh <- struct{}{}
}

// RefreshAll refreshes all schema metadata
func (si *SchemaIntrospector) RefreshAll() error {
	si.mu.Lock()
	defer si.mu.Unlock()

	si.logger.Info("Starting full schema refresh")

	// Get all schemas
	schemas, err := si.GetSchemas()
	if err != nil {
		return err
	}

	for _, schemaName := range schemas {
		// Skip internal schemas
		if schemaName == "information_schema" || schemaName == "system" {
			continue
		}

		si.logger.Debug("Refreshing schema", zap.String("schema", schemaName))

		// Build SchemaMetadata object
		metadata := SchemaMetadata{
			Name:       schemaName,
			LastUpdate: time.Now(),
		}

		// Get tables for this schema
		tables, err := si.GetTables(schemaName)
		if err != nil {
			si.logger.Error("Failed to get tables",
				zap.String("schema", schemaName),
				zap.Error(err))
			continue
		}

		// For each table, get columns
		for _, tableName := range tables {
			tableMetadata := TableMetadata{
				Name:   tableName,
				Schema: schemaName,
			}

			columns, err := si.GetColumns(schemaName, tableName)
			if err != nil {
				si.logger.Error("Failed to get columns",
					zap.String("schema", schemaName),
					zap.String("table", tableName),
					zap.Error(err))
				continue
			}

			tableMetadata.Columns = columns
			metadata.Tables = append(metadata.Tables, tableMetadata)
		}

		// Store this schema in the cache
		if err := si.cache.StoreSchema(metadata); err != nil {
			si.logger.Error("Failed to store schema in cache",
				zap.String("schema", schemaName),
				zap.Error(err))
		}
	}

	si.lastRefresh = time.Now()
	si.logger.Info("Full schema refresh complete")
	return nil
}

// GetSchemas retrieves all schema names from Trino
func (si *SchemaIntrospector) GetSchemas() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := "SELECT schema_name FROM information_schema.schemata"
	rows, err := si.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		schemas = append(schemas, name)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return schemas, nil
}

// GetTables retrieves all table names for a specific schema
func (si *SchemaIntrospector) GetTables(schemaName string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := "SELECT table_name FROM information_schema.tables WHERE table_schema = ?"
	rows, err := si.db.QueryContext(ctx, query, schemaName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tables, nil
}

// GetColumns retrieves all column metadata for a specific table
func (si *SchemaIntrospector) GetColumns(schemaName, tableName string) ([]ColumnMetadata, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
		SELECT column_name, data_type 
		FROM information_schema.columns 
		WHERE table_schema = ? AND table_name = ?
		ORDER BY ordinal_position
	`
	rows, err := si.db.QueryContext(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []ColumnMetadata
	for rows.Next() {
		var col ColumnMetadata
		if err := rows.Scan(&col.Name, &col.DataType); err != nil {
			return nil, err
		}
		col.Table = tableName
		col.Schema = schemaName
		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return columns, nil
}

// RefreshSchema refreshes metadata for a specific schema
func (si *SchemaIntrospector) RefreshSchema(schemaName string) error {
	si.mu.Lock()
	defer si.mu.Unlock()

	si.logger.Info("Refreshing schema", zap.String("schema", schemaName))

	// Build SchemaMetadata object
	metadata := SchemaMetadata{
		Name:       schemaName,
		LastUpdate: time.Now(),
	}

	// Get tables for this schema
	tables, err := si.GetTables(schemaName)
	if err != nil {
		return err
	}

	// For each table, get columns
	for _, tableName := range tables {
		tableMetadata := TableMetadata{
			Name:   tableName,
			Schema: schemaName,
		}

		columns, err := si.GetColumns(schemaName, tableName)
		if err != nil {
			si.logger.Error("Failed to get columns",
				zap.String("table", tableName),
				zap.Error(err))
			continue
		}

		tableMetadata.Columns = columns
		metadata.Tables = append(metadata.Tables, tableMetadata)
	}

	// Store this schema in the cache
	return si.cache.StoreSchema(metadata)
}

// GetLastRefreshTime returns when the last introspection refresh occurred
func (si *SchemaIntrospector) GetLastRefreshTime() time.Time {
	si.mu.Lock()
	defer si.mu.Unlock()
	return si.lastRefresh
}
