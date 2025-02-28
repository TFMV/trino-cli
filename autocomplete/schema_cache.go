package autocomplete

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"go.uber.org/zap"
)

// SchemaMetadata represents a complete schema's metadata
type SchemaMetadata struct {
	Name       string          `json:"name"`
	Tables     []TableMetadata `json:"tables"`
	LastUpdate time.Time       `json:"last_update"`
}

// TableMetadata represents a table's metadata
type TableMetadata struct {
	Name    string           `json:"name"`
	Schema  string           `json:"schema"`
	Columns []ColumnMetadata `json:"columns"`
}

// ColumnMetadata represents a column's metadata
type ColumnMetadata struct {
	Name     string `json:"name"`
	DataType string `json:"data_type"`
	Table    string `json:"table"`
	Schema   string `json:"schema"`
}

// SchemaCache manages caching of Trino schema metadata
type SchemaCache struct {
	db          *sql.DB
	trie        *Trie
	cacheFile   string
	lock        sync.RWMutex
	logger      *zap.Logger
	lastRefresh time.Time
}

// NewSchemaCache creates a new schema cache
func NewSchemaCache(cacheDir string, logger *zap.Logger) (*SchemaCache, error) {
	if logger == nil {
		var err error
		logger, err = zap.NewProduction()
		if err != nil {
			return nil, err
		}
	}

	// Ensure cache directory exists
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		logger.Error("Failed to create cache directory", zap.Error(err))
		return nil, err
	}

	dbPath := filepath.Join(cacheDir, "schema_cache.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		logger.Error("Failed to open cache database", zap.Error(err))
		return nil, err
	}

	// Create tables if they don't exist
	if err := initCacheDB(db); err != nil {
		logger.Error("Failed to initialize cache database", zap.Error(err))
		db.Close()
		return nil, err
	}

	sc := &SchemaCache{
		db:        db,
		trie:      NewTrie(),
		cacheFile: filepath.Join(cacheDir, "schema_cache.json"),
		logger:    logger,
	}

	// Load existing trie data from cache
	if err := sc.loadTrieFromCache(); err != nil {
		logger.Warn("Failed to load trie from cache (continuing with empty trie)", zap.Error(err))
	}

	return sc, nil
}

// initCacheDB initializes the SQLite database schema
func initCacheDB(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS schemas (
			name TEXT PRIMARY KEY,
			last_update TIMESTAMP
		);
		
		CREATE TABLE IF NOT EXISTS tables (
			name TEXT,
			schema_name TEXT,
			PRIMARY KEY (name, schema_name),
			FOREIGN KEY (schema_name) REFERENCES schemas(name) ON DELETE CASCADE
		);
		
		CREATE TABLE IF NOT EXISTS columns (
			name TEXT,
			data_type TEXT,
			table_name TEXT,
			schema_name TEXT,
			PRIMARY KEY (name, table_name, schema_name),
			FOREIGN KEY (table_name, schema_name) REFERENCES tables(name, schema_name) ON DELETE CASCADE
		);
		
		CREATE TABLE IF NOT EXISTS sql_keywords (
			keyword TEXT PRIMARY KEY,
			score INTEGER
		);
	`)
	return err
}

// loadTrieFromCache loads the trie from the cache database
func (sc *SchemaCache) loadTrieFromCache() error {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	// Load SQL keywords
	rows, err := sc.db.Query("SELECT keyword, score FROM sql_keywords")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var keyword string
		var score int
		if err := rows.Scan(&keyword, &score); err != nil {
			return err
		}
		sc.trie.Insert(keyword, score)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	// Load schema names
	schemaRows, err := sc.db.Query("SELECT name FROM schemas")
	if err != nil {
		return err
	}
	defer schemaRows.Close()

	for schemaRows.Next() {
		var schemaName string
		if err := schemaRows.Scan(&schemaName); err != nil {
			return err
		}
		sc.trie.Insert(schemaName, 500) // Medium priority for schema names
	}

	if err := schemaRows.Err(); err != nil {
		return err
	}

	// Load table names
	tableRows, err := sc.db.Query("SELECT schema_name, name FROM tables")
	if err != nil {
		return err
	}
	defer tableRows.Close()

	for tableRows.Next() {
		var schemaName, tableName string
		if err := tableRows.Scan(&schemaName, &tableName); err != nil {
			return err
		}
		sc.trie.Insert(tableName, 400)                // Lower priority for table names
		sc.trie.Insert(schemaName+"."+tableName, 450) // Higher for fully qualified names
	}

	if err := tableRows.Err(); err != nil {
		return err
	}

	// Load column names
	columnRows, err := sc.db.Query("SELECT schema_name, table_name, name FROM columns")
	if err != nil {
		return err
	}
	defer columnRows.Close()

	for columnRows.Next() {
		var schemaName, tableName, columnName string
		if err := columnRows.Scan(&schemaName, &tableName, &columnName); err != nil {
			return err
		}
		sc.trie.Insert(columnName, 300) // Lower priority for column names
	}

	if err := columnRows.Err(); err != nil {
		return err
	}

	sc.lastRefresh = time.Now()
	return nil
}

// LoadCache initializes the trie with data from the cache database
func (sc *SchemaCache) LoadCache() error {
	return sc.loadTrieFromCache()
}

// StoreSchema stores a schema's metadata in the cache
func (sc *SchemaCache) StoreSchema(metadata SchemaMetadata) error {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	tx, err := sc.db.Begin()
	if err != nil {
		return err
	}

	// Upsert schema
	_, err = tx.Exec(
		"INSERT OR REPLACE INTO schemas (name, last_update) VALUES (?, ?)",
		metadata.Name, time.Now(),
	)
	if err != nil {
		tx.Rollback()
		return err
	}

	// Add schema name to trie
	sc.trie.Insert(metadata.Name, 100)

	// Process tables and columns
	for _, table := range metadata.Tables {
		// Upsert table
		_, err = tx.Exec(
			"INSERT OR REPLACE INTO tables (name, schema_name) VALUES (?, ?)",
			table.Name, metadata.Name,
		)
		if err != nil {
			tx.Rollback()
			return err
		}

		// Add table names to trie
		sc.trie.Insert(table.Name, 90)
		sc.trie.Insert(metadata.Name+"."+table.Name, 95)

		// Process columns
		for _, col := range table.Columns {
			// Upsert column
			_, err = tx.Exec(
				"INSERT OR REPLACE INTO columns (name, data_type, table_name, schema_name) VALUES (?, ?, ?, ?)",
				col.Name, col.DataType, table.Name, metadata.Name,
			)
			if err != nil {
				tx.Rollback()
				return err
			}

			// Add column names to trie
			sc.trie.Insert(col.Name, 80)
			sc.trie.Insert(table.Name+"."+col.Name, 85)
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	sc.lastRefresh = time.Now()
	sc.logger.Info("Stored schema in cache", zap.String("schema", metadata.Name))
	return nil
}

// GetSuggestions returns autocomplete suggestions for a given prefix
func (sc *SchemaCache) GetSuggestions(prefix string, limit int) []string {
	sc.lock.RLock()
	defer sc.lock.RUnlock()

	return sc.trie.GetSuggestions(prefix, limit)
}

// GetFuzzyMatches gets fuzzy-matched suggestions for a prefix
func (sc *SchemaCache) GetFuzzyMatches(prefix string, maxDistance int, limit int) []string {
	sc.lock.RLock()
	defer sc.lock.RUnlock()

	return sc.trie.GetFuzzyMatches(prefix, maxDistance, limit)
}

// GetSchemas returns all schema names from the cache
func (sc *SchemaCache) GetSchemas() ([]string, error) {
	sc.lock.RLock()
	defer sc.lock.RUnlock()

	rows, err := sc.db.Query("SELECT name FROM schemas")
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

	return schemas, nil
}

// GetTables returns all table names for a schema from the cache
func (sc *SchemaCache) GetTables(schemaName string) ([]string, error) {
	sc.lock.RLock()
	defer sc.lock.RUnlock()

	rows, err := sc.db.Query("SELECT name FROM tables WHERE schema_name = ?", schemaName)
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

	return tables, nil
}

// GetColumns returns all column names for a table from the cache
func (sc *SchemaCache) GetColumns(schemaName, tableName string) ([]ColumnMetadata, error) {
	sc.lock.RLock()
	defer sc.lock.RUnlock()

	rows, err := sc.db.Query(
		"SELECT name, data_type FROM columns WHERE schema_name = ? AND table_name = ?",
		schemaName, tableName,
	)
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

	return columns, nil
}

// GetAllColumns returns all column names from the cache
func (sc *SchemaCache) GetAllColumns() ([]string, error) {
	sc.lock.RLock()
	defer sc.lock.RUnlock()

	rows, err := sc.db.Query("SELECT DISTINCT name FROM columns")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		columns = append(columns, name)
	}

	return columns, nil
}

// GetAllTables returns all table names from the cache
func (sc *SchemaCache) GetAllTables() ([]string, error) {
	sc.lock.RLock()
	defer sc.lock.RUnlock()

	rows, err := sc.db.Query("SELECT DISTINCT name FROM tables")
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

	return tables, nil
}

// GetAllSchemaQualifiedTables returns all schema-qualified table names (schema.table) from the cache
func (sc *SchemaCache) GetAllSchemaQualifiedTables() ([]string, error) {
	sc.lock.RLock()
	defer sc.lock.RUnlock()

	rows, err := sc.db.Query("SELECT schema_name, name FROM tables")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var schemaName, tableName string
		if err := rows.Scan(&schemaName, &tableName); err != nil {
			return nil, err
		}
		tables = append(tables, schemaName+"."+tableName)
	}

	return tables, nil
}

// Close closes the schema cache and database connection
func (sc *SchemaCache) Close() error {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	// Export cache to JSON before closing
	if err := sc.exportToJSON(); err != nil {
		sc.logger.Warn("Failed to export cache to JSON", zap.Error(err))
	}

	return sc.db.Close()
}

// exportToJSON exports the cache to a JSON file for persistence
func (sc *SchemaCache) exportToJSON() error {
	// Get all schemas
	rows, err := sc.db.Query("SELECT name, last_update FROM schemas")
	if err != nil {
		return err
	}
	defer rows.Close()

	var schemas []SchemaMetadata
	for rows.Next() {
		var schema SchemaMetadata
		var lastUpdate time.Time
		if err := rows.Scan(&schema.Name, &lastUpdate); err != nil {
			return err
		}
		schema.LastUpdate = lastUpdate

		// Get tables for this schema
		tables, err := sc.GetTables(schema.Name)
		if err != nil {
			return err
		}

		// Get columns for each table
		for _, tableName := range tables {
			table := TableMetadata{
				Name:   tableName,
				Schema: schema.Name,
			}

			columns, err := sc.GetColumns(schema.Name, tableName)
			if err != nil {
				return err
			}
			table.Columns = columns
			schema.Tables = append(schema.Tables, table)
		}

		schemas = append(schemas, schema)
	}

	// Write to JSON file
	data, err := json.MarshalIndent(schemas, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(sc.cacheFile, data, 0644)
}

// Initialize the cache with SQL keywords
func (sc *SchemaCache) InitializeSQLKeywords() error {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	// Common SQL keywords and their scores
	keywords := map[string]int{
		"select": 1000, "from": 1000, "where": 1000, "group": 990, "by": 990,
		"having": 980, "order": 980, "limit": 980, "offset": 970, "join": 970,
		"left": 960, "right": 960, "inner": 960, "outer": 960, "full": 950,
		"on": 950, "as": 950, "with": 940, "union": 940, "all": 940,
		"insert": 930, "into": 930, "values": 930, "update": 920, "set": 920,
		"delete": 920, "create": 910, "table": 910, "view": 910, "index": 900,
		"drop": 900, "alter": 900, "add": 890, "column": 890, "constraint": 890,
		"primary": 880, "key": 880, "foreign": 880, "references": 870, "unique": 870,
		"not": 870, "null": 870, "default": 860, "check": 860, "between": 860,
		"like": 850, "in": 850, "exists": 850, "case": 840, "when": 840,
		"then": 840, "else": 840, "end": 840, "cast": 830, "count": 830,
		"sum": 830, "avg": 820, "min": 820, "max": 820, "distinct": 820,
		"and": 810, "or": 810, "is": 810, "true": 800, "false": 800,
	}

	tx, err := sc.db.Begin()
	if err != nil {
		return err
	}

	// Clear existing keywords
	_, err = tx.Exec("DELETE FROM sql_keywords")
	if err != nil {
		tx.Rollback()
		return err
	}

	// Insert keywords
	stmt, err := tx.Prepare("INSERT INTO sql_keywords (keyword, score) VALUES (?, ?)")
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	for keyword, score := range keywords {
		_, err = stmt.Exec(keyword, score)
		if err != nil {
			tx.Rollback()
			return err
		}

		// Add to trie as well
		sc.trie.Insert(keyword, score)
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	sc.logger.Info("Initialized SQL keywords in cache")
	return nil
}

// GetLastRefreshTime returns when the cache was last refreshed
func (sc *SchemaCache) GetLastRefreshTime() time.Time {
	sc.lock.RLock()
	defer sc.lock.RUnlock()
	return sc.lastRefresh
}

// BoostWord increases the score of a word in the trie
func (sc *SchemaCache) BoostWord(word string, boostAmount int) bool {
	if sc.trie == nil {
		return false
	}

	success := sc.trie.BoostWord(word, boostAmount)
	if success {
		// Optionally update the database with the new score
		// This is a simple implementation - in a production system,
		// you might want to batch these updates or update periodically
		sc.logger.Debug("Boosted word score in schema cache",
			zap.String("word", word),
			zap.Int("boost", boostAmount))
	}

	return success
}
