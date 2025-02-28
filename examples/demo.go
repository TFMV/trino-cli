package main

import (
	"fmt"
	"os"
	"time"

	"github.com/TFMV/trino-cli/config"
	"github.com/TFMV/trino-cli/engine"
	"github.com/TFMV/trino-cli/history"
	"github.com/TFMV/trino-cli/schema"
	"go.uber.org/zap"
)

// This demo shows how to use the Trino CLI programmatically
// It demonstrates various features of the CLI including:
// - Configuration handling
// - Query execution
// - Schema browsing
// - History management
// - Result export

func main() {
	// Initialize logger
	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	// Step 1: Load configuration
	fmt.Println("=== Step 1: Loading Configuration ===")
	if err := setupConfiguration(); err != nil {
		logger.Error("Failed to set up configuration", zap.Error(err))
		os.Exit(1)
	}

	// Step 2: Initialize history
	fmt.Println("\n=== Step 2: Initializing Query History ===")
	if err := history.Initialize(); err != nil {
		logger.Error("Failed to initialize history", zap.Error(err))
		os.Exit(1)
	}
	defer history.Close()

	// Step 3: Execute a simple query
	fmt.Println("\n=== Step 3: Executing a Simple Query ===")
	if err := executeSimpleQuery(); err != nil {
		logger.Error("Failed to execute query", zap.Error(err))
		os.Exit(1)
	}

	// Step 4: Work with query history
	fmt.Println("\n=== Step 4: Working with Query History ===")
	if err := workWithHistory(); err != nil {
		logger.Error("Failed to work with history", zap.Error(err))
		os.Exit(1)
	}

	// Step 5: Export query results in different formats
	fmt.Println("\n=== Step 5: Exporting Query Results ===")
	if err := exportQueryResults(); err != nil {
		logger.Error("Failed to export results", zap.Error(err))
		os.Exit(1)
	}

	// Step 6: Demonstrate schema browsing (non-UI version)
	fmt.Println("\n=== Step 6: Schema Browsing ===")
	if err := demonstrateSchemaAccess(); err != nil {
		logger.Error("Failed to demonstrate schema access", zap.Error(err))
		os.Exit(1)
	}

	fmt.Println("\n=== Demo Complete ===")
}

// setupConfiguration loads and displays the configuration
func setupConfiguration() error {
	// In a real application, this would be a path to your config file
	// For demo purposes, we'll create a sample config
	fmt.Println("Creating and loading sample configuration...")

	// Sample config data
	configData := `
profiles:
  default:
    host: localhost
    port: 8080
    user: user
    catalog: default
    schema: public
  
  prod:
    host: trino.production.example.com
    port: 443
    user: prod_user
    catalog: hive
    schema: analytics
    ssl: true

defaults:
  max_rows: 1000
  format: table
`

	// Write sample config to temporary file
	tmpFile, err := os.CreateTemp("", "trino-cli-config-*.yaml")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(configData); err != nil {
		return fmt.Errorf("failed to write config data: %w", err)
	}
	tmpFile.Close()

	// Load config
	if err := config.LoadConfig(tmpFile.Name()); err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Display loaded config
	fmt.Println("Configuration loaded successfully!")
	fmt.Println("Available profiles:")
	for name, profile := range config.AppConfig.Profiles {
		fmt.Printf("  - %s: %s:%d (catalog: %s, schema: %s)\n",
			name, profile.Host, profile.Port, profile.Catalog, profile.Schema)
	}

	return nil
}

// executeSimpleQuery demonstrates how to execute a query and process results
func executeSimpleQuery() error {
	fmt.Println("Executing a sample query...")

	// In a real application, this would connect to an actual Trino server
	// For demo purposes, we'll mock the result

	// Create a sample result (this would normally come from engine.ExecuteQuery)
	result := &engine.QueryResult{
		Columns: []string{"id", "name", "value"},
		Rows: [][]interface{}{
			{1, "Item 1", 10.5},
			{2, "Item 2", 20.75},
			{3, "Item 3", 30.0},
		},
	}

	// Display the result
	fmt.Println("Query result:")
	engine.DisplayResult(result)

	// Add to history (with profile "default")
	id, err := history.AddQuery(
		"SELECT id, name, value FROM items ORDER BY id",
		150*time.Millisecond,
		len(result.Rows),
		"default",
	)
	if err != nil {
		return fmt.Errorf("failed to add query to history: %w", err)
	}

	fmt.Printf("Query added to history with ID: %s\n", id)
	return nil
}

// workWithHistory demonstrates how to work with query history
func workWithHistory() error {
	// Get recent queries
	fmt.Println("Getting recent queries...")
	queries, err := history.GetQueries(10, 0)
	if err != nil {
		return fmt.Errorf("failed to get queries: %w", err)
	}

	fmt.Printf("Found %d queries in history:\n", len(queries))
	for i, q := range queries {
		fmt.Printf("  %d. [%s] %s (took %v, returned %d rows)\n",
			i+1, q.Timestamp.Format(time.RFC3339), q.Query, q.Duration, q.Rows)
	}

	// Search for queries
	fmt.Println("\nSearching for queries containing 'items'...")
	searchResults, err := history.SearchQueries("items", 5)
	if err != nil {
		return fmt.Errorf("failed to search queries: %w", err)
	}

	fmt.Printf("Found %d matching queries:\n", len(searchResults))
	for i, q := range searchResults {
		fmt.Printf("  %d. %s\n", i+1, q.Query)
	}

	return nil
}

// exportQueryResults demonstrates how to export query results in different formats
func exportQueryResults() error {
	// Sample query result
	result := &engine.QueryResult{
		Columns: []string{"id", "name", "value"},
		Rows: [][]interface{}{
			{1, "Item 1", 10.5},
			{2, "Item 2", 20.75},
			{3, "Item 3", 30.0},
		},
	}

	// Export to CSV
	csvData, err := engine.ExportCSV(result)
	if err != nil {
		return fmt.Errorf("failed to export to CSV: %w", err)
	}
	fmt.Println("CSV Export:")
	fmt.Println(csvData)

	// Export to JSON
	jsonData, err := engine.ExportJSON(result)
	if err != nil {
		return fmt.Errorf("failed to export to JSON: %w", err)
	}
	fmt.Println("JSON Export:")
	fmt.Println(jsonData)

	// Note: Arrow and Parquet exports are placeholder implementations in the engine
	fmt.Println("(Arrow and Parquet exports are also available in the real implementation)")

	return nil
}

// demonstrateSchemaAccess shows how to work with the schema browser programmatically
func demonstrateSchemaAccess() error {
	fmt.Println("Working with schema metadata...")

	// Create a schema cache
	cache := schema.NewSchemaCache()

	// Build a mock schema tree (in a real app, this would come from querying Trino)
	tree := schema.NewSchemaTree()

	// Add catalogs
	catalogs := []string{"hive", "mysql", "postgresql"}
	for _, catalog := range catalogs {
		tree.Catalogs[catalog] = true
	}

	// Add schemas
	tree.Schemas["hive"] = map[string]bool{
		"default":   true,
		"analytics": true,
	}
	tree.Schemas["mysql"] = map[string]bool{
		"public": true,
	}
	tree.Schemas["postgresql"] = map[string]bool{
		"public":  true,
		"reports": true,
	}

	// Add tables
	tree.Tables["hive"] = map[string]map[string]bool{
		"default": {
			"customers": true,
			"orders":    true,
		},
		"analytics": {
			"daily_metrics":  true,
			"monthly_report": true,
		},
	}

	// Add columns
	tree.Columns["hive"] = map[string]map[string][]schema.Column{
		"default": {
			"customers": {
				{Name: "id", Type: "bigint", Nullable: false},
				{Name: "name", Type: "varchar", Nullable: false},
				{Name: "email", Type: "varchar", Nullable: true},
			},
			"orders": {
				{Name: "id", Type: "bigint", Nullable: false},
				{Name: "customer_id", Type: "bigint", Nullable: false},
				{Name: "amount", Type: "double", Nullable: false},
				{Name: "created_at", Type: "timestamp", Nullable: false},
			},
		},
	}

	// Update the cache with this tree
	cache.Update(tree, 10*time.Minute)

	// Demonstrate accessing schema information
	fmt.Println("Catalogs:")
	for _, catalog := range cache.GetCatalogs() {
		fmt.Printf("  - %s\n", catalog)

		if cache.HasCatalog(catalog) {
			schemas := cache.GetSchemas(catalog)
			if len(schemas) > 0 {
				fmt.Printf("    Schemas in %s:\n", catalog)
				for _, schema := range schemas {
					fmt.Printf("      - %s\n", schema)

					if cache.HasSchema(catalog, schema) {
						tables := cache.GetTables(catalog, schema)
						if len(tables) > 0 {
							fmt.Printf("        Tables in %s.%s:\n", catalog, schema)
							for _, table := range tables {
								fmt.Printf("          - %s\n", table)

								if cache.HasTable(catalog, schema, table) {
									columns := cache.GetColumns(catalog, schema, table)
									if len(columns) > 0 {
										fmt.Printf("            Columns in %s.%s.%s:\n", catalog, schema, table)
										for _, col := range columns {
											nullability := "NOT NULL"
											if col.Nullable {
												nullability = "NULL"
											}
											fmt.Printf("              - %s (%s, %s)\n", col.Name, col.Type, nullability)
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}

	// Demonstrate fuzzy search
	fmt.Println("\nFuzzy Search for 'monthly':")
	for _, item := range schema.FuzzySearch("monthly", []string{
		"daily_metrics",
		"monthly_report",
		"customers",
		"orders",
	}) {
		fmt.Printf("  - %s\n", item)
	}

	return nil
}
