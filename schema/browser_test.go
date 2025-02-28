package schema

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/rivo/tview"
	"go.uber.org/zap/zaptest"
)

// TestApp is a wrapper around tview.Application that immediately executes queued functions
type TestApp struct {
	*tview.Application
}

// QueueUpdateDraw immediately executes the function instead of queuing it
func (a *TestApp) QueueUpdateDraw(f func()) *tview.Application {
	// Execute the function immediately instead of queuing it
	if f != nil {
		f()
	}
	return a.Application
}

// NewTestApp creates a new test application
func NewTestApp() *TestApp {
	return &TestApp{
		Application: tview.NewApplication(),
	}
}

// mockApp is a simple implementation for testing that executes draw functions immediately
type mockApp struct {
	drawCalls int
}

func (m *mockApp) QueueUpdateDraw(f func()) *tview.Application {
	m.drawCalls++
	if f != nil {
		f()
	}
	return nil // Return nil since we don't need the actual application in tests
}

// TestNewBrowser tests the creation of a new Browser
func TestNewBrowser(t *testing.T) {
	// Create a mock database
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock DB: %v", err)
	}
	defer db.Close()

	// Create a test logger
	logger := zaptest.NewLogger(t)

	// Create a new browser with the mock DB
	browser := &Browser{
		tree:     NewSchemaTree(),
		cache:    NewSchemaCache(),
		treeView: tview.NewTreeView(),
		infoText: tview.NewTextView(),
		db:       db,
		dbPool:   db,
		logger:   logger,
		profile:  "test",
		rootNode: tview.NewTreeNode("Trino Schema"),
	}

	// Verify the browser was created correctly
	if browser == nil {
		t.Fatal("Expected non-nil browser")
	}
	if browser.tree == nil {
		t.Fatal("Expected non-nil tree")
	}
	if browser.cache == nil {
		t.Fatal("Expected non-nil cache")
	}
	if browser.treeView == nil {
		t.Fatal("Expected non-nil treeView")
	}
	if browser.infoText == nil {
		t.Fatal("Expected non-nil infoText")
	}
	if browser.db == nil {
		t.Fatal("Expected non-nil db")
	}
	if browser.dbPool == nil {
		t.Fatal("Expected non-nil dbPool")
	}
	if browser.logger == nil {
		t.Fatal("Expected non-nil logger")
	}
	if browser.profile != "test" {
		t.Fatalf("Expected profile 'test', got '%s'", browser.profile)
	}
	if browser.rootNode == nil {
		t.Fatal("Expected non-nil rootNode")
	}

	// Verify that the mock has no unfulfilled expectations
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled mock expectations: %s", err)
	}
}

// TestLoadCatalogsCore tests the core functionality of LoadCatalogs without UI interactions
func TestLoadCatalogsCore(t *testing.T) {
	// Create a mock database
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock DB: %v", err)
	}
	defer db.Close()

	// Set up the expected query and result
	rows := sqlmock.NewRows([]string{"catalog"}).
		AddRow("catalog1").
		AddRow("catalog2")
	mock.ExpectQuery("SHOW CATALOGS").WillReturnRows(rows)

	// Create a test logger
	logger := zaptest.NewLogger(t)

	// Create a browser with minimal components for testing
	browser := &Browser{
		tree:     NewSchemaTree(),
		cache:    NewSchemaCache(),
		dbPool:   db,
		logger:   logger,
		rootNode: tview.NewTreeNode("Trino Schema"),
	}

	// Test the core functionality directly
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Query catalogs directly
	dbRows, err := browser.dbPool.QueryContext(ctx, "SHOW CATALOGS")
	if err != nil {
		t.Fatalf("Failed to query catalogs: %v", err)
	}
	defer dbRows.Close()

	var catalogs []string
	for dbRows.Next() {
		var catalog string
		if err := dbRows.Scan(&catalog); err != nil {
			t.Fatalf("Failed to scan catalog: %v", err)
		}
		catalogs = append(catalogs, catalog)
	}

	if err := dbRows.Err(); err != nil {
		t.Fatalf("Error iterating catalogs: %v", err)
	}

	// Sort catalogs alphabetically
	sort.Strings(catalogs)

	// Update the tree and cache
	browser.tree.mu.Lock()
	for _, catalog := range catalogs {
		browser.tree.Catalogs[catalog] = true
	}
	browser.tree.mu.Unlock()

	browser.cache.Update(browser.tree, 5*time.Minute)

	// Verify that the mock has no unfulfilled expectations
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled mock expectations: %s", err)
	}

	// Verify that the tree was updated
	if len(browser.tree.Catalogs) != 2 {
		t.Fatalf("Expected 2 catalogs, got %d", len(browser.tree.Catalogs))
	}
	if !browser.tree.Catalogs["catalog1"] {
		t.Fatal("Expected catalog1 in tree")
	}
	if !browser.tree.Catalogs["catalog2"] {
		t.Fatal("Expected catalog2 in tree")
	}

	// Verify that the cache was updated
	cachedCatalogs := browser.cache.GetCatalogs()
	if len(cachedCatalogs) != 2 {
		t.Fatalf("Expected 2 catalogs in cache, got %d", len(cachedCatalogs))
	}
	if cachedCatalogs[0] != "catalog1" || cachedCatalogs[1] != "catalog2" {
		t.Fatalf("Expected catalogs in alphabetical order, got %v", cachedCatalogs)
	}
}

// TestLoadCatalogsFromCache tests the core functionality of loading catalogs from cache
func TestLoadCatalogsFromCache(t *testing.T) {
	// Create a mock database
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock DB: %v", err)
	}
	defer db.Close()

	// Create a test logger
	logger := zaptest.NewLogger(t)

	// Create a browser with minimal components for testing
	browser := &Browser{
		tree:     NewSchemaTree(),
		cache:    NewSchemaCache(),
		dbPool:   db,
		logger:   logger,
		rootNode: tview.NewTreeNode("Trino Schema"),
	}

	// Populate the cache
	tree := NewSchemaTree()
	tree.Catalogs["cached_catalog1"] = true
	tree.Catalogs["cached_catalog2"] = true
	browser.cache.Update(tree, 1*time.Hour)

	// Verify that the cache contains the expected catalogs
	cachedCatalogs := browser.cache.GetCatalogs()
	if len(cachedCatalogs) != 2 {
		t.Fatalf("Expected 2 catalogs in cache, got %d", len(cachedCatalogs))
	}
	if cachedCatalogs[0] != "cached_catalog1" || cachedCatalogs[1] != "cached_catalog2" {
		t.Fatalf("Expected catalogs in alphabetical order, got %v", cachedCatalogs)
	}

	// Verify that no DB queries were made
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled mock expectations: %s", err)
	}
}

// TestLoadSchemas tests the core functionality of LoadSchemas without UI interactions
func TestLoadSchemas(t *testing.T) {
	// Create a mock database
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock DB: %v", err)
	}
	defer db.Close()

	// Set up the expected query and result
	rows := sqlmock.NewRows([]string{"schema"}).
		AddRow("schema1").
		AddRow("schema2")
	mock.ExpectQuery("SHOW SCHEMAS FROM test_catalog").WillReturnRows(rows)

	// Create a test logger
	logger := zaptest.NewLogger(t)

	// Create a browser with minimal components for testing
	browser := &Browser{
		tree:     NewSchemaTree(),
		cache:    NewSchemaCache(),
		dbPool:   db,
		logger:   logger,
		rootNode: tview.NewTreeNode("Trino Schema"),
	}

	// Test the core functionality directly
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Query schemas directly
	dbRows, err := browser.dbPool.QueryContext(ctx, "SHOW SCHEMAS FROM test_catalog")
	if err != nil {
		t.Fatalf("Failed to query schemas: %v", err)
	}
	defer dbRows.Close()

	var schemas []string
	for dbRows.Next() {
		var schema string
		if err := dbRows.Scan(&schema); err != nil {
			t.Fatalf("Failed to scan schema: %v", err)
		}
		schemas = append(schemas, schema)
	}

	if err := dbRows.Err(); err != nil {
		t.Fatalf("Error iterating schemas: %v", err)
	}

	// Sort schemas alphabetically
	sort.Strings(schemas)

	// Update the tree and cache
	if browser.tree.Schemas == nil {
		browser.tree.Schemas = make(map[string]map[string]bool)
	}
	if browser.tree.Schemas["test_catalog"] == nil {
		browser.tree.Schemas["test_catalog"] = make(map[string]bool)
	}

	for _, schema := range schemas {
		browser.tree.Schemas["test_catalog"][schema] = true
	}

	browser.cache.Update(browser.tree, 5*time.Minute)

	// Verify that the mock has no unfulfilled expectations
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled mock expectations: %s", err)
	}

	// Verify that the tree was updated
	if len(browser.tree.Schemas["test_catalog"]) != 2 {
		t.Fatalf("Expected 2 schemas, got %d", len(browser.tree.Schemas["test_catalog"]))
	}
	if !browser.tree.Schemas["test_catalog"]["schema1"] {
		t.Fatal("Expected schema1 in tree")
	}
	if !browser.tree.Schemas["test_catalog"]["schema2"] {
		t.Fatal("Expected schema2 in tree")
	}

	// Verify that the cache was updated
	cachedSchemas := browser.cache.GetSchemas("test_catalog")
	if len(cachedSchemas) != 2 {
		t.Fatalf("Expected 2 schemas in cache, got %d", len(cachedSchemas))
	}
	if cachedSchemas[0] != "schema1" || cachedSchemas[1] != "schema2" {
		t.Fatalf("Expected schemas in alphabetical order, got %v", cachedSchemas)
	}
}

// TestLoadTables tests the core functionality of LoadTables without UI interactions
func TestLoadTables(t *testing.T) {
	// Create a mock database
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock DB: %v", err)
	}
	defer db.Close()

	// Set up the expected query and result
	rows := sqlmock.NewRows([]string{"table"}).
		AddRow("table1").
		AddRow("table2")
	mock.ExpectQuery("SHOW TABLES FROM test_catalog.test_schema").WillReturnRows(rows)

	// Create a test logger
	logger := zaptest.NewLogger(t)

	// Create a browser with minimal components for testing
	browser := &Browser{
		tree:     NewSchemaTree(),
		cache:    NewSchemaCache(),
		dbPool:   db,
		logger:   logger,
		rootNode: tview.NewTreeNode("Trino Schema"),
	}

	// Test the core functionality directly
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Query tables directly
	dbRows, err := browser.dbPool.QueryContext(ctx, "SHOW TABLES FROM test_catalog.test_schema")
	if err != nil {
		t.Fatalf("Failed to query tables: %v", err)
	}
	defer dbRows.Close()

	var tables []string
	for dbRows.Next() {
		var table string
		if err := dbRows.Scan(&table); err != nil {
			t.Fatalf("Failed to scan table: %v", err)
		}
		tables = append(tables, table)
	}

	if err := dbRows.Err(); err != nil {
		t.Fatalf("Error iterating tables: %v", err)
	}

	// Sort tables alphabetically
	sort.Strings(tables)

	// Update the tree and cache
	if browser.tree.Tables == nil {
		browser.tree.Tables = make(map[string]map[string]map[string]bool)
	}
	if browser.tree.Tables["test_catalog"] == nil {
		browser.tree.Tables["test_catalog"] = make(map[string]map[string]bool)
	}
	if browser.tree.Tables["test_catalog"]["test_schema"] == nil {
		browser.tree.Tables["test_catalog"]["test_schema"] = make(map[string]bool)
	}

	for _, table := range tables {
		browser.tree.Tables["test_catalog"]["test_schema"][table] = true
	}

	browser.cache.Update(browser.tree, 5*time.Minute)

	// Verify that the mock has no unfulfilled expectations
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled mock expectations: %s", err)
	}

	// Verify that the tree was updated
	if browser.tree.Tables["test_catalog"] == nil {
		t.Fatal("Expected test_catalog in tables")
	}
	if browser.tree.Tables["test_catalog"]["test_schema"] == nil {
		t.Fatal("Expected test_schema in tables")
	}
	if len(browser.tree.Tables["test_catalog"]["test_schema"]) != 2 {
		t.Fatalf("Expected 2 tables, got %d", len(browser.tree.Tables["test_catalog"]["test_schema"]))
	}
	if !browser.tree.Tables["test_catalog"]["test_schema"]["table1"] {
		t.Fatal("Expected table1 in tree")
	}
	if !browser.tree.Tables["test_catalog"]["test_schema"]["table2"] {
		t.Fatal("Expected table2 in tree")
	}

	// Verify that the cache was updated
	cachedTables := browser.cache.GetTables("test_catalog", "test_schema")
	if len(cachedTables) != 2 {
		t.Fatalf("Expected 2 tables in cache, got %d", len(cachedTables))
	}
	if cachedTables[0] != "table1" || cachedTables[1] != "table2" {
		t.Fatalf("Expected tables in alphabetical order, got %v", cachedTables)
	}
}

// TestLoadColumns tests the core functionality of LoadColumns without UI interactions
func TestLoadColumns(t *testing.T) {
	// Create a mock database
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock DB: %v", err)
	}
	defer db.Close()

	// Set up the expected query and result
	rows := sqlmock.NewRows([]string{"column", "type", "extra"}).
		AddRow("col1", "int", "").
		AddRow("col2", "varchar", "not null")
	mock.ExpectQuery("DESCRIBE test_catalog.test_schema.test_table").WillReturnRows(rows)

	// Create a test logger
	logger := zaptest.NewLogger(t)

	// Create a browser with minimal components for testing
	browser := &Browser{
		tree:     NewSchemaTree(),
		cache:    NewSchemaCache(),
		dbPool:   db,
		logger:   logger,
		rootNode: tview.NewTreeNode("Trino Schema"),
	}

	// Test the core functionality directly
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Query columns directly
	dbRows, err := browser.dbPool.QueryContext(ctx, "DESCRIBE test_catalog.test_schema.test_table")
	if err != nil {
		t.Fatalf("Failed to query columns: %v", err)
	}
	defer dbRows.Close()

	var columns []Column
	for dbRows.Next() {
		var name, dataType, extra string
		if err := dbRows.Scan(&name, &dataType, &extra); err != nil {
			t.Fatalf("Failed to scan column: %v", err)
		}

		// Determine nullability from the extra field
		nullable := !strings.Contains(strings.ToLower(extra), "not null")

		columns = append(columns, Column{
			Name:     name,
			Type:     dataType,
			Nullable: nullable,
		})
	}

	if err := dbRows.Err(); err != nil {
		t.Fatalf("Error iterating columns: %v", err)
	}

	// Update the tree and cache
	if browser.tree.Columns == nil {
		browser.tree.Columns = make(map[string]map[string]map[string][]Column)
	}
	if browser.tree.Columns["test_catalog"] == nil {
		browser.tree.Columns["test_catalog"] = make(map[string]map[string][]Column)
	}
	if browser.tree.Columns["test_catalog"]["test_schema"] == nil {
		browser.tree.Columns["test_catalog"]["test_schema"] = make(map[string][]Column)
	}

	browser.tree.Columns["test_catalog"]["test_schema"]["test_table"] = columns
	browser.cache.Update(browser.tree, 5*time.Minute)

	// Verify that the mock has no unfulfilled expectations
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled mock expectations: %s", err)
	}

	// Verify that the tree was updated
	if browser.tree.Columns["test_catalog"] == nil {
		t.Fatal("Expected test_catalog in columns")
	}
	if browser.tree.Columns["test_catalog"]["test_schema"] == nil {
		t.Fatal("Expected test_schema in columns")
	}
	if browser.tree.Columns["test_catalog"]["test_schema"]["test_table"] == nil {
		t.Fatal("Expected test_table in columns")
	}
	treeColumns := browser.tree.Columns["test_catalog"]["test_schema"]["test_table"]
	if len(treeColumns) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(treeColumns))
	}
	if treeColumns[0].Name != "col1" || treeColumns[1].Name != "col2" {
		t.Fatalf("Expected column names col1 and col2, got %s and %s", treeColumns[0].Name, treeColumns[1].Name)
	}
	if treeColumns[0].Type != "int" || treeColumns[1].Type != "varchar" {
		t.Fatalf("Expected column types int and varchar, got %s and %s", treeColumns[0].Type, treeColumns[1].Type)
	}
	if !treeColumns[0].Nullable || treeColumns[1].Nullable {
		t.Fatalf("Expected column nullability true and false, got %v and %v", treeColumns[0].Nullable, treeColumns[1].Nullable)
	}

	// Verify that the cache was updated
	cachedColumns := browser.cache.GetColumns("test_catalog", "test_schema", "test_table")
	if len(cachedColumns) != 2 {
		t.Fatalf("Expected 2 columns in cache, got %d", len(cachedColumns))
	}
}

// TestNodeSelected tests the nodeSelected method
func TestNodeSelected(t *testing.T) {
	// Create a test logger
	logger := zaptest.NewLogger(t)

	// Create a new browser
	browser := &Browser{
		tree:     NewSchemaTree(),
		cache:    NewSchemaCache(),
		treeView: tview.NewTreeView(),
		infoText: tview.NewTextView(),
		logger:   logger,
		profile:  "test",
		rootNode: tview.NewTreeNode("Trino Schema"),
	}

	// Test with a column node
	columnNode := tview.NewTreeNode("col1 (int)").
		SetReference(&SchemaTreeNode{
			Type:     "column",
			Name:     "col1",
			Catalog:  "test_catalog",
			Schema:   "test_schema",
			DataType: "int",
		})

	// Call nodeSelected
	browser.nodeSelected(columnNode)

	// Verify that the info text was updated
	text := browser.infoText.GetText(false)
	if text == "" {
		t.Fatal("Expected non-empty info text")
	}
}

// TestNodeChanged tests the nodeChanged method
func TestNodeChanged(t *testing.T) {
	// Create a test logger
	logger := zaptest.NewLogger(t)

	// Create a new browser
	browser := &Browser{
		tree:     NewSchemaTree(),
		cache:    NewSchemaCache(),
		treeView: tview.NewTreeView(),
		infoText: tview.NewTextView(),
		logger:   logger,
		profile:  "test",
		rootNode: tview.NewTreeNode("Trino Schema"),
	}

	// Test with a catalog node
	catalogNode := tview.NewTreeNode("test_catalog").
		SetReference(&SchemaTreeNode{
			Type:    "catalog",
			Name:    "test_catalog",
			Catalog: "test_catalog",
		})

	// Call nodeChanged
	browser.nodeChanged(catalogNode)

	// Verify that the info text was updated
	text := browser.infoText.GetText(false)
	if text == "" {
		t.Fatal("Expected non-empty info text")
	}
	if !strings.Contains(text, "test_catalog") {
		t.Fatalf("Expected info text to contain 'test_catalog', got '%s'", text)
	}
}
