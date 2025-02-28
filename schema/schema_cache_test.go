package schema

import (
	"testing"
	"time"
)

func TestNewSchemaCache(t *testing.T) {
	cache := NewSchemaCache()
	if cache == nil {
		t.Fatal("Expected non-nil cache")
	}
	if cache.Data == nil {
		t.Fatal("Expected non-nil cache.Data")
	}
	if !cache.Expiry.Before(time.Now().Add(time.Second)) {
		t.Fatal("Expected cache to be expired initially")
	}
}

func TestSchemaCacheGet(t *testing.T) {
	cache := NewSchemaCache()

	// Initially, cache should be expired and Get should return nil
	if cache.Get() != nil {
		t.Fatal("Expected nil from Get() on expired cache")
	}

	// Update cache with a future expiry
	tree := NewSchemaTree()
	tree.Catalogs["test_catalog"] = true
	cache.Update(tree, 1*time.Hour)

	// Now Get should return the tree
	result := cache.Get()
	if result == nil {
		t.Fatal("Expected non-nil result from Get() after update")
	}
	if _, ok := result.Catalogs["test_catalog"]; !ok {
		t.Fatal("Expected test_catalog in result")
	}
}

func TestSchemaCacheUpdate(t *testing.T) {
	cache := NewSchemaCache()

	// Create a test tree
	tree := NewSchemaTree()
	tree.Catalogs["test_catalog"] = true

	// Update with a short duration
	cache.Update(tree, 50*time.Millisecond)

	// Verify cache is not expired
	if cache.Get() == nil {
		t.Fatal("Expected non-nil result from Get() immediately after update")
	}

	// Wait for expiry
	time.Sleep(100 * time.Millisecond)

	// Verify cache is now expired
	if cache.Get() != nil {
		t.Fatal("Expected nil result from Get() after expiry")
	}
}

func TestSchemaCacheHasCatalog(t *testing.T) {
	cache := NewSchemaCache()

	// Initially, HasCatalog should return false
	if cache.HasCatalog("test_catalog") {
		t.Fatal("Expected HasCatalog to return false on empty cache")
	}

	// Add a catalog
	tree := NewSchemaTree()
	tree.Catalogs["test_catalog"] = true
	cache.Update(tree, 1*time.Hour)

	// Now HasCatalog should return true for the added catalog
	if !cache.HasCatalog("test_catalog") {
		t.Fatal("Expected HasCatalog to return true for existing catalog")
	}

	// But false for a non-existent catalog
	if cache.HasCatalog("nonexistent_catalog") {
		t.Fatal("Expected HasCatalog to return false for non-existent catalog")
	}
}

func TestSchemaCacheHasSchema(t *testing.T) {
	cache := NewSchemaCache()

	// Initially, HasSchema should return false
	if cache.HasSchema("test_catalog", "test_schema") {
		t.Fatal("Expected HasSchema to return false on empty cache")
	}

	// Add a schema
	tree := NewSchemaTree()
	tree.Schemas["test_catalog"] = map[string]bool{"test_schema": true}
	cache.Update(tree, 1*time.Hour)

	// Now HasSchema should return true for the added schema
	if !cache.HasSchema("test_catalog", "test_schema") {
		t.Fatal("Expected HasSchema to return true for existing schema")
	}

	// But false for a non-existent schema
	if cache.HasSchema("test_catalog", "nonexistent_schema") {
		t.Fatal("Expected HasSchema to return false for non-existent schema")
	}

	// And false for a non-existent catalog
	if cache.HasSchema("nonexistent_catalog", "test_schema") {
		t.Fatal("Expected HasSchema to return false for non-existent catalog")
	}
}

func TestSchemaCacheHasTable(t *testing.T) {
	cache := NewSchemaCache()

	// Initially, HasTable should return false
	if cache.HasTable("test_catalog", "test_schema", "test_table") {
		t.Fatal("Expected HasTable to return false on empty cache")
	}

	// Add a table
	tree := NewSchemaTree()
	tree.Tables["test_catalog"] = map[string]map[string]bool{
		"test_schema": {"test_table": true},
	}
	cache.Update(tree, 1*time.Hour)

	// Now HasTable should return true for the added table
	if !cache.HasTable("test_catalog", "test_schema", "test_table") {
		t.Fatal("Expected HasTable to return true for existing table")
	}

	// But false for a non-existent table
	if cache.HasTable("test_catalog", "test_schema", "nonexistent_table") {
		t.Fatal("Expected HasTable to return false for non-existent table")
	}

	// And false for a non-existent schema
	if cache.HasTable("test_catalog", "nonexistent_schema", "test_table") {
		t.Fatal("Expected HasTable to return false for non-existent schema")
	}

	// And false for a non-existent catalog
	if cache.HasTable("nonexistent_catalog", "test_schema", "test_table") {
		t.Fatal("Expected HasTable to return false for non-existent catalog")
	}
}

func TestSchemaCacheGetCatalogs(t *testing.T) {
	cache := NewSchemaCache()

	// Initially, GetCatalogs should return nil
	if cache.GetCatalogs() != nil {
		t.Fatal("Expected GetCatalogs to return nil on empty cache")
	}

	// Add catalogs
	tree := NewSchemaTree()
	tree.Catalogs["catalog_b"] = true
	tree.Catalogs["catalog_a"] = true
	cache.Update(tree, 1*time.Hour)

	// Now GetCatalogs should return the catalogs in alphabetical order
	catalogs := cache.GetCatalogs()
	if len(catalogs) != 2 {
		t.Fatalf("Expected 2 catalogs, got %d", len(catalogs))
	}
	if catalogs[0] != "catalog_a" || catalogs[1] != "catalog_b" {
		t.Fatalf("Expected catalogs in alphabetical order, got %v", catalogs)
	}
}

func TestSchemaCacheGetSchemas(t *testing.T) {
	cache := NewSchemaCache()

	// Initially, GetSchemas should return nil
	if cache.GetSchemas("test_catalog") != nil {
		t.Fatal("Expected GetSchemas to return nil on empty cache")
	}

	// Add schemas
	tree := NewSchemaTree()
	tree.Schemas["test_catalog"] = map[string]bool{
		"schema_b": true,
		"schema_a": true,
	}
	cache.Update(tree, 1*time.Hour)

	// Now GetSchemas should return the schemas in alphabetical order
	schemas := cache.GetSchemas("test_catalog")
	if len(schemas) != 2 {
		t.Fatalf("Expected 2 schemas, got %d", len(schemas))
	}
	if schemas[0] != "schema_a" || schemas[1] != "schema_b" {
		t.Fatalf("Expected schemas in alphabetical order, got %v", schemas)
	}

	// But nil for a non-existent catalog
	if cache.GetSchemas("nonexistent_catalog") != nil {
		t.Fatal("Expected GetSchemas to return nil for non-existent catalog")
	}
}

func TestSchemaCacheGetTables(t *testing.T) {
	cache := NewSchemaCache()

	// Initially, GetTables should return nil
	if cache.GetTables("test_catalog", "test_schema") != nil {
		t.Fatal("Expected GetTables to return nil on empty cache")
	}

	// Add tables
	tree := NewSchemaTree()
	tree.Tables["test_catalog"] = map[string]map[string]bool{
		"test_schema": {
			"table_b": true,
			"table_a": true,
		},
	}
	cache.Update(tree, 1*time.Hour)

	// Now GetTables should return the tables in alphabetical order
	tables := cache.GetTables("test_catalog", "test_schema")
	if len(tables) != 2 {
		t.Fatalf("Expected 2 tables, got %d", len(tables))
	}
	if tables[0] != "table_a" || tables[1] != "table_b" {
		t.Fatalf("Expected tables in alphabetical order, got %v", tables)
	}

	// But nil for a non-existent schema
	if cache.GetTables("test_catalog", "nonexistent_schema") != nil {
		t.Fatal("Expected GetTables to return nil for non-existent schema")
	}

	// And nil for a non-existent catalog
	if cache.GetTables("nonexistent_catalog", "test_schema") != nil {
		t.Fatal("Expected GetTables to return nil for non-existent catalog")
	}
}

func TestSchemaCacheGetColumns(t *testing.T) {
	cache := NewSchemaCache()

	// Initially, GetColumns should return nil
	if cache.GetColumns("test_catalog", "test_schema", "test_table") != nil {
		t.Fatal("Expected GetColumns to return nil on empty cache")
	}

	// Add columns
	tree := NewSchemaTree()
	tree.Columns["test_catalog"] = map[string]map[string][]Column{
		"test_schema": {
			"test_table": {
				{Name: "col1", Type: "int", Nullable: true},
				{Name: "col2", Type: "varchar", Nullable: false},
			},
		},
	}
	cache.Update(tree, 1*time.Hour)

	// Now GetColumns should return the columns
	columns := cache.GetColumns("test_catalog", "test_schema", "test_table")
	if len(columns) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(columns))
	}
	if columns[0].Name != "col1" || columns[1].Name != "col2" {
		t.Fatalf("Expected column names col1 and col2, got %s and %s", columns[0].Name, columns[1].Name)
	}
	if columns[0].Type != "int" || columns[1].Type != "varchar" {
		t.Fatalf("Expected column types int and varchar, got %s and %s", columns[0].Type, columns[1].Type)
	}
	if !columns[0].Nullable || columns[1].Nullable {
		t.Fatalf("Expected column nullability true and false, got %v and %v", columns[0].Nullable, columns[1].Nullable)
	}

	// But nil for a non-existent table
	if cache.GetColumns("test_catalog", "test_schema", "nonexistent_table") != nil {
		t.Fatal("Expected GetColumns to return nil for non-existent table")
	}

	// And nil for a non-existent schema
	if cache.GetColumns("test_catalog", "nonexistent_schema", "test_table") != nil {
		t.Fatal("Expected GetColumns to return nil for non-existent schema")
	}

	// And nil for a non-existent catalog
	if cache.GetColumns("nonexistent_catalog", "test_schema", "test_table") != nil {
		t.Fatal("Expected GetColumns to return nil for non-existent catalog")
	}
}
