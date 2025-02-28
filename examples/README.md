# Trino CLI Examples

This directory contains example code showing how to use the Trino CLI programmatically.

## Demo App

The `demo.go` file provides a comprehensive example of using Trino CLI's core functionality programmatically:

- Configuration handling
- Query execution
- Schema metadata browsing
- Query history management
- Result export in multiple formats

## Running the Demo

To run the demo, execute the following from the project root:

```bash
go run ./examples/demo.go
```

Note: The demo uses mock data and doesn't require a running Trino server. In a real application, you would connect to an actual Trino instance.

## Understanding the Code

The demo is organized into several sections:

1. **Configuration Handling**: Shows how to create, load, and access configuration profiles.
2. **Query History**: Demonstrates initializing and working with the persistent query history.
3. **Query Execution**: Shows how to execute SQL queries and process results.
4. **History Management**: Illustrates retrieving and searching historical queries.
5. **Result Export**: Shows exporting query results to CSV, JSON, and other formats.
6. **Schema Browsing**: Demonstrates working with Trino's metadata system to explore catalogs, schemas, tables, and columns.

## Using in Your Own Applications

To use Trino CLI functionality in your own Go applications:

1. Import the necessary packages:

   ```go
   import (
       "github.com/TFMV/trino-cli/config"
       "github.com/TFMV/trino-cli/engine"
       "github.com/TFMV/trino-cli/history"
       "github.com/TFMV/trino-cli/schema"
   )
   ```

2. Initialize the components you need:

   ```go
   // Load configuration
   config.LoadConfig("/path/to/config.yaml")
   
   // Initialize history
   history.Initialize()
   defer history.Close()
   ```

3. Execute queries:

   ```go
   result, err := engine.ExecuteQuery("SELECT * FROM catalog.schema.table", "profile_name")
   if err != nil {
       // Handle error
   }
   // Process result
   ```

4. Work with schema metadata:

   ```go
   cache := schema.NewSchemaCache()
   // Access schema information
   catalogs := cache.GetCatalogs()
   ```

## Additional Examples

For more examples specific to particular features, refer to:

- The unit tests in each package
- The implementation of the command-line interface in the `cmd` package
- The interactive UI implementation in the `ui` package
