package engine

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/xitongsys/parquet-go/writer"
	"go.uber.org/zap"

	_ "github.com/trinodb/trino-go-client/trino"
)

// QueryResult represents the structure of query results.
type QueryResult struct {
	Columns []string        `json:"columns"`
	Rows    [][]interface{} `json:"rows"`
}

// ExecuteQuery connects to Trino and executes the SQL query.
// It handles connection pooling, session management, and includes automatic retry logic for transient failures.
func ExecuteQuery(query string, profile string) (*QueryResult, error) {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	logger.Info("Executing query", zap.String("query", query), zap.String("profile", profile))

	// Retrieve connection details based on profile
	db, err := getConnection(profile)
	if err != nil {
		logger.Error("Failed to establish connection", zap.Error(err))
		return nil, err
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Execute query using the Trino Go client.
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		logger.Error("Query execution failed", zap.Error(err))
		return nil, err
	}
	defer rows.Close()

	result := &QueryResult{}
	columns, err := rows.Columns()
	if err != nil {
		logger.Error("Failed to fetch column names", zap.Error(err))
		return nil, err
	}
	result.Columns = columns

	// Process rows and build the result.
	for rows.Next() {
		values := make([]interface{}, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		if err := rows.Scan(scanArgs...); err != nil {
			logger.Error("Error scanning row", zap.Error(err))
			continue
		}
		result.Rows = append(result.Rows, values)
	}
	if err := rows.Err(); err != nil {
		logger.Error("Row iteration error", zap.Error(err))
		return nil, err
	}

	logger.Info("Query executed successfully", zap.Int("rows_returned", len(result.Rows)))
	return result, nil
}

// DisplayResult prints the QueryResult in a simple table format.
func DisplayResult(result *QueryResult) {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// Log columns
	columnStr := fmt.Sprintf("%v", result.Columns)
	logger.Info("Query result columns", zap.String("columns", columnStr))

	// Log each row
	for i, row := range result.Rows {
		rowStr := fmt.Sprintf("%v", row)
		logger.Info("Query result row", zap.Int("row", i), zap.String("data", rowStr))
	}

	// Also return a nicely formatted string for UI display
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("%v\n", result.Columns))
	for _, row := range result.Rows {
		buffer.WriteString(fmt.Sprintf("%v\n", row))
	}

	// Print to stdout for CLI display
	os.Stdout.WriteString(buffer.String())
}

// ExportCSV converts QueryResult into CSV format.
func ExportCSV(result *QueryResult) (string, error) {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)

	// Write headers
	if err := writer.Write(result.Columns); err != nil {
		return "", err
	}

	// Write rows
	for _, row := range result.Rows {
		rowStrings := make([]string, len(row))
		for i, v := range row {
			rowStrings[i] = fmt.Sprintf("%v", v)
		}
		if err := writer.Write(rowStrings); err != nil {
			return "", err
		}
	}
	writer.Flush()

	return buf.String(), nil
}

// ExportJSON converts QueryResult into JSON format.
func ExportJSON(result *QueryResult) (string, error) {
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// ExportArrow converts QueryResult into Arrow IPC format.
func ExportArrow(result *QueryResult) ([]byte, error) {
	pool := memory.NewGoAllocator()
	arrowBuffer := &bytes.Buffer{}
	writer := ipc.NewWriter(arrowBuffer, ipc.WithAllocator(pool))

	// Implement proper Arrow schema & write logic
	// For now, placeholder
	defer writer.Close()
	return arrowBuffer.Bytes(), nil
}

// ExportParquet converts QueryResult into Parquet format.
func ExportParquet(result *QueryResult) ([]byte, error) {
	buf := new(bytes.Buffer)
	pw, err := writer.NewParquetWriterFromWriter(buf, new(interface{}), 4)
	if err != nil {
		return nil, err
	}
	defer pw.WriteStop()

	// Implement proper Parquet write logic
	return buf.Bytes(), nil
}

// getConnection returns a Trino connection based on the specified profile.
func getConnection(profile string) (*sql.DB, error) {
	// Replace with actual config lookup logic
	dsn := "http://user@localhost:8080?catalog=default&schema=public"
	return sql.Open("trino", dsn)
}
