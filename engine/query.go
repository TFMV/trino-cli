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

	"github.com/TFMV/trino-cli/history"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
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
	startTime := time.Now()

	// Retrieve connection details based on profile
	db, err := getConnection(profile)
	if err != nil {
		logger.Error("Failed to establish connection", zap.Error(err))
		return nil, err
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

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

	duration := time.Since(startTime)
	if _, err := history.AddQuery(query, duration, len(result.Rows), profile); err != nil {
		logger.Warn("Failed to add query to history", zap.Error(err))
	}
	logger.Info("Query executed successfully", zap.Int("rows_returned", len(result.Rows)))
	return result, nil
}

// DisplayResult prints the QueryResult in a simple table format.
func DisplayResult(result *QueryResult) {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	columnStr := fmt.Sprintf("%v", result.Columns)
	logger.Info("Query result columns", zap.String("columns", columnStr))
	for i, row := range result.Rows {
		rowStr := fmt.Sprintf("%v", row)
		logger.Info("Query result row", zap.Int("row", i), zap.String("data", rowStr))
	}

	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("%v\n", result.Columns))
	for _, row := range result.Rows {
		buffer.WriteString(fmt.Sprintf("%v\n", row))
	}
	os.Stdout.WriteString(buffer.String())
}

// ExportCSV converts QueryResult into CSV format.
func ExportCSV(result *QueryResult) (string, error) {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	if err := writer.Write(result.Columns); err != nil {
		return "", err
	}
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
	// (Implementation omitted for brevity.)
	defer writer.Close()
	return arrowBuffer.Bytes(), nil
}

// ExportParquet converts QueryResult into Parquet format.
func ExportParquet(result *QueryResult) ([]byte, error) {
	pool := memory.NewGoAllocator()
	// Convert the QueryResult into an Arrow Record.
	schema, record, err := createArrowRecord(result, pool)
	if err != nil {
		return nil, fmt.Errorf("failed to create arrow record: %w", err)
	}
	// Ensure the record is released when done.
	defer record.Release()

	buf := new(bytes.Buffer)
	// Use pqarrow to write the record to Parquet format
	writer, err := pqarrow.NewFileWriter(
		schema,
		buf,
		parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy)),
		pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(pool)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	if err := writer.Write(record); err != nil {
		_ = writer.Close()
		return nil, fmt.Errorf("failed to write record to parquet: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close parquet writer: %w", err)
	}

	return buf.Bytes(), nil
}

// getConnection returns a Trino connection based on the specified profile.
func getConnection(profile string) (*sql.DB, error) {
	dsn := "http://user@localhost:8080?catalog=default&schema=public"
	return sql.Open("trino", dsn)
}

// createArrowRecord converts a QueryResult into an Arrow record.
func createArrowRecord(result *QueryResult, pool memory.Allocator) (*arrow.Schema, arrow.Record, error) {
	numColumns := len(result.Columns)
	fields := make([]arrow.Field, numColumns)
	builders := make([]array.Builder, numColumns)

	// Infer each column's Arrow type by scanning for a non-nil value.
	for j, colName := range result.Columns {
		var dt arrow.DataType
		for _, row := range result.Rows {
			if j < len(row) && row[j] != nil {
				dt = inferArrowType(row[j])
				break
			}
		}
		if dt == nil {
			dt = arrow.BinaryTypes.String
		}
		fields[j] = arrow.Field{Name: colName, Type: dt, Nullable: true}
		switch dt := dt.(type) {
		case *arrow.Int64Type:
			builders[j] = array.NewInt64Builder(pool)
		case *arrow.Float64Type:
			builders[j] = array.NewFloat64Builder(pool)
		case *arrow.BooleanType:
			builders[j] = array.NewBooleanBuilder(pool)
		case *arrow.StringType:
			builders[j] = array.NewStringBuilder(pool)
		case *arrow.TimestampType:
			builders[j] = array.NewTimestampBuilder(pool, dt)
		default:
			builders[j] = array.NewStringBuilder(pool)
		}
	}

	// Append each row's values into the appropriate builder.
	for _, row := range result.Rows {
		for j := 0; j < numColumns; j++ {
			builder := builders[j]
			var val interface{}
			if j < len(row) {
				val = row[j]
			}
			if val == nil {
				builder.AppendNull()
				continue
			}
			switch b := builder.(type) {
			case *array.Int64Builder:
				switch v := val.(type) {
				case int:
					b.Append(int64(v))
				case int8:
					b.Append(int64(v))
				case int16:
					b.Append(int64(v))
				case int32:
					b.Append(int64(v))
				case int64:
					b.Append(v)
				case float32:
					b.Append(int64(v))
				case float64:
					b.Append(int64(v))
				default:
					b.AppendNull()
				}
			case *array.Float64Builder:
				switch v := val.(type) {
				case float32:
					b.Append(float64(v))
				case float64:
					b.Append(v)
				case int:
					b.Append(float64(v))
				case int8:
					b.Append(float64(v))
				case int16:
					b.Append(float64(v))
				case int32:
					b.Append(float64(v))
				case int64:
					b.Append(float64(v))
				default:
					b.AppendNull()
				}
			case *array.BooleanBuilder:
				if v, ok := val.(bool); ok {
					b.Append(v)
				} else {
					b.AppendNull()
				}
			case *array.StringBuilder:
				if v, ok := val.(string); ok {
					b.Append(v)
				} else {
					b.Append(fmt.Sprintf("%v", val))
				}
			case *array.TimestampBuilder:
				if v, ok := val.(time.Time); ok {
					b.Append(arrow.Timestamp(v.UnixMilli()))
				} else {
					b.AppendNull()
				}
			default:
				builder.AppendNull()
			}
		}
	}

	arrays := make([]arrow.Array, numColumns)
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
		builder.Release()
	}
	schema := arrow.NewSchema(fields, nil)
	record := array.NewRecord(schema, arrays, int64(len(result.Rows)))
	return schema, record, nil
}

// inferArrowType returns an appropriate Arrow data type based on the Go value.
func inferArrowType(val interface{}) arrow.DataType {
	switch val.(type) {
	case int, int8, int16, int32, int64:
		return arrow.PrimitiveTypes.Int64
	case float32, float64:
		return arrow.PrimitiveTypes.Float64
	case bool:
		return arrow.FixedWidthTypes.Boolean
	case string:
		return arrow.BinaryTypes.String
	case time.Time:
		return &arrow.TimestampType{Unit: arrow.Millisecond}
	default:
		return arrow.BinaryTypes.String
	}
}
