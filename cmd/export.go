package cmd

import (
	"os"

	"github.com/TFMV/trino-cli/engine"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	exportFormat string
	outputFile   string
)

// exportCmd exports query results in various formats.
var exportCmd = &cobra.Command{
	Use:   "export [SQL]",
	Short: "Exports query results to a specified format",
	Long: `Executes the provided SQL query and exports the result in the specified format.
Supported formats: csv, json, arrow, parquet. You can specify an output file using --output.`,
	Args: cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		log := logger.With(zap.String("command", "export"))
		defer log.Sync()

		sql := args[0]
		log.Info("Executing export command",
			zap.String("query", sql),
			zap.String("format", exportFormat),
			zap.String("output", outputFile))

		// Execute the query
		result, err := engine.ExecuteQuery(sql, profile)
		if err != nil {
			log.Error("Error executing query", zap.Error(err))
			os.Stderr.WriteString("Error executing query: " + err.Error() + "\n")
			return
		}

		var stringOutput string
		var binaryOutput []byte
		isBinary := false

		switch exportFormat {
		case "csv":
			stringOutput, err = engine.ExportCSV(result)
		case "json":
			stringOutput, err = engine.ExportJSON(result)
		case "arrow":
			binaryOutput, err = engine.ExportArrow(result)
			isBinary = true
		case "parquet":
			binaryOutput, err = engine.ExportParquet(result)
			isBinary = true
		default:
			log.Error("Unsupported export format", zap.String("format", exportFormat))
			os.Stderr.WriteString("Unsupported export format: " + exportFormat + "\n")
			return
		}

		if err != nil {
			log.Error("Error exporting data", zap.Error(err))
			os.Stderr.WriteString("Error exporting data: " + err.Error() + "\n")
			return
		}

		// Write output to a file if specified, otherwise print to stdout
		if outputFile != "" {
			err = writeToFile(outputFile, stringOutput, binaryOutput, isBinary)
			if err != nil {
				log.Error("Error writing to file", zap.String("file", outputFile), zap.Error(err))
				os.Stderr.WriteString("Error writing to file: " + err.Error() + "\n")
			} else {
				log.Info("Export successful", zap.String("file", outputFile))
			}
		} else {
			// Write to stdout
			log.Info("Writing result to stdout")
			if isBinary {
				os.Stdout.Write(binaryOutput)
			} else {
				os.Stdout.WriteString(stringOutput)
			}
		}
	},
}

func init() {
	exportCmd.Flags().StringVar(&exportFormat, "format", "json", "Export format: csv, json, arrow, parquet")
	exportCmd.Flags().StringVar(&outputFile, "output", "", "Output file path (optional, defaults to stdout)")
}

// writeToFile writes data to a file, supporting both text and binary formats.
func writeToFile(filename string, textData string, binaryData []byte, isBinary bool) error {
	var err error
	var file *os.File

	// Open file with appropriate mode
	if isBinary {
		file, err = os.Create(filename)
	} else {
		file, err = os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	}

	if err != nil {
		return err
	}
	defer file.Close()

	if isBinary {
		_, err = file.Write(binaryData)
	} else {
		_, err = file.WriteString(textData)
	}

	return err
}
