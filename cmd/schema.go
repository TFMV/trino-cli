package cmd

import (
	"fmt"
	"os"

	"github.com/TFMV/trino-cli/schema"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// schemaCmd is the parent command for schema-related operations.
var schemaCmd = &cobra.Command{
	Use:   "schema",
	Short: "Schema management commands",
	Long:  "Explore and manage Trino schema metadata.",
}

// schemaBrowseCmd launches the interactive schema browser.
var schemaBrowseCmd = &cobra.Command{
	Use:   "browse",
	Short: "Browse schema interactively",
	Long:  "Launch an interactive TUI to browse catalogs, schemas, tables, and columns.",
	Run: func(cmd *cobra.Command, args []string) {
		log := logger.With(zap.String("command", "schema browse"))
		defer log.Sync()

		log.Info("Starting schema browser", zap.String("profile", profile))

		// Create a new schema browser
		browser, err := schema.NewBrowser(profile, log)
		if err != nil {
			log.Error("Failed to create schema browser", zap.Error(err))
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return
		}

		// Start the browser
		if err := browser.Start(); err != nil {
			log.Error("Schema browser error", zap.Error(err))
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return
		}

		log.Info("Schema browser closed")
	},
}

func init() {
	// Add subcommands to schema command
	schemaCmd.AddCommand(schemaBrowseCmd)

	// Add schema command to root command
	rootCmd.AddCommand(schemaCmd)
}
