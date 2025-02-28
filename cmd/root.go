package cmd

import (
	"fmt"
	"os"

	"github.com/TFMV/trino-cli/config"
	"github.com/TFMV/trino-cli/engine"
	"github.com/TFMV/trino-cli/ui"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	cfgFile   string
	profile   string
	execQuery string
	logger    *zap.Logger
)

// rootCmd represents the base command when called without any subcommands.
var rootCmd = &cobra.Command{
	Use:   "trino-cli",
	Short: "Trino CLI tool for interactive querying and analysis",
	Long:  `A high-performance, feature-rich CLI tool for connecting to Trino, executing queries interactively or in batch mode, caching results, and exporting in multiple formats.`,
	// If -e flag is provided then run a single query in batch mode, otherwise launch the interactive TUI.
	Run: func(cmd *cobra.Command, args []string) {
		if execQuery != "" {
			result, err := engine.ExecuteQuery(execQuery, profile)
			if err != nil {
				logger.Error("Error executing query", zap.Error(err))
				os.Exit(1)
				return
			}
			// Display results in table format
			engine.DisplayResult(result)
			return
		}
		// Launch interactive TUI
		ui.StartInteractive(profile)
	},
}

// Execute initializes and executes the root command.
func Execute() {
	// Setup structured logging.
	var err error
	logger, err = zap.NewProduction()
	if err != nil {
		// Can't use logger here as it's not initialized yet
		os.Stderr.WriteString("Failed to initialize logger: " + err.Error() + "\n")
		os.Exit(1)
	}
	defer logger.Sync()

	// Load configuration.
	if err := initConfig(); err != nil {
		logger.Error("Failed to load configuration", zap.Error(err))
	}

	// Add subcommands.
	rootCmd.AddCommand(historyCmd)
	rootCmd.AddCommand(cacheCmd)
	rootCmd.AddCommand(exportCmd)

	if err := rootCmd.Execute(); err != nil {
		logger.Error("Command execution error", zap.Error(err))
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(func() {
		if err := initConfig(); err != nil {
			logger.Error("Failed to initialize config", zap.Error(err))
		}
	})
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.trino-cli.yaml)")
	rootCmd.PersistentFlags().StringVar(&profile, "profile", "default", "Trino profile to use")
	rootCmd.PersistentFlags().StringVarP(&execQuery, "execute", "e", "", "Execute a single query in batch mode")
}

func initConfig() error {
	// Use the provided config file or default to $HOME/.trino-cli.yaml.
	if cfgFile != "" {
		return config.LoadConfig(cfgFile)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("unable to find home directory: %v", err)
	}
	cfgFile = home + "/.trino-cli.yaml"
	return config.LoadConfig(cfgFile)
}
