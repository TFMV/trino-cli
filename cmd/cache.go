package cmd

import (
	"os"
	"strings"

	"github.com/TFMV/trino-cli/cache"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// cacheCmd is the parent command for cache-related operations.
var cacheCmd = &cobra.Command{
	Use:   "cache",
	Short: "Cache management commands",
	Long:  "Manage locally cached query results stored in Apache Arrow IPC format.",
}

// cacheListCmd lists all cached query results.
var cacheListCmd = &cobra.Command{
	Use:   "list",
	Short: "Lists cached query results",
	Run: func(cmd *cobra.Command, args []string) {
		log := logger.With(zap.String("command", "cache list"))

		entries, err := cache.ListCache()
		if err != nil {
			log.Error("Error listing cache", zap.Error(err))
			os.Stderr.WriteString("[red]Error listing cache:[white] " + err.Error() + "\n")
			return
		}

		if len(entries) == 0 {
			log.Info("No cached queries found")
			os.Stdout.WriteString("[yellow]No cached queries found.[white]\n")
			return
		}

		// Display results in a tabular format
		log.Info("Displaying cached query IDs", zap.Int("count", len(entries)))

		var output strings.Builder
		output.WriteString("[green]Cached Query IDs:[white]\n")
		output.WriteString(strings.Repeat("-", 40) + "\n")
		for _, entry := range entries {
			output.WriteString(entry + "\n")
		}
		output.WriteString(strings.Repeat("-", 40) + "\n")

		os.Stdout.WriteString(output.String())
	},
}

// cacheReplayCmd replays a cached query result by its query ID.
var cacheReplayCmd = &cobra.Command{
	Use:   "replay <query_id>",
	Short: "Replays cached query result",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		log := logger.With(zap.String("command", "cache replay"), zap.String("queryID", args[0]))

		queryID := args[0]
		pretty, _ := cmd.Flags().GetBool("pretty")
		log.Info("Attempting to replay cached query", zap.Bool("pretty", pretty))

		resultStr, err := cache.ReplayCache(queryID)
		if err != nil {
			log.Error("Error replaying cache", zap.Error(err))
			os.Stderr.WriteString("[red]Error replaying cache:[white] " + err.Error() + "\n")
			return
		}

		if resultStr == "" {
			log.Warn("Cached result is empty")
			os.Stdout.WriteString("[yellow]Cached result is empty.[white]\n")
			return
		}

		// Print cached result with optional pretty print
		log.Info("Displaying cached result")

		var output strings.Builder
		output.WriteString("[green]Cached Result:[white]\n")

		if pretty {
			// Apply some basic formatting for pretty output
			output.WriteString(formatCachedResult(resultStr) + "\n")
		} else {
			output.WriteString(resultStr + "\n")
		}

		os.Stdout.WriteString(output.String())
	},
}

// formatCachedResult applies basic formatting to the cached result string
func formatCachedResult(result string) string {
	// Simple implementation - in a real app, you might want to parse and format more elegantly
	lines := strings.Split(result, "\n")
	for i, line := range lines {
		if i == 0 {
			lines[i] = "[green]" + line + "[white]"
		}
	}
	return strings.Join(lines, "\n")
}

func init() {
	cacheReplayCmd.Flags().Bool("pretty", false, "Pretty-print cached results")
	cacheCmd.AddCommand(cacheListCmd)
	cacheCmd.AddCommand(cacheReplayCmd)
}
