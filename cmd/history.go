package cmd

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/TFMV/trino-cli/engine"
	"github.com/TFMV/trino-cli/history"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	historyLimit      int
	historyOffset     int
	historySearchTerm string
	historyFuzzy      bool
	historyDays       int
	historyCmd        *cobra.Command
)

func init() {
	var err error
	logger, err = zap.NewProduction()
	if err != nil {
		// Panic if the logger cannot be initialized.
		panic("Failed to initialize logger: " + err.Error())
	}

	// Initialize the history database
	if err := history.Initialize(); err != nil {
		logger.Error("Failed to initialize history database", zap.Error(err))
		// Continue anyway - history commands will fail gracefully
	}

	// Create the history command
	historyCmd = &cobra.Command{
		Use:   "history",
		Short: "Manage and view query history",
		Long:  `Manage and view the history of executed queries. List, search, and replay previous queries.`,
	}

	// List subcommand
	historyListCmd := &cobra.Command{
		Use:   "list",
		Short: "List query history",
		Run:   historyListCmdFunc,
	}
	historyListCmd.Flags().IntVarP(&historyLimit, "limit", "l", 20, "Maximum number of queries to show")
	historyListCmd.Flags().IntVarP(&historyOffset, "offset", "o", 0, "Number of queries to skip")

	// Search subcommand
	historySearchCmd := &cobra.Command{
		Use:   "search [search term]",
		Short: "Search query history",
		Args:  cobra.MinimumNArgs(1),
		Run:   historySearchCmdFunc,
	}
	historySearchCmd.Flags().IntVarP(&historyLimit, "limit", "l", 20, "Maximum number of queries to show")
	historySearchCmd.Flags().BoolVarP(&historyFuzzy, "fuzzy", "f", false, "Use fuzzy search")

	// Replay subcommand
	historyReplayCmd := &cobra.Command{
		Use:   "replay [query id]",
		Short: "Replay a query from history",
		Args:  cobra.ExactArgs(1),
		Run:   historyReplayCmdFunc,
	}

	// Clear subcommand
	historyClearCmd := &cobra.Command{
		Use:   "clear",
		Short: "Clear query history",
		Run:   historyClearCmdFunc,
	}
	historyClearCmd.Flags().IntVarP(&historyDays, "days", "d", 0, "Clear history older than N days (0 = all history)")

	// Add subcommands to history command
	historyCmd.AddCommand(historyListCmd)
	historyCmd.AddCommand(historySearchCmd)
	historyCmd.AddCommand(historyReplayCmd)
	historyCmd.AddCommand(historyClearCmd)

	// Add history command to root command
	rootCmd.AddCommand(historyCmd)
}

func historyListCmdFunc(cmd *cobra.Command, args []string) {
	queries, err := history.GetQueries(historyLimit, historyOffset)
	if err != nil {
		logger.Error("Error retrieving query history", zap.Error(err))
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return
	}

	displayQueryHistory(queries)
}

func historySearchCmdFunc(cmd *cobra.Command, args []string) {
	// Join all the args to form the search term
	searchTerm := strings.Join(args, " ")

	var queries []history.QueryHistory
	var err error

	if historyFuzzy {
		queries, err = history.FuzzySearchQueries(searchTerm, historyLimit)
	} else {
		queries, err = history.SearchQueries(searchTerm, historyLimit)
	}

	if err != nil {
		logger.Error("Error searching query history", zap.Error(err))
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return
	}

	if len(queries) == 0 {
		fmt.Println("No matching queries found.")
		return
	}

	displayQueryHistory(queries)
}

func historyReplayCmdFunc(cmd *cobra.Command, args []string) {
	id := args[0]

	// Get the query from history
	query, err := history.GetQueryByID(id)
	if err != nil {
		logger.Error("Error retrieving query", zap.Error(err), zap.String("id", id))
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return
	}

	fmt.Printf("Replaying query: %s\n", query.Query)

	// Execute the query
	result, err := engine.ExecuteQuery(query.Query, query.Profile)
	if err != nil {
		logger.Error("Error executing query", zap.Error(err))
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return
	}

	// Display the results
	displayQueryResult(result)
}

func historyClearCmdFunc(cmd *cobra.Command, args []string) {
	var olderThan time.Time

	if historyDays > 0 {
		olderThan = time.Now().AddDate(0, 0, -historyDays)
	}

	count, err := history.ClearHistory(olderThan)
	if err != nil {
		logger.Error("Error clearing history", zap.Error(err))
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return
	}

	if historyDays > 0 {
		fmt.Printf("Cleared %d queries older than %d days.\n", count, historyDays)
	} else {
		fmt.Printf("Cleared %d queries from history.\n", count)
	}
}

func displayQueryHistory(queries []history.QueryHistory) {
	if len(queries) == 0 {
		fmt.Println("No queries in history.")
		return
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"ID", "Timestamp", "Profile", "Duration", "Rows", "Query"})
	table.SetBorder(false)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetCenterSeparator("")
	table.SetColumnSeparator("")
	table.SetRowSeparator("")
	table.SetHeaderLine(false)
	table.SetAutoWrapText(true)

	for _, q := range queries {
		// Truncate query if it's too long
		queryStr := q.Query
		if len(queryStr) > 80 {
			queryStr = queryStr[:77] + "..."
		}

		// Format duration in a readable way
		duration := formatDuration(q.Duration)

		// Format timestamp
		timestamp := q.Timestamp.Format("Jan 02 15:04:05")

		table.Append([]string{
			q.ID,
			timestamp,
			q.Profile,
			duration,
			strconv.Itoa(q.Rows),
			queryStr,
		})
	}

	table.Render()
}

func formatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%d ms", d.Milliseconds())
	} else if d < time.Minute {
		return fmt.Sprintf("%.2f s", d.Seconds())
	} else {
		return fmt.Sprintf("%.1f m", d.Minutes())
	}
}

func displayQueryResult(result *engine.QueryResult) {
	if len(result.Rows) == 0 {
		fmt.Println("Query returned no results.")
		return
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(result.Columns)
	table.SetAutoFormatHeaders(false)
	table.SetBorder(false)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetCenterSeparator("")
	table.SetColumnSeparator("  ")
	table.SetRowSeparator("")
	table.SetHeaderLine(false)
	table.SetAutoWrapText(true)

	for _, row := range result.Rows {
		// Convert each cell to string representation
		rowStr := make([]string, len(row))
		for i, cell := range row {
			if cell == nil {
				rowStr[i] = "NULL"
			} else {
				rowStr[i] = fmt.Sprintf("%v", cell)
			}
		}
		table.Append(rowStr)
	}

	table.Render()
}
