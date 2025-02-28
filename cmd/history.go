package cmd

import (
	"github.com/TFMV/trino-cli/cache"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func init() {
	var err error
	logger, err = zap.NewProduction()
	if err != nil {
		// Panic if the logger cannot be initialized.
		panic("Failed to initialize logger: " + err.Error())
	}
}

// historyCmd displays the query history.
var historyCmd = &cobra.Command{
	Use:   "history",
	Short: "Displays query history",
	Run: func(cmd *cobra.Command, args []string) {
		history, err := cache.GetHistory()
		if err != nil {
			logger.Error("Error retrieving query history", zap.Error(err))
			return
		}
		logger.Info("Query History retrieved", zap.Int("total_queries", len(history)))
		for i, q := range history {
			logger.Info("Query", zap.Int("index", i+1), zap.String("query", q))
		}
	},
}
