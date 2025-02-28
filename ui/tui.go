package ui

import (
	"fmt"
	"strings"
	"sync"

	"github.com/TFMV/trino-cli/engine"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"go.uber.org/zap"
)

// StartInteractive launches an interactive TUI-based query shell.
func StartInteractive(profile string) {
	// Initialize logger
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	log := logger.With(zap.String("component", "tui"), zap.String("profile", profile))

	log.Info("Starting interactive mode")

	app := tview.NewApplication()
	queryHistory := []string{}
	historyIndex := -1
	var historyLock sync.Mutex

	// Input field for SQL queries.
	input := tview.NewInputField().
		SetLabel("SQL> ").
		SetFieldWidth(0)

	// Output area to display query results and messages.
	output := tview.NewTextView().
		SetDynamicColors(true).
		SetScrollable(true).
		SetWrap(false).
		SetText("Welcome to Trino CLI. Enter your SQL query and press [green]Enter[white].")

	// Status bar to show execution state.
	statusBar := tview.NewTextView().
		SetDynamicColors(true).
		SetText("[blue]Ready[white]").
		SetTextAlign(tview.AlignLeft)

	// Layout container.
	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(output, 0, 1, false).
		AddItem(input, 3, 0, true).
		AddItem(statusBar, 1, 0, false)

	// Execute query when Enter is pressed.
	input.SetDoneFunc(func(key tcell.Key) {
		if key != tcell.KeyEnter {
			return
		}

		query := strings.TrimSpace(input.GetText())
		if query == "" {
			return
		}

		// Store query in history
		historyLock.Lock()
		queryHistory = append(queryHistory, query)
		historyIndex = len(queryHistory)
		historyLock.Unlock()

		// Update status bar
		statusBar.SetText("[yellow]Executing query...")
		log.Info("Executing query", zap.String("query", query))

		go func() {
			result, err := engine.ExecuteQuery(query, profile)
			app.QueueUpdateDraw(func() {
				if err != nil {
					log.Error("Query execution failed", zap.Error(err))
					output.SetText(fmt.Sprintf("[red]Error:[white] %v", err))
					statusBar.SetText("[red]Execution failed")
				} else {
					log.Info("Query executed successfully",
						zap.Int("rows", len(result.Rows)),
						zap.Int("columns", len(result.Columns)))
					output.SetText(formatResult(result))
					statusBar.SetText("[green]Execution complete")
				}
				input.SetText("")
			})
		}()
	})

	// Keyboard shortcuts.
	app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyUp: // Navigate history (previous query)
			historyLock.Lock()
			if historyIndex > 0 {
				historyIndex--
				input.SetText(queryHistory[historyIndex])
				log.Debug("History navigation", zap.String("direction", "up"), zap.Int("index", historyIndex))
			}
			historyLock.Unlock()
			return nil
		case tcell.KeyDown: // Navigate history (next query)
			historyLock.Lock()
			if historyIndex < len(queryHistory)-1 {
				historyIndex++
				input.SetText(queryHistory[historyIndex])
				log.Debug("History navigation", zap.String("direction", "down"), zap.Int("index", historyIndex))
			} else {
				input.SetText("")
			}
			historyLock.Unlock()
			return nil
		case tcell.KeyEscape: // Clear input
			input.SetText("")
			log.Debug("Input cleared")
			return nil
		case tcell.KeyCtrlC: // Exit application
			log.Info("User initiated application exit")
			app.Stop()
			return nil
		}
		return event
	})

	// Run the application.
	log.Info("TUI application starting")
	if err := app.SetRoot(flex, true).Run(); err != nil {
		log.Fatal("Application crashed", zap.Error(err))
	}
	log.Info("TUI application closed")
}

// formatResult renders query results in a table-like format.
func formatResult(result *engine.QueryResult) string {
	if len(result.Rows) == 0 {
		return "[yellow]No results found."
	}

	// Column headers
	output := "[green]" + strings.Join(result.Columns, " | ") + "[white]\n"
	output += strings.Repeat("-", len(output)) + "\n"

	// Rows
	for _, row := range result.Rows {
		rowStrings := make([]string, len(row))
		for i, val := range row {
			if val == nil {
				rowStrings[i] = "NULL"
			} else {
				rowStrings[i] = fmt.Sprintf("%v", val)
			}
		}
		output += strings.Join(rowStrings, " | ") + "\n"
	}
	return output
}
