package ui

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/TFMV/trino-cli/autocomplete"
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

	// Start schema cache updater in the background
	if err := autocomplete.StartSchemaCacheUpdater(10*time.Minute, profile, log); err != nil {
		log.Warn("Failed to start schema cache updater", zap.Error(err))
		// Continue anyway - autocomplete will still work with initial data
	} else {
		log.Info("Schema cache updater started with 10-minute refresh interval")
	}

	app := tview.NewApplication()
	queryHistory := []string{}
	historyIndex := -1
	var historyLock sync.Mutex

	// Input field for SQL queries.
	input := tview.NewInputField().
		SetLabel("SQL> ").
		SetFieldWidth(0)

	// Results area - will be replaced with a table when results are available
	resultsArea := tview.NewFlex()

	// Initial welcome message
	welcomeText := tview.NewTextView().
		SetDynamicColors(true).
		SetScrollable(true).
		SetWrap(false).
		SetText("Welcome to Trino CLI. Enter your SQL query and press [green]Enter[white].\nPress [yellow]Ctrl+Space[white] for autocompletion.")

	resultsArea.AddItem(welcomeText, 0, 1, false)

	// Status bar to show execution state.
	statusBar := tview.NewTextView().
		SetDynamicColors(true).
		SetText("[yellow]Ready")

	// Layout.
	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(input, 1, 0, true).
		AddItem(resultsArea, 0, 1, false).
		AddItem(statusBar, 1, 0, false)

	// Set up autocomplete
	var autocompleteHandler *autocomplete.AutocompleteHandler
	autocompleteHandler, err := autocomplete.IntegrateWithTUI(app, input, flex, profile, log)
	if err != nil {
		log.Warn("Failed to initialize autocomplete", zap.Error(err))
		// Continue without autocomplete
	} else {
		log.Info("Autocomplete initialized successfully")
		defer autocompleteHandler.Stop()
	}

	// Handle query execution.
	input.SetDoneFunc(func(key tcell.Key) {
		if key != tcell.KeyEnter {
			return
		}

		query := input.GetText()
		if strings.TrimSpace(query) == "" {
			return
		}

		// Add to history.
		historyLock.Lock()
		queryHistory = append(queryHistory, query)
		historyIndex = len(queryHistory)
		historyLock.Unlock()

		log.Info("Executing query", zap.String("query", query))
		statusBar.SetText("[yellow]Executing query...")

		go func() {
			result, err := engine.ExecuteQuery(query, profile)
			app.QueueUpdateDraw(func() {
				if err != nil {
					log.Error("Query execution failed", zap.Error(err))

					// Show error message
					errorText := tview.NewTextView().
						SetDynamicColors(true).
						SetScrollable(true).
						SetWrap(true).
						SetText(fmt.Sprintf("[red]Error:[white] %v", err))

					// Clear results area and add error message
					resultsArea.Clear()
					resultsArea.AddItem(errorText, 0, 1, false)

					statusBar.SetText("[red]Execution failed")
				} else {
					log.Info("Query executed successfully",
						zap.Int("rows", len(result.Rows)),
						zap.Int("columns", len(result.Columns)))

					// Create a scrollable table for results
					resultTable := createResultTable(result, app, input)

					// Set a title showing the number of rows returned
					resultTable.SetTitle(fmt.Sprintf(" Query Results: %d rows ", len(result.Rows)))
					resultTable.SetTitleAlign(tview.AlignLeft)
					resultTable.SetBorderPadding(0, 0, 1, 1)

					// Clear results area and add the table
					resultsArea.Clear()
					resultsArea.AddItem(resultTable, 0, 1, false)

					// Set focus on the table to enable scrolling with arrow keys
					app.SetFocus(resultTable)

					statusBar.SetText("[green]Execution complete")
				}
				input.SetText("")
			})
		}()
	})

	// Keyboard shortcuts.
	app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		// First check if autocomplete handler wants to handle this key
		if autocompleteHandler != nil && autocompleteHandler.ProcessKey(event) {
			return nil
		}

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

// createResultTable renders query results as a scrollable, interactive table.
func createResultTable(result *engine.QueryResult, app *tview.Application, input *tview.InputField) *tview.Table {
	if len(result.Rows) == 0 {
		// Return a table with just the header and a "No results" message
		table := tview.NewTable().SetBorders(true)

		// Add column headers
		for colIndex, colName := range result.Columns {
			table.SetCell(0, colIndex,
				tview.NewTableCell(colName).
					SetTextColor(tcell.ColorGreen).
					SetAlign(tview.AlignLeft).
					SetExpansion(1))
		}

		// Add "No results" message
		if len(result.Columns) > 0 {
			table.SetCell(1, 0,
				tview.NewTableCell("[yellow]No results found.").
					SetAlign(tview.AlignLeft).
					SetSelectable(false))
		}

		return table
	}

	table := tview.NewTable().
		SetBorders(true).
		SetSelectable(true, false) // Allow row selection for easier reading

	// Add column headers with styling
	for colIndex, colName := range result.Columns {
		table.SetCell(0, colIndex,
			tview.NewTableCell(colName).
				SetTextColor(tcell.ColorGreen).
				SetAlign(tview.AlignLeft).
				SetExpansion(1).
				SetSelectable(false))
	}

	// Add data rows
	for rowIndex, row := range result.Rows {
		for colIndex, value := range row {
			var cellText string
			if value == nil {
				cellText = "NULL"
			} else {
				cellText = fmt.Sprintf("%v", value)
			}

			table.SetCell(rowIndex+1, colIndex, // +1 to account for header row
				tview.NewTableCell(cellText).
					SetAlign(tview.AlignLeft).
					SetExpansion(1))
		}
	}

	// Set table properties
	table.SetFixed(1, 0) // Fix header row
	table.SetSeparator(tview.Borders.Vertical)

	// Make the table scrollable and selectable
	table.SetSelectable(true, false)
	table.SetSelectedStyle(tcell.StyleDefault.Background(tcell.ColorNavy).Foreground(tcell.ColorWhite))

	// Add key handler for the table
	table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyEscape:
			// Return focus to the input field when Escape is pressed
			app.SetFocus(input)
			return nil
		}
		return event
	})

	return table
}
