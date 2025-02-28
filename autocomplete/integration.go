package autocomplete

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/TFMV/trino-cli/config"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"go.uber.org/zap"
)

// AutocompleteHandler manages SQL autocompletion integration with TUI
type AutocompleteHandler struct {
	service           *AutocompleteService
	suggestionBox     *tview.List
	inputField        *tview.InputField
	app               *tview.Application
	logger            *zap.Logger
	suggestionVisible bool
	suggestionText    string
	suggestionOffset  int
	currentCatalog    string
	currentSchema     string
	suggestions       []Suggestion
	suggestionsMutex  sync.RWMutex
}

// NewAutocompleteHandler creates a new autocomplete handler for the TUI
func NewAutocompleteHandler(db *sql.DB, profileName string, app *tview.Application,
	inputField *tview.InputField, logger *zap.Logger) (*AutocompleteHandler, error) {

	if logger == nil {
		var err error
		logger, err = zap.NewProduction()
		if err != nil {
			return nil, fmt.Errorf("failed to create logger: %w", err)
		}
	}

	// Create cache directory in user's home directory
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("failed to get home directory: %w", err)
	}
	cacheDir := filepath.Join(homeDir, ".trino-cli", "autocomplete_cache")

	// Create autocomplete service
	service, err := NewAutocompleteService(db, cacheDir, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create autocomplete service: %w", err)
	}

	// Create suggestion box
	suggestionBox := tview.NewList().
		ShowSecondaryText(true).
		SetHighlightFullLine(true).
		SetMainTextColor(tcell.ColorWhite).
		SetSelectedTextColor(tcell.ColorBlack).
		SetSelectedBackgroundColor(tcell.ColorAqua)

	handler := &AutocompleteHandler{
		service:           service,
		suggestionBox:     suggestionBox,
		inputField:        inputField,
		app:               app,
		logger:            logger,
		suggestionVisible: false,
		currentCatalog:    "default", // Default catalog
		currentSchema:     "public",  // Default schema
	}

	// Start autocomplete service
	if err := service.Start(); err != nil {
		logger.Warn("Autocomplete service initialization had issues", zap.Error(err))
		// Continue anyway - still usable for keywords
	}

	return handler, nil
}

// ProcessKey handles keyboard input for autocompletion
func (ah *AutocompleteHandler) ProcessKey(event *tcell.EventKey) bool {
	// If suggestions are visible, handle selection navigation
	if ah.suggestionVisible {
		switch event.Key() {
		case tcell.KeyDown:
			// Select next suggestion
			count := ah.suggestionBox.GetItemCount()
			if count > 0 {
				current := ah.suggestionBox.GetCurrentItem()
				if current < count-1 {
					ah.suggestionBox.SetCurrentItem(current + 1)
				}
			}
			return true
		case tcell.KeyUp:
			// Select previous suggestion
			if ah.suggestionBox.GetItemCount() > 0 {
				current := ah.suggestionBox.GetCurrentItem()
				if current > 0 {
					ah.suggestionBox.SetCurrentItem(current - 1)
				}
			}
			return true
		case tcell.KeyEnter, tcell.KeyTab:
			// Accept current suggestion
			if ah.suggestionBox.GetItemCount() > 0 {
				ah.acceptSuggestion(ah.suggestionBox.GetCurrentItem())
			}
			return true
		case tcell.KeyEscape:
			// Hide suggestions
			ah.HideSuggestions()
			return true
		}
	}

	// Handle Tab for opening suggestions
	if event.Key() == tcell.KeyTab && !ah.suggestionVisible {
		ah.ShowSuggestions()
		return true
	}

	// Handle Ctrl+Space for opening suggestions
	if event.Key() == tcell.KeyCtrlSpace && !ah.suggestionVisible {
		ah.ShowSuggestions()
		return true
	}

	return false // Event not handled
}

// Update should be called when the input text changes
func (ah *AutocompleteHandler) Update(text string, cursorPos int) {
	// Update suggestions based on new text
	go func() {
		suggestions, err := ah.service.GetCompletions(text, cursorPos)
		if err != nil {
			ah.logger.Error("Failed to get completions", zap.Error(err))
			return
		}

		ah.suggestionsMutex.Lock()
		ah.suggestions = suggestions
		ah.suggestionsMutex.Unlock()

		// If suggestions box is visible, update it
		if ah.suggestionVisible {
			ah.app.QueueUpdateDraw(func() {
				ah.updateSuggestionBox()
			})
		}
	}()
}

// Stop should be called when closing the application
func (ah *AutocompleteHandler) Stop() {
	ah.service.Stop()
}

// ShowSuggestions displays the suggestion box
func (ah *AutocompleteHandler) ShowSuggestions() {
	text := ah.inputField.GetText()
	cursorPos := len(text) // Default to end of text if no cursor position available

	word, wordStart := getWordAtCursor(text, cursorPos)
	ah.suggestionText = word
	ah.suggestionOffset = wordStart

	// Update suggestions box content
	ah.updateSuggestionBox()

	// Make the suggestions visible if we have suggestions
	if ah.suggestionBox.GetItemCount() > 0 {
		ah.suggestionVisible = true

		// Position the suggestion box below the input field
		// This needs to be implemented with the specific TUI layout
		ah.app.SetFocus(ah.suggestionBox)
	}
}

// HideSuggestions hides the suggestion box
func (ah *AutocompleteHandler) HideSuggestions() {
	if !ah.suggestionVisible {
		return
	}

	ah.suggestionVisible = false
	ah.app.SetFocus(ah.inputField)
}

// UpdateSuggestionBox updates the content of the suggestion box
func (ah *AutocompleteHandler) updateSuggestionBox() {
	ah.suggestionBox.Clear()

	ah.suggestionsMutex.RLock()
	defer ah.suggestionsMutex.RUnlock()

	for i, suggestion := range ah.suggestions {
		switch suggestion.Type {
		case Keyword:
			ah.suggestionBox.AddItem(suggestion.Text, "Keyword", 0, nil)
		case SchemaName:
			ah.suggestionBox.AddItem(suggestion.Text, "Schema", 0, nil)
		case TableName:
			ah.suggestionBox.AddItem(suggestion.Text, suggestion.DetailText, 0, nil)
		case ColumnName:
			ah.suggestionBox.AddItem(suggestion.Text, suggestion.DetailText, 0, nil)
		case Function:
			ah.suggestionBox.AddItem(suggestion.Text, "Function", 0, nil)
		}

		// Limit the number of displayed suggestions
		if i >= 9 { // Show max 10 suggestions
			break
		}
	}
}

// AcceptSuggestion applies the selected suggestion to the input field
func (ah *AutocompleteHandler) acceptSuggestion(index int) {
	if index < 0 || index >= len(ah.suggestions) {
		return
	}

	suggestion := ah.suggestions[index]

	// Boost the score of the selected suggestion
	go ah.service.BoostSuggestion(suggestion)

	// Get current text and cursor position
	text := ah.inputField.GetText()
	cursorPos := len(text) // Default to end of text

	// Find the word we're replacing
	_, wordStart := getWordAtCursor(text, cursorPos)

	// Replace the current word with the suggestion
	newText := text[:wordStart] + suggestion.Text

	// Add proper spacing based on suggestion type
	switch suggestion.Type {
	case SchemaName:
		newText += "."
	case TableName:
		// If we're in a FROM clause, add a space
		if strings.Contains(strings.ToUpper(text[:wordStart]), "FROM") {
			newText += " "
		}
	case ColumnName:
		// Add comma if we're in a SELECT list
		if strings.Contains(strings.ToUpper(text[:wordStart]), "SELECT") &&
			!strings.Contains(strings.ToUpper(text[wordStart:]), "FROM") {
			newText += ", "
		}
	case Keyword:
		// Add space after keywords
		newText += " "
	}

	// Add any text that was after the current word
	if wordStart+len(ah.suggestionText) < len(text) {
		newText += text[wordStart+len(ah.suggestionText):]
	}

	// Update the input field
	ah.inputField.SetText(newText)

	// Hide the suggestions
	ah.HideSuggestions()
}

// IntegrateWithTUI integrates the autocomplete handler with the TUI
func IntegrateWithTUI(app *tview.Application, input *tview.InputField, flex *tview.Flex, profileName string, logger *zap.Logger) (*AutocompleteHandler, error) {
	// Get database connection
	dsn := fmt.Sprintf("http://%s@%s:%d?catalog=%s&schema=%s",
		config.AppConfig.Profiles[profileName].User,
		config.AppConfig.Profiles[profileName].Host,
		config.AppConfig.Profiles[profileName].Port,
		config.AppConfig.Profiles[profileName].Catalog,
		config.AppConfig.Profiles[profileName].Schema)

	db, err := sql.Open("trino", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Create autocomplete handler
	handler, err := NewAutocompleteHandler(db, profileName, app, input, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create autocomplete handler: %w", err)
	}

	// Create a flex container for the suggestion box
	suggestionFlex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(handler.suggestionBox, 0, 1, false)

	// Add suggestion box to main flex (invisible initially)
	flex.AddItem(suggestionFlex, 0, 0, false)

	// Set up input field to trigger autocomplete updates
	input.SetChangedFunc(func(text string) {
		cursorPos := len(text) // Default to end of text
		handler.Update(text, cursorPos)
	})

	// Intercept key events for autocomplete navigation
	originalInputCapture := app.GetInputCapture()
	app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		// Give autocomplete handler first chance to process the key
		if handler.ProcessKey(event) {
			return nil
		}

		// Otherwise, pass to original handler if it exists
		if originalInputCapture != nil {
			return originalInputCapture(event)
		}

		return event
	})

	return handler, nil
}
