package autocomplete

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"go.uber.org/zap"
)

// Common SQL keywords for autocompletion
var CommonSQLKeywords = []string{
	"SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "HAVING", "LIMIT",
	"JOIN", "LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "FULL JOIN", "CROSS JOIN",
	"ON", "AND", "OR", "NOT", "IN", "EXISTS", "BETWEEN", "LIKE", "IS NULL", "IS NOT NULL",
	"COUNT", "SUM", "AVG", "MIN", "MAX", "DISTINCT", "AS", "WITH", "UNION", "ALL",
	"INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE", "CREATE", "TABLE", "VIEW",
	"DROP", "ALTER", "ADD", "COLUMN", "DESC", "ASC", "PARTITION BY", "OVER", "CAST",
	"CASE", "WHEN", "THEN", "ELSE", "END", "COALESCE", "NULLIF", "EXTRACT", "CURRENT_DATE",
	"CURRENT_TIME", "CURRENT_TIMESTAMP", "INTERVAL",
}

// SQLCompletion types represent different categories of SQL suggestions
type SQLCompletionType int

const (
	Keyword SQLCompletionType = iota
	SchemaName
	TableName
	ColumnName
	Function
)

// Suggestion represents a single autocompletion suggestion
type Suggestion struct {
	Text       string
	Type       SQLCompletionType
	Score      float64 // Higher is better
	Schema     string  // Only for table/column suggestions
	Table      string  // Only for column suggestions
	DetailText string  // Additional context/details
}

// AutocompleteService provides SQL autocompletion functionality
type AutocompleteService struct {
	db             *sql.DB
	cache          *SchemaCache
	introspector   *SchemaIntrospector
	keywordTrie    *Trie
	logger         *zap.Logger
	mu             sync.RWMutex
	maxSuggestions int
}

// NewAutocompleteService creates a new autocomplete service
func NewAutocompleteService(db *sql.DB, cacheDir string, logger *zap.Logger) (*AutocompleteService, error) {
	if logger == nil {
		var err error
		logger, err = zap.NewProduction()
		if err != nil {
			// Fallback to empty logger if we can't create one
			logger = zap.NewNop()
		}
	}

	// Initialize schema cache
	cache, err := NewSchemaCache(cacheDir, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema cache: %w", err)
	}

	// Initialize schema introspector
	introspector := NewSchemaIntrospector(db, cache, logger)

	// Create keyword trie
	keywordTrie := NewTrie()
	for _, keyword := range CommonSQLKeywords {
		keywordTrie.Insert(keyword, 1) // Add priority 1 for all keywords
	}

	return &AutocompleteService{
		db:             db,
		cache:          cache,
		introspector:   introspector,
		keywordTrie:    keywordTrie,
		logger:         logger,
		maxSuggestions: 20, // Default max suggestions to show
	}, nil
}

// Start initializes the service and begins background refresh
func (ac *AutocompleteService) Start() error {
	// Load initial metadata from cache
	if err := ac.cache.LoadCache(); err != nil {
		ac.logger.Warn("Failed to initialize from cache", zap.Error(err))
		// Non-fatal, we'll refresh from Trino
	}

	// Do an initial refresh from Trino
	if err := ac.introspector.RefreshAll(); err != nil {
		ac.logger.Error("Initial schema refresh failed", zap.Error(err))
		// Return this error as we need metadata for autocomplete to work
		return fmt.Errorf("initial schema refresh failed: %w", err)
	}

	// Start background refresh
	ac.introspector.StartBackgroundRefresh()
	return nil
}

// Stop gracefully shuts down the service
func (ac *AutocompleteService) Stop() {
	ac.introspector.StopBackgroundRefresh()
}

// SetMaxSuggestions sets the maximum number of suggestions to return
func (ac *AutocompleteService) SetMaxSuggestions(max int) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	ac.maxSuggestions = max
}

// GetCompletions returns suggestions for the given SQL input and cursor position
func (ac *AutocompleteService) GetCompletions(sql string, cursorPos int) ([]Suggestion, error) {
	ac.mu.RLock()
	defer ac.mu.RUnlock()

	// Ensure we don't go out of bounds
	if cursorPos < 0 {
		cursorPos = 0
	}
	if cursorPos > len(sql) {
		cursorPos = len(sql)
	}

	// Get the word at cursor
	word, wordStart := getWordAtCursor(sql, cursorPos)
	ac.logger.Debug("Getting completions",
		zap.String("word", word),
		zap.Int("wordStart", wordStart),
		zap.Int("cursorPos", cursorPos))

	// Get context to determine what type of completions to show
	ctx := analyzeContext(sql, cursorPos)

	// Use the new contextual suggestions function to get more relevant suggestions
	contextualSuggestions := GetContextualSuggestions(sql, cursorPos, ac.cache)

	// Convert string suggestions to Suggestion objects
	var suggestions []Suggestion
	if len(contextualSuggestions) > 0 {
		for _, text := range contextualSuggestions {
			// Determine suggestion type based on context
			suggestionType := Keyword
			switch ctx.completionType {
			case SchemaName:
				suggestionType = SchemaName
			case TableName:
				suggestionType = TableName
			case ColumnName:
				suggestionType = ColumnName
			case Function:
				suggestionType = Function
			}

			score := calculateScore(word, text)
			suggestions = append(suggestions, Suggestion{
				Text:  text,
				Type:  suggestionType,
				Score: score,
			})
		}
	} else {
		// Fall back to the original method if contextual suggestions are empty
		suggestions = ac.getSuggestionsByContext(word, ctx)
	}

	// Sort by score and limit results
	sortSuggestionsByScore(suggestions)
	if len(suggestions) > ac.maxSuggestions {
		suggestions = suggestions[:ac.maxSuggestions]
	}

	return suggestions, nil
}

// Get suggestions based on SQL context
func (ac *AutocompleteService) getSuggestionsByContext(prefix string, ctx sqlContext) []Suggestion {
	var suggestions []Suggestion

	// Always include keyword suggestions
	keywordSuggestions := ac.getKeywordSuggestions(prefix)
	suggestions = append(suggestions, keywordSuggestions...)

	// Add context-specific suggestions
	switch ctx.completionType {
	case SchemaName:
		schemaSuggestions := ac.getSchemaSuggestions(prefix)
		suggestions = append(suggestions, schemaSuggestions...)

	case TableName:
		var tableSuggestions []Suggestion
		if ctx.schema != "" {
			// If we know the schema, only get tables from that schema
			tableSuggestions = ac.getTableSuggestions(prefix, ctx.schema)
		} else {
			// Otherwise get all tables
			tableSuggestions = ac.getAllTableSuggestions(prefix)
		}
		suggestions = append(suggestions, tableSuggestions...)

	case ColumnName:
		var columnSuggestions []Suggestion
		if ctx.table != "" {
			// If we know the table, only get columns from that table
			columnSuggestions = ac.getColumnSuggestions(prefix, ctx.schema, ctx.table)
		} else if ctx.schema != "" {
			// If we only know the schema, get all columns from that schema
			columnSuggestions = ac.getAllColumnSuggestionsForSchema(prefix, ctx.schema)
		} else {
			// Otherwise get all columns
			columnSuggestions = ac.getAllColumnSuggestions(prefix)
		}
		suggestions = append(suggestions, columnSuggestions...)

	case Function:
		// Add function suggestions
		functionSuggestions := ac.getFunctionSuggestions(prefix)
		suggestions = append(suggestions, functionSuggestions...)
	}

	return suggestions
}

// getKeywordSuggestions returns SQL keyword suggestions
func (ac *AutocompleteService) getKeywordSuggestions(prefix string) []Suggestion {
	words := ac.keywordTrie.GetSuggestions(strings.ToUpper(prefix), 10) // Get top 10 keyword matches
	suggestions := make([]Suggestion, 0, len(words))

	for _, word := range words {
		score := calculateScore(prefix, word)
		suggestions = append(suggestions, Suggestion{
			Text:  word,
			Type:  Keyword,
			Score: score,
		})
	}

	return suggestions
}

// getSchemaSuggestions returns schema name suggestions
func (ac *AutocompleteService) getSchemaSuggestions(prefix string) []Suggestion {
	schemas, err := ac.cache.GetSchemas()
	if err != nil {
		ac.logger.Error("Failed to get schemas from cache", zap.Error(err))
		return nil
	}

	suggestions := make([]Suggestion, 0, len(schemas))
	for _, schema := range schemas {
		if strings.HasPrefix(strings.ToLower(schema), strings.ToLower(prefix)) {
			score := calculateScore(prefix, schema)
			suggestions = append(suggestions, Suggestion{
				Text:  schema,
				Type:  SchemaName,
				Score: score,
			})
		}
	}

	return suggestions
}

// getTableSuggestions returns table suggestions for a specific schema
func (ac *AutocompleteService) getTableSuggestions(prefix, schema string) []Suggestion {
	tables, err := ac.cache.GetTables(schema)
	if err != nil {
		ac.logger.Error("Failed to get tables from cache",
			zap.String("schema", schema),
			zap.Error(err))
		return nil
	}

	suggestions := make([]Suggestion, 0, len(tables))
	for _, table := range tables {
		if strings.HasPrefix(strings.ToLower(table), strings.ToLower(prefix)) {
			score := calculateScore(prefix, table)
			suggestions = append(suggestions, Suggestion{
				Text:       table,
				Type:       TableName,
				Score:      score,
				Schema:     schema,
				DetailText: fmt.Sprintf("%s.%s", schema, table),
			})
		}
	}

	return suggestions
}

// getAllTableSuggestions returns table suggestions across all schemas
func (ac *AutocompleteService) getAllTableSuggestions(prefix string) []Suggestion {
	var suggestions []Suggestion

	schemas, err := ac.cache.GetSchemas()
	if err != nil {
		ac.logger.Error("Failed to get schemas from cache", zap.Error(err))
		return nil
	}

	for _, schema := range schemas {
		schemaSuggestions := ac.getTableSuggestions(prefix, schema)
		suggestions = append(suggestions, schemaSuggestions...)
	}

	return suggestions
}

// getColumnSuggestions returns column suggestions for a specific table
func (ac *AutocompleteService) getColumnSuggestions(prefix, schema, table string) []Suggestion {
	columns, err := ac.cache.GetColumns(schema, table)
	if err != nil {
		ac.logger.Error("Failed to get columns from cache",
			zap.String("schema", schema),
			zap.String("table", table),
			zap.Error(err))
		return nil
	}

	suggestions := make([]Suggestion, 0, len(columns))
	for _, col := range columns {
		if strings.HasPrefix(strings.ToLower(col.Name), strings.ToLower(prefix)) {
			score := calculateScore(prefix, col.Name)
			suggestions = append(suggestions, Suggestion{
				Text:       col.Name,
				Type:       ColumnName,
				Score:      score,
				Schema:     schema,
				Table:      table,
				DetailText: fmt.Sprintf("%s.%s.%s (%s)", schema, table, col.Name, col.DataType),
			})
		}
	}

	return suggestions
}

// getAllColumnSuggestionsForSchema returns column suggestions across all tables in a schema
func (ac *AutocompleteService) getAllColumnSuggestionsForSchema(prefix, schema string) []Suggestion {
	var suggestions []Suggestion

	tables, err := ac.cache.GetTables(schema)
	if err != nil {
		ac.logger.Error("Failed to get tables from cache",
			zap.String("schema", schema),
			zap.Error(err))
		return nil
	}

	for _, table := range tables {
		tableSuggestions := ac.getColumnSuggestions(prefix, schema, table)
		suggestions = append(suggestions, tableSuggestions...)
	}

	return suggestions
}

// getAllColumnSuggestions returns column suggestions across all schemas and tables
func (ac *AutocompleteService) getAllColumnSuggestions(prefix string) []Suggestion {
	var suggestions []Suggestion

	schemas, err := ac.cache.GetSchemas()
	if err != nil {
		ac.logger.Error("Failed to get schemas from cache", zap.Error(err))
		return nil
	}

	for _, schema := range schemas {
		schemaSuggestions := ac.getAllColumnSuggestionsForSchema(prefix, schema)
		suggestions = append(suggestions, schemaSuggestions...)
	}

	return suggestions
}

// getFunctionSuggestions returns SQL function suggestions
func (ac *AutocompleteService) getFunctionSuggestions(prefix string) []Suggestion {
	// Common SQL functions
	functions := []string{
		"COUNT", "SUM", "AVG", "MIN", "MAX", "STDDEV", "VARIANCE",
		"LOWER", "UPPER", "CONCAT", "SUBSTRING", "TRIM", "LENGTH",
		"CAST", "ROUND", "FLOOR", "CEILING", "ABS", "MOD",
		"CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "EXTRACT",
	}

	suggestions := make([]Suggestion, 0)
	for _, fn := range functions {
		if strings.HasPrefix(strings.ToUpper(fn), strings.ToUpper(prefix)) {
			score := calculateScore(prefix, fn)
			suggestions = append(suggestions, Suggestion{
				Text:  fn,
				Type:  Function,
				Score: score,
			})
		}
	}

	return suggestions
}

// sqlContext represents the SQL context at a given position
type sqlContext struct {
	completionType SQLCompletionType
	schema         string // Set if we know the schema
	table          string // Set if we know the table
}

// analyzeContext determines what type of completion to show based on SQL context
func analyzeContext(sql string, cursorPos int) sqlContext {
	// This is a simplified implementation
	// A full implementation would use a SQL parser

	// Default to keyword completion
	ctx := sqlContext{
		completionType: Keyword,
	}

	// Check for schema completion (e.g., "SELECT * FROM sch")
	fromMatch := strings.LastIndex(sql[:cursorPos], "FROM ")
	if fromMatch != -1 {
		afterFrom := sql[fromMatch+5 : cursorPos]
		if !strings.Contains(afterFrom, " ") {
			ctx.completionType = SchemaName
			return ctx
		}
	}

	// Check for table completion (e.g., "SELECT * FROM schema.")
	dotMatch := strings.LastIndex(sql[:cursorPos], ".")
	if dotMatch != -1 && dotMatch < cursorPos-1 {
		beforeDot := sql[:dotMatch]
		lastSpaceBeforeDot := strings.LastIndex(beforeDot, " ")
		if lastSpaceBeforeDot != -1 {
			potentialSchema := strings.TrimSpace(beforeDot[lastSpaceBeforeDot:])
			ctx.schema = potentialSchema
			ctx.completionType = TableName
			return ctx
		}
	}

	// Check for column completion after SELECT
	selectMatch := strings.LastIndex(sql[:cursorPos], "SELECT ")
	if selectMatch != -1 {
		afterSelect := sql[selectMatch+7 : cursorPos]
		if !strings.Contains(afterSelect, "FROM") && !strings.Contains(afterSelect, " WHERE ") {
			ctx.completionType = ColumnName
			return ctx
		}
	}

	// Check for column completion after WHERE
	whereMatch := strings.LastIndex(sql[:cursorPos], "WHERE ")
	if whereMatch != -1 {
		ctx.completionType = ColumnName
		return ctx
	}

	return ctx
}

// GetContextualSuggestions returns suggestions based on the SQL query context
// It analyzes the query and cursor position to provide more relevant suggestions
func GetContextualSuggestions(query string, cursorPos int, cache *SchemaCache) []string {
	// Only look at query before cursor
	queryBeforeCursor := query
	if cursorPos < len(query) {
		queryBeforeCursor = query[:cursorPos]
	}

	tokens := strings.Fields(queryBeforeCursor)
	if len(tokens) == 0 {
		return nil
	}

	lastToken := tokens[len(tokens)-1]
	lastTokenUpper := strings.ToUpper(lastToken)

	// Get the word at cursor for prefix matching
	word, _ := getWordAtCursor(query, cursorPos)

	// Default limit for suggestions
	limit := 50

	switch lastTokenUpper {
	case "SELECT":
		// After SELECT, suggest columns and functions
		columns, err := cache.GetAllColumns()
		if err != nil {
			return nil
		}

		// Add SQL functions that are commonly used in SELECT
		selectFunctions := []string{
			"COUNT", "SUM", "AVG", "MIN", "MAX", "DISTINCT", "CAST", "COALESCE",
			"NULLIF", "EXTRACT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
		}

		// Combine columns and functions
		suggestions := append(columns, selectFunctions...)

		// Filter by prefix if there is one
		if word != "" {
			var filtered []string
			for _, s := range suggestions {
				if strings.HasPrefix(strings.ToLower(s), strings.ToLower(word)) {
					filtered = append(filtered, s)
				}
			}
			return filtered
		}

		return suggestions

	case "FROM":
		// After FROM, suggest tables and schemas
		tables, err := cache.GetAllTables()
		if err != nil {
			return nil
		}

		// Also include schema-qualified tables
		schemaQualifiedTables, err := cache.GetAllSchemaQualifiedTables()
		if err == nil {
			tables = append(tables, schemaQualifiedTables...)
		}

		// Get schemas too
		schemas, err := cache.GetSchemas()
		if err == nil {
			tables = append(tables, schemas...)
		}

		// Filter by prefix if there is one
		if word != "" {
			var filtered []string
			for _, s := range tables {
				if strings.HasPrefix(strings.ToLower(s), strings.ToLower(word)) {
					filtered = append(filtered, s)
				}
			}
			return filtered
		}

		return tables

	case "JOIN":
		// After JOIN, suggest tables
		tables, err := cache.GetAllTables()
		if err != nil {
			return nil
		}

		// Also include schema-qualified tables
		schemaQualifiedTables, err := cache.GetAllSchemaQualifiedTables()
		if err == nil {
			tables = append(tables, schemaQualifiedTables...)
		}

		// Filter by prefix if there is one
		if word != "" {
			var filtered []string
			for _, s := range tables {
				if strings.HasPrefix(strings.ToLower(s), strings.ToLower(word)) {
					filtered = append(filtered, s)
				}
			}
			return filtered
		}

		return tables

	case "WHERE", "AND", "OR", "ON":
		// After WHERE/AND/OR/ON, suggest columns
		columns, err := cache.GetAllColumns()
		if err != nil {
			return nil
		}

		// Filter by prefix if there is one
		if word != "" {
			var filtered []string
			for _, s := range columns {
				if strings.HasPrefix(strings.ToLower(s), strings.ToLower(word)) {
					filtered = append(filtered, s)
				}
			}
			return filtered
		}

		return columns

	case "ORDER", "GROUP":
		// The next token should be "BY"
		return []string{"BY"}

	case "BY":
		// After ORDER BY or GROUP BY, suggest columns
		// Check if the token before "BY" is "ORDER" or "GROUP"
		if len(tokens) >= 2 {
			prevToken := strings.ToUpper(tokens[len(tokens)-2])
			if prevToken == "ORDER" || prevToken == "GROUP" {
				columns, err := cache.GetAllColumns()
				if err != nil {
					return nil
				}

				// Filter by prefix if there is one
				if word != "" {
					var filtered []string
					for _, s := range columns {
						if strings.HasPrefix(strings.ToLower(s), strings.ToLower(word)) {
							filtered = append(filtered, s)
						}
					}
					return filtered
				}

				return columns
			}
		}

		// Default to general suggestions
		return cache.GetSuggestions(word, limit)

	default:
		// For other contexts, provide general suggestions
		return cache.GetSuggestions(word, limit)
	}
}

// getWordAtCursor returns the word at the cursor position
func getWordAtCursor(sql string, cursorPos int) (string, int) {
	if cursorPos <= 0 || cursorPos > len(sql) {
		return "", 0
	}

	// Find start of the word
	start := cursorPos - 1
	for start >= 0 && isWordChar(sql[start]) {
		start--
	}
	start++

	// Find end of the word
	end := cursorPos
	for end < len(sql) && isWordChar(sql[end]) {
		end++
	}

	if start >= end {
		return "", start
	}

	return sql[start:end], start
}

// isWordChar returns whether a character is part of a word
func isWordChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}

// calculateScore calculates a relevance score for a suggestion
func calculateScore(prefix, suggestion string) float64 {
	if len(prefix) == 0 {
		return 0.5 // Default score for empty prefix
	}

	// Exact prefix match gets highest score
	if strings.HasPrefix(strings.ToLower(suggestion), strings.ToLower(prefix)) {
		prefixRatio := float64(len(prefix)) / float64(len(suggestion))
		return 1.0 - (1.0-prefixRatio)*0.1 // Higher score for more complete matches
	}

	// Case insensitive matching
	lcPrefix := strings.ToLower(prefix)
	lcSuggestion := strings.ToLower(suggestion)

	// Check for substring match
	if strings.Contains(lcSuggestion, lcPrefix) {
		return 0.7
	}

	// Check for fuzzy match (e.g., acronym matching)
	if fuzzyMatch(lcPrefix, lcSuggestion) {
		return 0.6
	}

	// Levenshtein distance could be added here
	// Lower score for weak matches
	return 0.1
}

// fuzzyMatch checks if prefix matches the suggestion in a fuzzy way
// For example, "sel" would match "SELECT"
func fuzzyMatch(prefix, suggestion string) bool {
	if len(prefix) == 0 {
		return true
	}

	i, j := 0, 0
	for i < len(prefix) && j < len(suggestion) {
		if prefix[i] == suggestion[j] {
			i++
		}
		j++
	}

	return i == len(prefix)
}

// sortSuggestionsByScore sorts suggestions by score in descending order
func sortSuggestionsByScore(suggestions []Suggestion) {
	// Simple bubble sort for simplicity
	// In production, use sort.Slice with a more efficient sort algorithm
	n := len(suggestions)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			if suggestions[j].Score < suggestions[j+1].Score {
				suggestions[j], suggestions[j+1] = suggestions[j+1], suggestions[j]
			}
		}
	}
}

// BoostSuggestion increases the score of a suggestion when it's used
func (ac *AutocompleteService) BoostSuggestion(suggestion Suggestion) {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	// Boost score in the appropriate trie based on suggestion type
	switch suggestion.Type {
	case Keyword:
		ac.keywordTrie.BoostWord(suggestion.Text, 5) // Boost keywords
	case SchemaName, TableName, ColumnName, Function:
		// For schema objects, we'll boost them in the cache's trie
		if ac.cache != nil {
			ac.cache.BoostWord(suggestion.Text, 10) // Higher boost for schema objects
		}
	}

	ac.logger.Debug("Boosted suggestion score",
		zap.String("text", suggestion.Text),
		zap.Int("type", int(suggestion.Type)))
}
