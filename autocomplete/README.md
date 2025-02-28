# SQL Autocompletion for Trino CLI

This package implements an intelligent SQL autocompletion system for the Trino CLI. It provides real-time suggestions for SQL keywords, schema names, table names, and column names as you type.

## Features

- **Trie-based Prefix Matching**: Fast lookup of SQL keywords, schema names, table names, and column names.
- **Schema Metadata Caching**: Local SQLite database to store schema metadata for quick access.
- **Real-time Schema Introspection**: Queries Trino's `information_schema` to keep metadata up-to-date.
- **Automatic Background Refresh**: Periodically refreshes schema metadata to ensure suggestions are current.
- **Context-aware Suggestions**: Provides relevant suggestions based on the SQL context (e.g., after SELECT, FROM, WHERE).
- **Fuzzy Matching**: Supports fuzzy matching for more flexible autocompletion.
- **Dynamic Suggestion Boosting**: Frequently used items get higher scores and appear higher in suggestion lists

## Components

The autocompletion system consists of several components:

1. **Trie**: A prefix tree data structure for efficient prefix-based lookups.
2. **SchemaCache**: A local SQLite database that stores schema metadata.
3. **SchemaIntrospector**: A component that queries Trino's `information_schema` to keep metadata up-to-date.
4. **AutocompleteService**: The main service that integrates all components and provides autocompletion suggestions.
5. **AutocompleteHandler**: Integrates the autocompletion service with the TUI.

## Usage

The autocompletion system is integrated with the Trino CLI's TUI. To use it:

1. Press `Ctrl+Space` to show autocompletion suggestions.
2. Use `Up` and `Down` arrow keys to navigate through suggestions.
3. Press `Enter` or `Tab` to accept the selected suggestion.
4. Press `Escape` to hide the suggestions.

## Implementation Details

### Trie

The trie data structure is used for efficient prefix-based lookups. It supports:

- Inserting words with priority scores
- Exact matching
- Prefix-based suggestions
- Fuzzy matching
- Dynamic suggestion boosting

The trie implementation uses a scoring system to rank suggestions, with higher scores appearing first in the suggestion list. The scoring is dynamic - as users select suggestions, their scores are automatically boosted, making frequently used items appear higher in the suggestion list over time.

### Schema Cache

The schema cache stores metadata in a local SQLite database. It includes:

- Schema names
- Table names
- Column names and data types
- SQL keywords

### Schema Introspector

The schema introspector queries Trino's `information_schema` to keep metadata up-to-date. It supports:

- Full schema refresh
- Background refresh at configurable intervals (default: 10 minutes)
- Selective schema refresh

The background refresh ensures that the autocompletion system always has up-to-date schema information, even when tables or columns are added, modified, or removed in the Trino database. This happens automatically without interrupting the user's workflow.

### Autocomplete Service

The autocomplete service integrates all components and provides autocompletion suggestions. It:

- Analyzes SQL context to determine what type of suggestions to show
- Ranks suggestions based on relevance
- Limits the number of suggestions to avoid overwhelming the user

### Context-Aware Suggestions

The autocompletion system provides intelligent, context-aware suggestions based on the SQL query structure:

- After `SELECT`: Suggests column names and SQL functions (COUNT, SUM, AVG, etc.)
- After `FROM`: Suggests table names and schema names
- After `JOIN`: Suggests table names
- After `WHERE`, `AND`, `OR`, `ON`: Suggests column names
- After `ORDER` or `GROUP`: Suggests `BY`
- After `ORDER BY` or `GROUP BY`: Suggests column names

This context-awareness makes the autocompletion system more helpful by providing relevant suggestions based on what the user is likely to type next, reducing the cognitive load and speeding up query writing.

## Dependencies

- `database/sql`: For database connections
- `github.com/mattn/go-sqlite3`: For SQLite database
- `github.com/rivo/tview`: For TUI components
- `github.com/gdamore/tcell/v2`: For keyboard handling
- `go.uber.org/zap`: For logging

## Setup

To set up the autocompletion system:

1. Ensure the required dependencies are installed:
   ```
   go mod tidy
   ```

2. The system will automatically create a cache directory in the user's home directory at `~/.trino-cli/autocomplete_cache`.

## Future Improvements

- Add support for more complex SQL parsing
- Improve fuzzy matching algorithm
- Add support for custom SQL dialects
- Implement more intelligent context-aware suggestions
- Add support for query history-based suggestions 