# Trino CLI

A high-performance, feature-rich command-line interface for Trino that enhances interactive data exploration and analysis.

## Overview

Trino CLI is a modern terminal-based tool designed to make working with Trino databases faster and more intuitive. It provides an interactive query experience with features like syntax highlighting, query history, result caching, and multiple export formats.

## Features

- **Interactive Query Interface**: Terminal UI with syntax highlighting and command history
- **Batch Query Execution**: Run queries directly from the command line
- **Local Result Caching**: Store and replay query results without re-executing queries
- **Multiple Export Formats**: Export results as CSV, JSON, Arrow, or Parquet
- **Connection Profiles**: Easily switch between different Trino environments
- **Intelligent SQL Autocompletion**: Powerful SQL autocompletion system
- **Persistent Query History**: Track, search, and replay queries across sessions
- **Interactive Schema Browser**: Navigate catalogs, schemas, tables, and columns in a TUI

## Installation

```bash
# Clone the repository
git clone https://github.com/TFMV/trino-cli.git
cd trino-cli

# Build the binary
go build -o trino-cli

# Move to a directory in your PATH (optional)
sudo mv trino-cli /usr/local/bin/
```

## Configuration

Create a configuration file at `~/.trino-cli.yaml`:

```yaml
profiles:
  default:
    host: localhost
    port: 8080
    user: user
    catalog: default
    schema: public
  
  prod:
    host: trino.production.example.com
    port: 443
    user: prod_user
    catalog: hive
    schema: analytics
    ssl: true
```

## Usage

### Interactive Mode

```bash
# Start interactive mode with default profile
trino-cli

# Start with a specific profile
trino-cli --profile prod
```

### Batch Mode

```bash
# Execute a single query and exit
trino-cli -e "SELECT * FROM orders LIMIT 10"

# Execute a query and export results
trino-cli export --format csv "SELECT * FROM users" > users.csv
```

### Query History Management

The Trino CLI records all executed queries to a local SQLite database, allowing you to easily review, search, and replay past queries.

```bash
# List recent queries
trino-cli history list

# List with pagination
trino-cli history list --limit 50 --offset 10

# Search for queries containing specific terms
trino-cli history search "orders"

# Use fuzzy search for more flexible matching
trino-cli history search "join users" --fuzzy

# Replay a specific query by its ID
trino-cli history replay 1630522845123456789

# Clear query history
trino-cli history clear

# Clear history older than a specific number of days
trino-cli history clear --days 30
```

Each history entry includes:

- Unique query ID
- Timestamp
- Profile used
- Execution duration
- Number of rows returned
- The SQL query itself

### Schema Browser

The CLI includes an interactive schema browser that allows you to explore the structure of your Trino databases directly from the terminal.

```bash
# Launch the schema browser
trino-cli schema browse
```

In the schema browser:

- Navigate with arrow keys
- Expand/collapse nodes with Enter
- Exit with Escape
- Explore catalogs, schemas, tables, and columns in a hierarchical tree view
- View detailed column metadata including data types

### Cache Management

```bash
# List cached queries
trino-cli cache list

# Replay a cached query result
trino-cli cache replay query_1234 --pretty
```

### Intelligent SQL Autocompletion

The Trino CLI features a powerful SQL autocompletion system that helps you write queries faster and with fewer errors:

- **Context-aware suggestions** based on your query structure
  - After SELECT: suggests columns and functions
  - After FROM/JOIN: suggests tables and schemas
  - After WHERE/AND/OR: suggests columns
  - After ORDER/GROUP: suggests BY
- **Schema-aware completions** for catalogs, schemas, tables, and columns
- **Automatic schema refresh** every 10 minutes to keep suggestions up-to-date
- **Keyword and function suggestions** for SQL syntax
- **Dynamic suggestion boosting** that learns from your usage patterns, prioritizing frequently used items
- **Fuzzy matching** for more flexible completions

To use autocompletion:

- Press `Ctrl+Space` to show suggestions based on your current cursor position
- Navigate suggestions with `Up/Down` arrow keys
- Press `Tab` or `Enter` to accept a suggestion
- Press `Esc` to dismiss suggestions

## Keyboard Shortcuts

In interactive mode:

- **Enter**: Execute query
- **Up/Down**: Navigate through query history
- **Esc**: Clear input
- **Ctrl+C**: Exit application

## Roadmap

The following features are planned for future releases:

### Near-term Goals

- **Enhanced SQL Intelligence**:
  - Improved autocomplete with schema awareness
  - Smart query suggestions based on history
  - Syntax error detection

- **Visualization Capabilities**:
  - Terminal-based charts (histograms, bar charts)
  - Column metadata previews

### Medium-term Goals

- **Advanced Caching**:
  - Arrow-powered local result storage
  - Offline query capability against cached results
  - Local joins between cached results

- **Performance Optimizations**:
  - Connection pooling
  - Parallel query execution
  - Streaming result processing

### Long-term Goals

- **Extended UI Features**:
  - Multi-line editing with syntax highlighting
  - Query plan visualization

- **Enterprise Features**:
  - Role-based access control integration
  - Secure credential management
  - Query auditing and logging

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
