# Trino CLI for Exploratory Data Analysis

## Overview

This Trino CLI aims to be a fast, intuitive, and interactive tool that enhances how developers, data engineers, and analysts interact with Trino. It should feel lightweight yet powerful, minimizing friction while maximizing productivity.

## 1. Core Design Principles

### Speed & Efficiency

- **Low-latency Query Execution**: Leverage Arrow for fast data transfers when possible
- **Smart Caching**: Store query results locally for instant replay without hitting Trino
- **Efficient Connection Pooling**: Minimize unnecessary reconnects and reuse sessions

### Intuitive UX for SQL Exploration

- **SQL Intelligence**:
  - Rich autocomplete functionality
  - Syntax highlighting
  - Smart query suggestions
- **Interactive Experience**:
  - Multi-line editing support
  - Persistent sessions
  - Query history with search

### Interactive Query Results

- **Rich Output Formats**:
  - Pretty table output (similar to pgcli/mycli)
  - Scrollable, paginated result viewer
- **Visualization**:
  - Terminal-based charts (histograms, line charts, bar charts)
  - Column metadata previews (types, nullability, constraints)

### Local-First Query Experience

- **Caching**:
  - Arrow-powered local result storage
  - Auto-persisted workspaces
  - Offline query capability
- **Performance**:
  - Instant query replay from cache
  - Session state preservation

### DevOps & Automation-Friendly

- **Output Flexibility**:
  - Multiple export formats (CSV, JSON, Arrow, Parquet)
  - Scripting support
- **Environment Management**:
  - Connection profiles for different environments
  - Easy switching between clusters

## 2. Architecture & Components

### Trino Query Engine Wrapper (Backend)

- Built on `trino-go-client`
- Features:
  - Query execution and connection management
  - Automatic retries and error handling
  - Result format conversion (Arrow, JSON, tables)
  - Query caching and history tracking

### Terminal UI (TUI) Interface

- Built with `tview`/`bubbletea`
- Features:
  - Intelligent autocompletion
  - Syntax highlighting via `chroma`/`sqlparser`
  - Interactive table rendering
  - Live terminal-based charting (`glow`/`termui`)

### Local Query Caching Layer

- **Storage**: Apache Arrow IPC format
- **Features**:
  - Query replay without re-execution
  - Offline analysis capabilities
  - Local joins between cached results

### Config & Connection Management

- Configuration via `.trino-cli.yaml`
- Environment profiles (dev, staging, prod)
- Secure credential management

## 3. CLI Commands

### Interactive Mode

```bash
trino-cli
```

Opens interactive query interface

### Batch Mode

```bash
trino-cli -e "SELECT * FROM orders LIMIT 10;"
```

Execute single query and exit

### Profile Selection

```bash
trino-cli --profile prod
```

Connect using specific profile

### History Management

```bash
trino-cli history
```

View query history

### Cache Operations

```bash
# List cached queries
trino-cli cache list

# Replay cached query
trino-cli cache replay query_1234
```

### Data Export

```bash
trino-cli export --format parquet "SELECT * FROM users" > users.parquet
```

Export query results in various formats
