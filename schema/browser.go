package schema

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/TFMV/trino-cli/config"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"go.uber.org/zap"

	_ "github.com/trinodb/trino-go-client/trino"
)

// SchemaTree represents the structure of the Trino schema
type SchemaTree struct {
	Catalogs map[string]bool
	Schemas  map[string]map[string]bool
	Tables   map[string]map[string]map[string]bool
	Columns  map[string]map[string]map[string][]Column
	mu       sync.RWMutex
}

// Column represents a column in a table
type Column struct {
	Name     string
	Type     string
	Nullable bool
}

// NewSchemaTree creates a new schema tree
func NewSchemaTree() *SchemaTree {
	return &SchemaTree{
		Catalogs: make(map[string]bool),
		Schemas:  make(map[string]map[string]bool),
		Tables:   make(map[string]map[string]map[string]bool),
		Columns:  make(map[string]map[string]map[string][]Column),
	}
}

// SchemaCache provides caching capabilities for schema metadata
type SchemaCache struct {
	Data   *SchemaTree
	Expiry time.Time
	mu     sync.RWMutex
}

// NewSchemaCache creates a new schema cache
func NewSchemaCache() *SchemaCache {
	return &SchemaCache{
		Data:   NewSchemaTree(),
		Expiry: time.Now(),
	}
}

// Get returns the cached schema tree if it's still valid, otherwise nil
func (sc *SchemaCache) Get() *SchemaTree {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	if time.Now().Before(sc.Expiry) {
		return sc.Data
	}
	return nil
}

// Update updates the schema cache with new data and sets an expiration time
func (sc *SchemaCache) Update(tree *SchemaTree, duration time.Duration) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.Data = tree
	sc.Expiry = time.Now().Add(duration)
}

// HasCatalog checks if a catalog exists in the cache
func (sc *SchemaCache) HasCatalog(catalog string) bool {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	if sc.Data == nil {
		return false
	}
	_, ok := sc.Data.Catalogs[catalog]
	return ok && time.Now().Before(sc.Expiry)
}

// HasSchema checks if a schema exists in the cache
func (sc *SchemaCache) HasSchema(catalog, schema string) bool {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	if sc.Data == nil {
		return false
	}
	if schemas, ok := sc.Data.Schemas[catalog]; ok {
		_, ok := schemas[schema]
		return ok && time.Now().Before(sc.Expiry)
	}
	return false
}

// HasTable checks if a table exists in the cache
func (sc *SchemaCache) HasTable(catalog, schema, table string) bool {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	if sc.Data == nil {
		return false
	}
	if schemas, ok := sc.Data.Tables[catalog]; ok {
		if tables, ok := schemas[schema]; ok {
			_, ok := tables[table]
			return ok && time.Now().Before(sc.Expiry)
		}
	}
	return false
}

// GetCatalogs returns all catalogs from the cache
func (sc *SchemaCache) GetCatalogs() []string {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	if sc.Data == nil || time.Now().After(sc.Expiry) {
		return nil
	}

	catalogs := make([]string, 0, len(sc.Data.Catalogs))
	for catalog := range sc.Data.Catalogs {
		catalogs = append(catalogs, catalog)
	}
	sort.Strings(catalogs)
	return catalogs
}

// GetSchemas returns all schemas for a catalog from the cache
func (sc *SchemaCache) GetSchemas(catalog string) []string {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	if sc.Data == nil || time.Now().After(sc.Expiry) {
		return nil
	}

	if schemas, ok := sc.Data.Schemas[catalog]; ok {
		result := make([]string, 0, len(schemas))
		for schema := range schemas {
			result = append(result, schema)
		}
		sort.Strings(result)
		return result
	}
	return nil
}

// GetTables returns all tables for a schema from the cache
func (sc *SchemaCache) GetTables(catalog, schema string) []string {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	if sc.Data == nil || time.Now().After(sc.Expiry) {
		return nil
	}

	if schemas, ok := sc.Data.Tables[catalog]; ok {
		if tables, ok := schemas[schema]; ok {
			result := make([]string, 0, len(tables))
			for table := range tables {
				result = append(result, table)
			}
			sort.Strings(result)
			return result
		}
	}
	return nil
}

// GetColumns returns all columns for a table from the cache
func (sc *SchemaCache) GetColumns(catalog, schema, table string) []Column {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	if sc.Data == nil || time.Now().After(sc.Expiry) {
		return nil
	}

	if schemas, ok := sc.Data.Columns[catalog]; ok {
		if tables, ok := schemas[schema]; ok {
			return tables[table]
		}
	}
	return nil
}

// SchemaTreeNode represents a node in the tview tree
type SchemaTreeNode struct {
	Type     string // "catalog", "schema", "table", "column"
	Name     string
	Catalog  string
	Schema   string
	Table    string
	DataType string // for columns
	Loaded   bool
}

// Browser manages the interactive schema browser
type Browser struct {
	tree       *SchemaTree
	cache      *SchemaCache
	treeView   *tview.TreeView
	app        *tview.Application
	infoText   *tview.TextView
	db         *sql.DB
	logger     *zap.Logger
	profile    string
	rootNode   *tview.TreeNode
	loadingJob context.CancelFunc
	dbPool     *sql.DB // Connection pool for better performance
}

// NewBrowser creates a new schema browser
func NewBrowser(profileName string, logger *zap.Logger) (*Browser, error) {
	if logger == nil {
		var err error
		logger, err = zap.NewProduction()
		if err != nil {
			return nil, fmt.Errorf("failed to create logger: %w", err)
		}
	}

	// Get database connection
	profile := config.AppConfig.Profiles[profileName]
	if profile.Host == "" {
		return nil, fmt.Errorf("profile %s not found", profileName)
	}

	dsn := fmt.Sprintf("http://%s@%s:%d?catalog=%s&schema=%s",
		profile.User,
		profile.Host,
		profile.Port,
		profile.Catalog,
		profile.Schema)

	// Create a connection pool instead of a single connection
	db, err := sql.Open("trino", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Configure connection pooling
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	tree := NewSchemaTree()
	cache := NewSchemaCache()

	// Set up the tree view
	rootNode := tview.NewTreeNode("Trino Schema").
		SetColor(tcell.ColorGreen).
		SetSelectable(false)

	treeView := tview.NewTreeView().
		SetRoot(rootNode).
		SetCurrentNode(rootNode)

	// Set up info text view
	infoText := tview.NewTextView().
		SetDynamicColors(true).
		SetRegions(true).
		SetWordWrap(true).
		SetTextAlign(tview.AlignLeft)
	infoText.SetText("Welcome to the Schema Browser. Navigate the tree to explore your Trino schema.")

	browser := &Browser{
		tree:     tree,
		cache:    cache,
		treeView: treeView,
		infoText: infoText,
		db:       db,
		dbPool:   db,
		logger:   logger,
		profile:  profileName,
		rootNode: rootNode,
	}

	// Set up the node selection handler
	treeView.SetSelectedFunc(browser.nodeSelected)
	treeView.SetChangedFunc(browser.nodeChanged)

	return browser, nil
}

// Start starts the schema browser
func (b *Browser) Start() error {
	// Create a new application
	b.app = tview.NewApplication()

	// Set up title bar
	titleBar := tview.NewTextView().
		SetText("Trino Schema Browser - Press Esc to exit").
		SetTextAlign(tview.AlignCenter).
		SetTextColor(tcell.ColorWhite)

	// Add borders for better UI
	b.treeView.SetBorder(true).
		SetTitle(" Schema Explorer ").
		SetTitleAlign(tview.AlignLeft).
		SetTitleColor(tcell.ColorGreen)

	b.infoText.SetBorder(true).
		SetTitle(" Info ").
		SetTitleAlign(tview.AlignLeft).
		SetTitleColor(tcell.ColorBlue)

	// Create a flex layout for the main content area
	contentFlex := tview.NewFlex().
		AddItem(b.treeView, 0, 3, true).
		AddItem(b.infoText, 0, 5, false)

	// Create a search field
	searchField := tview.NewInputField().
		SetLabel("Search: ").
		SetFieldWidth(30).
		SetDoneFunc(func(key tcell.Key) {
			// Return focus to the tree view when done
			b.app.SetFocus(b.treeView)
		})

	// Add behavior to search field
	searchField.SetChangedFunc(func(text string) {
		node := b.treeView.GetCurrentNode()
		if node == nil {
			return
		}

		nodeRef := node.GetReference()
		if nodeRef == nil {
			return
		}

		ref := nodeRef.(*SchemaTreeNode)

		// Handle different node types
		switch ref.Type {
		case "catalog":
			// If we have schemas loaded, search through them
			schemas := b.cache.GetSchemas(ref.Catalog)
			if schemas != nil {
				matchedSchemas := FuzzySearch(text, schemas)

				b.app.QueueUpdateDraw(func() {
					node.ClearChildren()
					for _, schema := range matchedSchemas {
						schemaNode := tview.NewTreeNode(schema).
							SetReference(&SchemaTreeNode{
								Type:    "schema",
								Name:    schema,
								Catalog: ref.Catalog,
								Schema:  schema,
								Loaded:  false,
							}).
							SetSelectable(true).
							SetColor(tcell.ColorLightBlue)
						node.AddChild(schemaNode)
					}
				})
			}
		case "schema":
			// Search tables in this schema
			tables := b.cache.GetTables(ref.Catalog, ref.Schema)
			if tables != nil {
				matchedTables := FuzzySearch(text, tables)

				b.app.QueueUpdateDraw(func() {
					node.ClearChildren()
					for _, table := range matchedTables {
						tableNode := tview.NewTreeNode(table).
							SetReference(&SchemaTreeNode{
								Type:    "table",
								Name:    table,
								Catalog: ref.Catalog,
								Schema:  ref.Schema,
								Table:   table,
								Loaded:  false,
							}).
							SetSelectable(true).
							SetColor(tcell.ColorLightCyan)
						node.AddChild(tableNode)
					}
				})
			}
		case "table":
			// If we have columns loaded, search through them
			columns := b.cache.GetColumns(ref.Catalog, ref.Schema, ref.Table)
			if columns != nil {
				// Extract column names for searching
				columnNames := make([]string, len(columns))
				for i, col := range columns {
					columnNames[i] = col.Name
				}

				matchedNames := FuzzySearch(text, columnNames)

				// Find the corresponding Column objects
				var matchedColumns []Column
				for _, name := range matchedNames {
					for _, col := range columns {
						if col.Name == name {
							matchedColumns = append(matchedColumns, col)
							break
						}
					}
				}

				b.app.QueueUpdateDraw(func() {
					node.ClearChildren()
					for _, col := range matchedColumns {
						colNode := tview.NewTreeNode(fmt.Sprintf("%s (%s)", col.Name, col.Type)).
							SetReference(&SchemaTreeNode{
								Type:     "column",
								Name:     col.Name,
								Catalog:  ref.Catalog,
								Schema:   ref.Schema,
								Table:    ref.Table,
								DataType: col.Type,
							}).
							SetSelectable(true).
							SetColor(tcell.ColorWhite)
						node.AddChild(colNode)
					}
				})
			}
		}
	})

	// Add search field to a flex container
	searchFlex := tview.NewFlex().
		AddItem(nil, 0, 1, false).
		AddItem(searchField, 30, 1, false).
		AddItem(nil, 0, 1, false)

	// Create the main flex layout
	mainFlex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(titleBar, 1, 0, false).
		AddItem(searchFlex, 1, 0, false).
		AddItem(contentFlex, 0, 1, true)

	// Load catalogs in the background after starting the UI
	go func() {
		if err := b.LoadCatalogs(); err != nil {
			b.logger.Error("Failed to load catalogs", zap.Error(err))
			b.infoText.SetText(fmt.Sprintf("[red]Error loading catalogs: %v[white]", err))
		}
	}()

	// Set keyboard shortcuts
	b.app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyEscape:
			if b.treeView.HasFocus() {
				// If the tree has focus, exit the application
				b.app.Stop()
				return nil
			} else {
				// Otherwise, return focus to the tree
				b.app.SetFocus(b.treeView)
				return nil
			}
		case tcell.KeyCtrlF, tcell.KeyF3:
			// Focus the search field
			b.app.SetFocus(searchField)
			return nil
		}
		return event
	})

	// Run the application
	if err := b.app.SetRoot(mainFlex, true).Run(); err != nil {
		return err
	}

	// Close the database connection when the application exits
	b.db.Close()
	return nil
}

// LoadCatalogs loads the catalogs from Trino
func (b *Browser) LoadCatalogs() error {
	// Check if we have this in cache
	if cachedCatalogs := b.cache.GetCatalogs(); cachedCatalogs != nil {
		b.logger.Info("Using cached catalogs")
		b.app.QueueUpdateDraw(func() {
			for _, catalog := range cachedCatalogs {
				node := tview.NewTreeNode(catalog).
					SetReference(&SchemaTreeNode{
						Type:    "catalog",
						Name:    catalog,
						Catalog: catalog,
						Loaded:  false,
					}).
					SetSelectable(true).
					SetColor(tcell.ColorYellow)
				b.rootNode.AddChild(node)
			}
		})
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := b.dbPool.QueryContext(ctx, "SHOW CATALOGS")
	if err != nil {
		return fmt.Errorf("failed to query catalogs: %w", err)
	}
	defer rows.Close()

	var catalogs []string
	for rows.Next() {
		var catalog string
		if err := rows.Scan(&catalog); err != nil {
			return fmt.Errorf("failed to scan catalog: %w", err)
		}
		catalogs = append(catalogs, catalog)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating catalogs: %w", err)
	}

	// Sort catalogs alphabetically
	sort.Strings(catalogs)

	// Add catalogs to the tree
	b.tree.mu.Lock()
	for _, catalog := range catalogs {
		b.tree.Catalogs[catalog] = true
	}
	b.tree.mu.Unlock()

	// Update the cache
	b.cache.Update(b.tree, 5*time.Minute)

	// Update the UI on the main thread
	b.app.QueueUpdateDraw(func() {
		for _, catalog := range catalogs {
			node := tview.NewTreeNode(catalog).
				SetReference(&SchemaTreeNode{
					Type:    "catalog",
					Name:    catalog,
					Catalog: catalog,
					Loaded:  false,
				}).
				SetSelectable(true).
				SetColor(tcell.ColorYellow)
			b.rootNode.AddChild(node)
		}
	})

	return nil
}

// LoadSchemas loads the schemas for a catalog
func (b *Browser) LoadSchemas(catalog string, node *tview.TreeNode) error {
	// Check if we have this in cache
	if cachedSchemas := b.cache.GetSchemas(catalog); cachedSchemas != nil {
		b.logger.Info("Using cached schemas", zap.String("catalog", catalog))
		b.app.QueueUpdateDraw(func() {
			node.ClearChildren()
			for _, schema := range cachedSchemas {
				schemaNode := tview.NewTreeNode(schema).
					SetReference(&SchemaTreeNode{
						Type:    "schema",
						Name:    schema,
						Catalog: catalog,
						Schema:  schema,
						Loaded:  false,
					}).
					SetSelectable(true).
					SetColor(tcell.ColorLightBlue)
				node.AddChild(schemaNode)
			}
			nodeRef := node.GetReference().(*SchemaTreeNode)
			nodeRef.Loaded = true
		})
		return nil
	}

	// Cancel any previous loading job
	if b.loadingJob != nil {
		b.loadingJob()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	b.loadingJob = cancel
	defer cancel()

	// Show loading indicator
	b.app.QueueUpdateDraw(func() {
		node.SetText(catalog + " (loading...)")
	})

	query := fmt.Sprintf("SHOW SCHEMAS FROM %s", catalog)
	rows, err := b.dbPool.QueryContext(ctx, query)
	if err != nil {
		b.app.QueueUpdateDraw(func() {
			node.SetText(catalog)
			b.infoText.SetText(fmt.Sprintf("[red]Error loading schemas: %v[white]", err))
		})
		return fmt.Errorf("failed to query schemas: %w", err)
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var schema string
		if err := rows.Scan(&schema); err != nil {
			b.app.QueueUpdateDraw(func() {
				node.SetText(catalog)
				b.infoText.SetText(fmt.Sprintf("[red]Error loading schemas: %v[white]", err))
			})
			return fmt.Errorf("failed to scan schema: %w", err)
		}
		schemas = append(schemas, schema)
	}

	if err := rows.Err(); err != nil {
		b.app.QueueUpdateDraw(func() {
			node.SetText(catalog)
			b.infoText.SetText(fmt.Sprintf("[red]Error loading schemas: %v[white]", err))
		})
		return fmt.Errorf("error iterating schemas: %w", err)
	}

	// Sort schemas alphabetically
	sort.Strings(schemas)

	// Add schemas to the tree
	b.tree.mu.Lock()
	if _, ok := b.tree.Schemas[catalog]; !ok {
		b.tree.Schemas[catalog] = make(map[string]bool)
	}
	for _, schema := range schemas {
		b.tree.Schemas[catalog][schema] = true
	}
	b.tree.mu.Unlock()

	// Update the cache
	b.cache.Update(b.tree, 5*time.Minute)

	// Update the UI on the main thread
	b.app.QueueUpdateDraw(func() {
		node.ClearChildren()
		node.SetText(catalog)
		for _, schema := range schemas {
			schemaNode := tview.NewTreeNode(schema).
				SetReference(&SchemaTreeNode{
					Type:    "schema",
					Name:    schema,
					Catalog: catalog,
					Schema:  schema,
					Loaded:  false,
				}).
				SetSelectable(true).
				SetColor(tcell.ColorLightBlue)
			node.AddChild(schemaNode)
		}
		nodeRef := node.GetReference().(*SchemaTreeNode)
		nodeRef.Loaded = true
	})

	return nil
}

// LoadTables loads the tables for a schema
func (b *Browser) LoadTables(catalog, schema string, node *tview.TreeNode) error {
	// Check if we have this in cache
	if cachedTables := b.cache.GetTables(catalog, schema); cachedTables != nil {
		b.logger.Info("Using cached tables",
			zap.String("catalog", catalog),
			zap.String("schema", schema))
		b.app.QueueUpdateDraw(func() {
			node.ClearChildren()
			for _, table := range cachedTables {
				tableNode := tview.NewTreeNode(table).
					SetReference(&SchemaTreeNode{
						Type:    "table",
						Name:    table,
						Catalog: catalog,
						Schema:  schema,
						Table:   table,
						Loaded:  false,
					}).
					SetSelectable(true).
					SetColor(tcell.ColorLightCyan)
				node.AddChild(tableNode)
			}
			nodeRef := node.GetReference().(*SchemaTreeNode)
			nodeRef.Loaded = true
		})
		return nil
	}

	// Cancel any previous loading job
	if b.loadingJob != nil {
		b.loadingJob()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	b.loadingJob = cancel
	defer cancel()

	// Show loading indicator
	b.app.QueueUpdateDraw(func() {
		node.SetText(schema + " (loading...)")
	})

	query := fmt.Sprintf("SHOW TABLES FROM %s.%s", catalog, schema)
	rows, err := b.dbPool.QueryContext(ctx, query)
	if err != nil {
		b.app.QueueUpdateDraw(func() {
			node.SetText(schema)
			b.infoText.SetText(fmt.Sprintf("[red]Error loading tables: %v[white]", err))
		})
		return fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			b.app.QueueUpdateDraw(func() {
				node.SetText(schema)
				b.infoText.SetText(fmt.Sprintf("[red]Error loading tables: %v[white]", err))
			})
			return fmt.Errorf("failed to scan table: %w", err)
		}
		tables = append(tables, table)
	}

	if err := rows.Err(); err != nil {
		b.app.QueueUpdateDraw(func() {
			node.SetText(schema)
			b.infoText.SetText(fmt.Sprintf("[red]Error loading tables: %v[white]", err))
		})
		return fmt.Errorf("error iterating tables: %w", err)
	}

	// Sort tables alphabetically
	sort.Strings(tables)

	// Add tables to the tree
	b.tree.mu.Lock()
	if _, ok := b.tree.Tables[catalog]; !ok {
		b.tree.Tables[catalog] = make(map[string]map[string]bool)
	}
	if _, ok := b.tree.Tables[catalog][schema]; !ok {
		b.tree.Tables[catalog][schema] = make(map[string]bool)
	}
	for _, table := range tables {
		b.tree.Tables[catalog][schema][table] = true
	}
	b.tree.mu.Unlock()

	// Update the cache
	b.cache.Update(b.tree, 5*time.Minute)

	// Update the UI on the main thread
	b.app.QueueUpdateDraw(func() {
		node.ClearChildren()
		node.SetText(schema)
		for _, table := range tables {
			tableNode := tview.NewTreeNode(table).
				SetReference(&SchemaTreeNode{
					Type:    "table",
					Name:    table,
					Catalog: catalog,
					Schema:  schema,
					Table:   table,
					Loaded:  false,
				}).
				SetSelectable(true).
				SetColor(tcell.ColorLightCyan)
			node.AddChild(tableNode)
		}
		nodeRef := node.GetReference().(*SchemaTreeNode)
		nodeRef.Loaded = true
	})

	return nil
}

// LoadColumns loads the columns for a table
func (b *Browser) LoadColumns(catalog, schema, table string, node *tview.TreeNode) error {
	// Check if we have this in cache
	if cachedColumns := b.cache.GetColumns(catalog, schema, table); cachedColumns != nil {
		b.logger.Info("Using cached columns",
			zap.String("catalog", catalog),
			zap.String("schema", schema),
			zap.String("table", table))
		b.app.QueueUpdateDraw(func() {
			node.ClearChildren()
			for _, col := range cachedColumns {
				colNode := tview.NewTreeNode(fmt.Sprintf("%s (%s)", col.Name, col.Type)).
					SetReference(&SchemaTreeNode{
						Type:     "column",
						Name:     col.Name,
						Catalog:  catalog,
						Schema:   schema,
						Table:    table,
						DataType: col.Type,
					}).
					SetSelectable(true).
					SetColor(tcell.ColorWhite)
				node.AddChild(colNode)
			}
			nodeRef := node.GetReference().(*SchemaTreeNode)
			nodeRef.Loaded = true
		})
		return nil
	}

	// Cancel any previous loading job
	if b.loadingJob != nil {
		b.loadingJob()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	b.loadingJob = cancel
	defer cancel()

	// Show loading indicator
	b.app.QueueUpdateDraw(func() {
		node.SetText(table + " (loading...)")
	})

	query := fmt.Sprintf("DESCRIBE %s.%s.%s", catalog, schema, table)
	rows, err := b.dbPool.QueryContext(ctx, query)
	if err != nil {
		b.app.QueueUpdateDraw(func() {
			node.SetText(table)
			b.infoText.SetText(fmt.Sprintf("[red]Error loading columns: %v[white]", err))
		})
		return fmt.Errorf("failed to query columns: %w", err)
	}
	defer rows.Close()

	var columns []Column
	for rows.Next() {
		var col Column
		var extraInfo string
		if err := rows.Scan(&col.Name, &col.Type, &extraInfo); err != nil {
			b.app.QueueUpdateDraw(func() {
				node.SetText(table)
				b.infoText.SetText(fmt.Sprintf("[red]Error loading columns: %v[white]", err))
			})
			return fmt.Errorf("failed to scan column: %w", err)
		}
		col.Nullable = !strings.Contains(extraInfo, "not null")
		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		b.app.QueueUpdateDraw(func() {
			node.SetText(table)
			b.infoText.SetText(fmt.Sprintf("[red]Error loading columns: %v[white]", err))
		})
		return fmt.Errorf("error iterating columns: %w", err)
	}

	// Add columns to the tree
	b.tree.mu.Lock()
	if _, ok := b.tree.Columns[catalog]; !ok {
		b.tree.Columns[catalog] = make(map[string]map[string][]Column)
	}
	if _, ok := b.tree.Columns[catalog][schema]; !ok {
		b.tree.Columns[catalog][schema] = make(map[string][]Column)
	}
	b.tree.Columns[catalog][schema][table] = columns
	b.tree.mu.Unlock()

	// Update the cache
	b.cache.Update(b.tree, 5*time.Minute)

	// Update the UI on the main thread
	b.app.QueueUpdateDraw(func() {
		node.ClearChildren()
		node.SetText(table)
		for _, col := range columns {
			colNode := tview.NewTreeNode(fmt.Sprintf("%s (%s)", col.Name, col.Type)).
				SetReference(&SchemaTreeNode{
					Type:     "column",
					Name:     col.Name,
					Catalog:  catalog,
					Schema:   schema,
					Table:    table,
					DataType: col.Type,
				}).
				SetSelectable(true).
				SetColor(tcell.ColorWhite)
			node.AddChild(colNode)
		}
		nodeRef := node.GetReference().(*SchemaTreeNode)
		nodeRef.Loaded = true
	})

	return nil
}

// nodeSelected is called when a node is selected
func (b *Browser) nodeSelected(node *tview.TreeNode) {
	nodeRef := node.GetReference()
	if nodeRef == nil {
		return
	}

	ref := nodeRef.(*SchemaTreeNode)
	switch ref.Type {
	case "catalog":
		if !ref.Loaded {
			go func() {
				if err := b.LoadSchemas(ref.Catalog, node); err != nil {
					b.logger.Error("Failed to load schemas", zap.Error(err), zap.String("catalog", ref.Catalog))
				}
			}()
		} else {
			node.SetExpanded(!node.IsExpanded())
		}
	case "schema":
		if !ref.Loaded {
			go func() {
				if err := b.LoadTables(ref.Catalog, ref.Schema, node); err != nil {
					b.logger.Error("Failed to load tables", zap.Error(err),
						zap.String("catalog", ref.Catalog),
						zap.String("schema", ref.Schema))
				}
			}()
		} else {
			node.SetExpanded(!node.IsExpanded())
		}
	case "table":
		if !ref.Loaded {
			go func() {
				if err := b.LoadColumns(ref.Catalog, ref.Schema, ref.Table, node); err != nil {
					b.logger.Error("Failed to load columns", zap.Error(err),
						zap.String("catalog", ref.Catalog),
						zap.String("schema", ref.Schema),
						zap.String("table", ref.Table))
				}
			}()
		} else {
			node.SetExpanded(!node.IsExpanded())
		}
	case "column":
		// Columns don't have children, just show info
		b.infoText.SetText(fmt.Sprintf("[green]Column:[white] %s\n[green]Type:[white] %s\n[green]Table:[white] %s.%s.%s",
			ref.Name, ref.DataType, ref.Catalog, ref.Schema, ref.Table))
	}
}

// nodeChanged is called when the selected node changes
func (b *Browser) nodeChanged(node *tview.TreeNode) {
	nodeRef := node.GetReference()
	if nodeRef == nil {
		return
	}

	ref := nodeRef.(*SchemaTreeNode)
	switch ref.Type {
	case "catalog":
		b.infoText.SetText(fmt.Sprintf("[green]Catalog:[white] %s\n\nPress Enter to view schemas.", ref.Name))
	case "schema":
		b.infoText.SetText(fmt.Sprintf("[green]Schema:[white] %s\n[green]Catalog:[white] %s\n\nPress Enter to view tables.",
			ref.Schema, ref.Catalog))
	case "table":
		b.infoText.SetText(fmt.Sprintf("[green]Table:[white] %s\n[green]Schema:[white] %s\n[green]Catalog:[white] %s\n\nPress Enter to view columns.",
			ref.Table, ref.Schema, ref.Catalog))
	case "column":
		b.infoText.SetText(fmt.Sprintf("[green]Column:[white] %s\n[green]Type:[white] %s\n[green]Table:[white] %s.%s.%s",
			ref.Name, ref.DataType, ref.Catalog, ref.Schema, ref.Table))
	}
}

// FuzzySearch implements fuzzy matching to quickly find items in a list
func FuzzySearch(input string, items []string) []string {
	if input == "" {
		return items
	}

	// Convert input to lowercase for case-insensitive matching
	lowerInput := strings.ToLower(input)

	// Score each item based on similarity to input
	type scoredItem struct {
		index     int
		score     int
		matchType string // For debugging
	}

	var scored []scoredItem
	for i, item := range items {
		lowerItem := strings.ToLower(item)

		// Simple scoring algorithm - the lower the score, the better the match
		if lowerItem == lowerInput { // Exact match
			scored = append(scored, scoredItem{i, 0, "exact"})
		} else if strings.HasPrefix(lowerItem, lowerInput) { // Prefix match
			scored = append(scored, scoredItem{i, 1, "prefix"})
		} else if strings.Contains(lowerItem, lowerInput) { // Contains match
			// Increase the score for contains matches to ensure they come after prefix matches
			scored = append(scored, scoredItem{i, 100 + strings.Index(lowerItem, lowerInput), "contains"})
		} else if lowerInput != "" {
			// Check for subsequence match (characters in the same order but not consecutive)
			match := true
			lastPos := -1
			for _, c := range lowerInput {
				pos := strings.IndexRune(lowerItem[lastPos+1:], c)
				if pos == -1 {
					match = false
					break
				}
				lastPos += pos + 1
			}

			if match {
				scored = append(scored, scoredItem{i, 1000 + lastPos, "subsequence"}) // Subsequence match, lowest priority
			}
		}
	}

	// Sort by score (lower is better)
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].score < scored[j].score
	})

	// Extract the original items in sorted order
	result := make([]string, 0, len(scored))
	for _, s := range scored {
		result = append(result, items[s.index])
	}

	return result
}
