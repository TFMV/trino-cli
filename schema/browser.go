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
	treeView   *tview.TreeView
	app        *tview.Application
	infoText   *tview.TextView
	db         *sql.DB
	logger     *zap.Logger
	profile    string
	rootNode   *tview.TreeNode
	loadingJob context.CancelFunc
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

	db, err := sql.Open("trino", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	tree := NewSchemaTree()

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
		treeView: treeView,
		infoText: infoText,
		db:       db,
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

	// Create a flex layout
	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(tview.NewTextView().SetText("Trino Schema Browser - Press Esc to exit").SetTextAlign(tview.AlignCenter), 1, 0, false).
		AddItem(tview.NewFlex().
			AddItem(b.treeView, 0, 3, true).
			AddItem(b.infoText, 0, 5, false),
			0, 1, true)

	// Load catalogs in the background after starting the UI
	go func() {
		if err := b.LoadCatalogs(); err != nil {
			b.logger.Error("Failed to load catalogs", zap.Error(err))
			b.infoText.SetText(fmt.Sprintf("[red]Error loading catalogs: %v[white]", err))
		}
	}()

	// Capture keyboard events
	b.app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEscape {
			b.app.Stop()
			return nil
		}
		return event
	})

	// Run the application
	if err := b.app.SetRoot(flex, true).Run(); err != nil {
		return err
	}

	// Close the database connection when the application exits
	b.db.Close()
	return nil
}

// LoadCatalogs loads the catalogs from Trino
func (b *Browser) LoadCatalogs() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := b.db.QueryContext(ctx, "SHOW CATALOGS")
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
	rows, err := b.db.QueryContext(ctx, query)
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
	rows, err := b.db.QueryContext(ctx, query)
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
	rows, err := b.db.QueryContext(ctx, query)
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
