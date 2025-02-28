package autocomplete

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/TFMV/trino-cli/config"
	_ "github.com/trinodb/trino-go-client/trino"
	"go.uber.org/zap"
)

// StartSchemaCacheUpdater starts a background goroutine that refreshes schema metadata
// at the specified interval for the given profile.
func StartSchemaCacheUpdater(interval time.Duration, profileName string, logger *zap.Logger) error {
	if logger == nil {
		var err error
		logger, err = zap.NewProduction()
		if err != nil {
			return fmt.Errorf("failed to create logger: %w", err)
		}
	}

	log := logger.With(zap.String("component", "schema_updater"), zap.String("profile", profileName))
	log.Info("Starting schema cache updater", zap.Duration("interval", interval))

	// Get database connection for the profile
	dsn := fmt.Sprintf("http://%s@%s:%d?catalog=%s&schema=%s",
		config.AppConfig.Profiles[profileName].User,
		config.AppConfig.Profiles[profileName].Host,
		config.AppConfig.Profiles[profileName].Port,
		config.AppConfig.Profiles[profileName].Catalog,
		config.AppConfig.Profiles[profileName].Schema)

	db, err := sql.Open("trino", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	// Create cache directory
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("failed to get home directory: %w", err)
	}
	cacheDir := fmt.Sprintf("%s/.trino-cli/autocomplete_cache", homeDir)

	// Create schema cache
	cache, err := NewSchemaCache(cacheDir, log)
	if err != nil {
		return fmt.Errorf("failed to create schema cache: %w", err)
	}

	// Create schema introspector
	introspector := NewSchemaIntrospector(db, cache, log)

	// Set refresh interval
	introspector.SetRefreshInterval(interval)

	// Start background refresh
	introspector.StartBackgroundRefresh()

	log.Info("Schema cache updater started successfully")
	return nil
}

// FetchAndCacheSchema fetches schema metadata for the given profile and caches it
func FetchAndCacheSchema(profileName string) error {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	log := logger.With(zap.String("component", "schema_updater"), zap.String("profile", profileName))

	// Get database connection for the profile
	dsn := fmt.Sprintf("http://%s@%s:%d?catalog=%s&schema=%s",
		config.AppConfig.Profiles[profileName].User,
		config.AppConfig.Profiles[profileName].Host,
		config.AppConfig.Profiles[profileName].Port,
		config.AppConfig.Profiles[profileName].Catalog,
		config.AppConfig.Profiles[profileName].Schema)

	db, err := sql.Open("trino", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	// Create cache directory
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("failed to get home directory: %w", err)
	}
	cacheDir := fmt.Sprintf("%s/.trino-cli/autocomplete_cache", homeDir)

	// Create schema cache
	cache, err := NewSchemaCache(cacheDir, log)
	if err != nil {
		return fmt.Errorf("failed to create schema cache: %w", err)
	}
	defer cache.Close()

	// Create schema introspector
	introspector := NewSchemaIntrospector(db, cache, log)

	// Refresh all schemas
	log.Info("Refreshing schema cache...")
	if err := introspector.RefreshAll(); err != nil {
		log.Error("Failed to refresh schema cache", zap.Error(err))
		return fmt.Errorf("failed to refresh schema cache: %w", err)
	}

	log.Info("Schema cache updated successfully")
	return nil
}
