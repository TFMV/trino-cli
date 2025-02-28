package cache

import (
	"errors"
	"fmt"
)

// Note: In a production implementation, query results would be stored in Apache Arrow IPC format on disk.
// For demonstration purposes, we use an in-memory map and slice.

// queryHistory holds the history of executed queries.
var queryHistory = []string{}

// cacheStore maps query IDs to cached result strings.
var cacheStore = map[string]string{}

// GetHistory returns the list of executed queries.
func GetHistory() ([]string, error) {
	// In production, read from a persistent history file or database.
	return queryHistory, nil
}

// AddToHistory appends a query to the history.
func AddToHistory(query string) {
	queryHistory = append(queryHistory, query)
}

// ListCache returns all cached query identifiers.
func ListCache() ([]string, error) {
	keys := []string{}
	for k := range cacheStore {
		keys = append(keys, k)
	}
	return keys, nil
}

// ReplayCache retrieves a cached query result by its query ID.
func ReplayCache(queryID string) (string, error) {
	result, ok := cacheStore[queryID]
	if !ok {
		return "", errors.New("cache entry not found")
	}
	return result, nil
}

// SaveCache stores a query result in the cache.
func SaveCache(queryID string, result string) error {
	if _, exists := cacheStore[queryID]; exists {
		return fmt.Errorf("cache entry already exists")
	}
	cacheStore[queryID] = result
	return nil
}
