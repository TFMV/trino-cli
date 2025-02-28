package schema

import (
	"testing"
)

func TestFuzzySearchEmptyInput(t *testing.T) {
	items := []string{"apple", "banana", "cherry"}
	result := FuzzySearch("", items)

	// Empty input should return all items in original order
	if len(result) != len(items) {
		t.Fatalf("Expected %d items, got %d", len(items), len(result))
	}

	for i, item := range items {
		if result[i] != item {
			t.Fatalf("Expected %s at position %d, got %s", item, i, result[i])
		}
	}
}

func TestFuzzySearchExactMatch(t *testing.T) {
	items := []string{"apple", "banana", "cherry"}
	result := FuzzySearch("banana", items)

	// Exact match should be first
	if len(result) != 1 {
		t.Fatalf("Expected 1 item, got %d", len(result))
	}

	if result[0] != "banana" {
		t.Fatalf("Expected 'banana' as first result, got '%s'", result[0])
	}
}

func TestFuzzySearchCaseInsensitive(t *testing.T) {
	items := []string{"Apple", "Banana", "Cherry"}
	result := FuzzySearch("apple", items)

	// Case-insensitive match should work
	if len(result) != 1 {
		t.Fatalf("Expected 1 item, got %d", len(result))
	}

	if result[0] != "Apple" {
		t.Fatalf("Expected 'Apple' as first result, got '%s'", result[0])
	}
}

func TestFuzzySearchPrefixMatch(t *testing.T) {
	items := []string{"apple", "application", "banana", "cherry"}
	result := FuzzySearch("app", items)

	// Prefix matches should be returned in order
	if len(result) != 2 {
		t.Fatalf("Expected 2 items, got %d", len(result))
	}

	// The order should be preserved based on the original items
	if result[0] != "apple" || result[1] != "application" {
		t.Fatalf("Expected 'apple' and 'application' in that order, got %v", result)
	}
}

func TestFuzzySearchContainsMatch(t *testing.T) {
	items := []string{"apple", "banana", "pineapple", "cherry"}
	result := FuzzySearch("apple", items)

	// Both exact and contains matches should be returned
	if len(result) != 2 {
		t.Fatalf("Expected 2 items, got %d", len(result))
	}

	// Exact match should come before contains match
	if result[0] != "apple" || result[1] != "pineapple" {
		t.Fatalf("Expected 'apple' and 'pineapple' in that order, got %v", result)
	}
}

func TestFuzzySearchSubsequenceMatch(t *testing.T) {
	items := []string{"apple", "banana", "application", "abracadabra"}
	result := FuzzySearch("apa", items)

	// Subsequence matches should be returned
	if len(result) != 1 {
		t.Fatalf("Expected 1 item, got %d", len(result))
	}

	if result[0] != "application" {
		t.Fatalf("Expected 'application' as result, got '%s'", result[0])
	}
}

func TestFuzzySearchNoMatch(t *testing.T) {
	items := []string{"apple", "banana", "cherry"}
	result := FuzzySearch("orange", items)

	// No matches should return empty slice
	if len(result) != 0 {
		t.Fatalf("Expected 0 items, got %d", len(result))
	}
}

func TestFuzzySearchMultipleMatchTypes(t *testing.T) {
	items := []string{
		"apple",       // Exact match for "apple"
		"application", // Prefix match for "app"
		"pineapple",   // Contains match for "apple"
		"apt",         // No match for "apple"
		"a_p_p_l_e",   // Subsequence match for "apple"
	}

	result := FuzzySearch("apple", items)

	// Should return matches in order: exact, prefix, contains, subsequence
	expectedMatches := []string{"apple", "pineapple", "a_p_p_l_e"}
	if len(result) != len(expectedMatches) {
		t.Fatalf("Expected %d items, got %d: %v", len(expectedMatches), len(result), result)
	}

	if result[0] != "apple" {
		t.Fatalf("Expected exact match 'apple' as first result, got '%s'", result[0])
	}

	// Check that pineapple (contains) is in the results
	containsFound := false
	for _, r := range result {
		if r == "pineapple" {
			containsFound = true
			break
		}
	}
	if !containsFound {
		t.Fatalf("Expected 'pineapple' in results, got %v", result)
	}

	// Check that a_p_p_l_e (subsequence) is in the results
	subsequenceFound := false
	for _, r := range result {
		if r == "a_p_p_l_e" {
			subsequenceFound = true
			break
		}
	}
	if !subsequenceFound {
		t.Fatalf("Expected 'a_p_p_l_e' in results, got %v", result)
	}
}

func TestFuzzySearchSorting(t *testing.T) {
	items := []string{
		"zzzapple", // Contains match (late position)
		"appzzzle", // Contains match (early position)
		"apple",    // Exact match
		"apricot",  // Prefix match
	}

	result := FuzzySearch("app", items)

	// Should return matches with appzzzle first (since it contains "app" at the beginning)
	if len(result) < 2 {
		t.Fatalf("Expected at least 2 items, got %d: %v", len(result), result)
	}

	// Check that both appzzzle and apple are in the results
	appleFound := false
	appzzzleFound := false

	for _, item := range result {
		if item == "apple" {
			appleFound = true
		} else if item == "appzzzle" {
			appzzzleFound = true
		}
	}

	// Verify that both items are in the results
	if !appleFound {
		t.Fatalf("Expected 'apple' in results, got %v", result)
	}

	if !appzzzleFound {
		t.Fatalf("Expected 'appzzzle' in results, got %v", result)
	}

	// Verify that appzzzle is the first item (since it contains "app" at position 0)
	if result[0] != "appzzzle" {
		t.Fatalf("Expected 'appzzzle' as first result, got '%s'", result[0])
	}
}
