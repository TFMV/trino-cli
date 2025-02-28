package autocomplete

import (
	"testing"
)

func TestTrieBoostWord(t *testing.T) {
	// Create a new trie
	trie := NewTrie()

	// Insert some words with initial scores
	trie.Insert("select", 5)
	trie.Insert("from", 3)
	trie.Insert("where", 2)

	// Test initial scores
	selectNode := trie.findNode("select")
	if selectNode == nil || selectNode.Score != 5 {
		t.Errorf("Expected 'select' to have score 5, got %v", selectNode.Score)
	}

	// Boost a word
	success := trie.BoostWord("select", 3)
	if !success {
		t.Errorf("BoostWord returned false for existing word")
	}

	// Verify the score was boosted
	selectNode = trie.findNode("select")
	if selectNode == nil || selectNode.Score != 8 {
		t.Errorf("Expected 'select' to have score 8 after boosting, got %v", selectNode.Score)
	}

	// Try to boost a non-existent word
	success = trie.BoostWord("nonexistent", 5)
	if success {
		t.Errorf("BoostWord returned true for non-existent word")
	}

	// Test case insensitivity
	success = trie.BoostWord("SELECT", 2)
	if !success {
		t.Errorf("BoostWord failed for case-insensitive match")
	}

	selectNode = trie.findNode("select")
	if selectNode == nil || selectNode.Score != 10 {
		t.Errorf("Expected 'select' to have score 10 after case-insensitive boost, got %v", selectNode.Score)
	}

	// Test suggestions ordering based on score
	suggestions := trie.GetSuggestions("s", 10)
	if len(suggestions) != 1 || suggestions[0] != "select" {
		t.Errorf("Expected 'select' as first suggestion, got %+v", suggestions)
	}

	// Add another word starting with 's' but lower score
	trie.Insert("sum", 4)

	// Verify ordering is by score
	suggestions = trie.GetSuggestions("s", 10)
	if len(suggestions) != 2 {
		t.Errorf("Expected 2 suggestions, got %d", len(suggestions))
	} else if suggestions[0] != "select" || suggestions[1] != "sum" {
		t.Errorf("Expected suggestions ordered by score: ['select', 'sum'], got [%s, %s]",
			suggestions[0], suggestions[1])
	}

	// Boost 'sum' above 'select'
	trie.BoostWord("sum", 7) // sum: 4+7=11, select: 10

	// Verify the new ordering
	suggestions = trie.GetSuggestions("s", 10)
	if len(suggestions) != 2 {
		t.Errorf("Expected 2 suggestions, got %d", len(suggestions))
	} else if suggestions[0] != "sum" || suggestions[1] != "select" {
		t.Errorf("Expected suggestions ordered by score: ['sum', 'select'], got [%s, %s]",
			suggestions[0], suggestions[1])
	}
}
