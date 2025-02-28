package autocomplete

import (
	"sort"
	"strings"
)

// TrieNode represents a node in the prefix tree (trie) for autocompletion
type TrieNode struct {
	Children map[rune]*TrieNode
	IsWord   bool
	Word     string
	Score    int // For ranking suggestions (frequency or recency)
}

// Trie is a prefix tree for fast autocompletion lookups
type Trie struct {
	Root *TrieNode
}

// NewTrie creates a new trie for autocompletion
func NewTrie() *Trie {
	return &Trie{
		Root: &TrieNode{
			Children: make(map[rune]*TrieNode),
		},
	}
}

// Insert adds a word to the trie with an optional score
func (t *Trie) Insert(word string, score int) {
	node := t.Root
	word = strings.ToLower(word)

	for _, char := range word {
		if _, exists := node.Children[char]; !exists {
			node.Children[char] = &TrieNode{
				Children: make(map[rune]*TrieNode),
			}
		}
		node = node.Children[char]
	}

	node.IsWord = true
	node.Word = word
	node.Score += score // Increment rather than replace for dynamic boosting
}

// BoostWord increases the score of a word when it's used
func (t *Trie) BoostWord(word string, boostAmount int) bool {
	node := t.findNode(strings.ToLower(word))
	if node == nil || !node.IsWord {
		return false
	}

	node.Score += boostAmount
	return true
}

// Search finds exact matches for a word in the trie
func (t *Trie) Search(word string) bool {
	node := t.findNode(strings.ToLower(word))
	return node != nil && node.IsWord
}

// findNode locates a node for a given prefix
func (t *Trie) findNode(prefix string) *TrieNode {
	node := t.Root
	prefix = strings.ToLower(prefix)

	for _, char := range prefix {
		if _, exists := node.Children[char]; !exists {
			return nil
		}
		node = node.Children[char]
	}
	return node
}

// GetSuggestions returns a list of suggestions that match the given prefix
func (t *Trie) GetSuggestions(prefix string, limit int) []string {
	node := t.findNode(prefix)
	if node == nil {
		return []string{}
	}

	// Get all possible completions
	suggestions := make([]struct {
		Word  string
		Score int
	}, 0)

	// Collect words starting with this prefix
	var collect func(node *TrieNode)
	collect = func(node *TrieNode) {
		if node.IsWord {
			suggestions = append(suggestions, struct {
				Word  string
				Score int
			}{Word: node.Word, Score: node.Score})
		}

		for _, child := range node.Children {
			collect(child)
		}
	}

	collect(node)

	// Sort by score
	sort.Slice(suggestions, func(i, j int) bool {
		return suggestions[i].Score > suggestions[j].Score
	})

	// Return top N suggestions
	result := make([]string, 0, limit)
	for i := 0; i < len(suggestions) && i < limit; i++ {
		result = append(result, suggestions[i].Word)
	}
	return result
}

// GetFuzzyMatches returns suggestions with fuzzy matching
func (t *Trie) GetFuzzyMatches(prefix string, maxDistance int, limit int) []string {
	prefix = strings.ToLower(prefix)
	matches := make(map[string]int) // word -> score

	// Helper function to recursively traverse the trie and find fuzzy matches
	var traverse func(node *TrieNode, currentPrefix string, currentDistance int)
	traverse = func(node *TrieNode, currentPrefix string, currentDistance int) {
		if currentDistance > maxDistance {
			return
		}

		if node.IsWord {
			// Calculate a score that combines edit distance and node score
			combinedScore := node.Score - currentDistance*10
			matches[node.Word] = combinedScore
		}

		// Try all possible next characters
		for char, childNode := range node.Children {
			// Case 1: Match (use the character)
			nextIndex := len(currentPrefix)
			if nextIndex < len(prefix) && rune(prefix[nextIndex]) == char {
				traverse(childNode, currentPrefix+string(char), currentDistance)
			} else {
				// Case 2: Insert (skip this character in the trie)
				traverse(childNode, currentPrefix, currentDistance+1)

				// Case 3: Substitute (use this character but count as error)
				if nextIndex < len(prefix) {
					traverse(childNode, currentPrefix+string(char), currentDistance+1)
				}
			}
		}

		// Case 4: Delete (skip a character in the input)
		if len(currentPrefix) < len(prefix) {
			traverse(node, currentPrefix+string(prefix[len(currentPrefix)]), currentDistance+1)
		}
	}

	traverse(t.Root, "", 0)

	// Convert map to a sorted slice of suggestions
	type Match struct {
		Word  string
		Score int
	}
	result := make([]Match, 0, len(matches))
	for word, score := range matches {
		result = append(result, Match{Word: word, Score: score})
	}

	// Sort by score (higher is better)
	sort.Slice(result, func(i, j int) bool {
		return result[i].Score > result[j].Score
	})

	// Return top matches
	suggestions := make([]string, 0, limit)
	for i := 0; i < len(result) && i < limit; i++ {
		suggestions = append(suggestions, result[i].Word)
	}
	return suggestions
}
