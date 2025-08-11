package tokenizer

import (
	"bytes"
	"log"
	"unicode/utf8"
)

// Tokenizer splits a sequence of bytes in normalized tokens
type Tokenizer func([]byte, int, int) [][]byte

var (
	seps        = " \r\n\t!()-[]{};:`'\"\\,<>./?@#$%^&*_~"
	sepRunesSet = make(map[rune]struct{}, len(seps))
)

func init() {
	for _, r := range seps {
		sepRunesSet[r] = struct{}{}
	}
}

// Tokenize processes a byte slice into normalized tokens with size constraints.
// It converts the input to lowercase and splits it into tokens based on predefined separators.
func Tokenize(input []byte, minSize, maxSize int) [][]byte {

	if minSize <= 0 {
		log.Panicf("tokenizer min token size is set to %d", minSize)
	}
	if minSize > maxSize {
		log.Panicf("tokenizer wrong min/max: %d/%d", minSize, maxSize)
	}

	input = bytes.ToLower(input) // alloc, it forgets the original pointer here
	tokens := split(input)
	tokens = filterShortTokensInPlaceCutLongTokens(tokens, minSize, maxSize)

	return tokens
}

// filterShortTokensInPlaceCutLongTokens filters out tokens with fewer runes than minRunes and truncates tokens
// longer than maxRunes. It operates in-place on the input slice to minimize memory allocations.
// The function processes each token by counting UTF-8 runes and ensures the token length is within
// the specified bounds. Filtered tokens are excluded from the result, and truncated tokens are copied
// to prevent referencing the original data.
func filterShortTokensInPlaceCutLongTokens(tokens [][]byte, minRunes, maxRunes int) [][]byte {
	var x int
	for i := range tokens {
		cut := 0
		runes := 0
		for runes < maxRunes {
			_, rSize := utf8.DecodeRune(tokens[i][cut:])
			if rSize == 0 {
				break
			}
			cut += rSize
			runes++
		}
		if runes < minRunes {
			continue
		}

		// this copies runes, so original source of runes is not referenced
		tokens[x] = append([]byte{}, tokens[i][:cut]...)
		x++
	}

	return append([][]byte{}, tokens[:x]...) // gc: forget filtered tokens (the tail)
}

// split divides a byte slice into tokens using predefined separators.
// It returns nil if the input slice is empty. The function uses bytes.FieldsFunc
// with a separator set defined in sepRunesSet to split the input into tokens.
func split(s []byte) [][]byte {
	if len(s) == 0 {
		return nil
	}

	f := func(r rune) bool {
		_, ok := sepRunesSet[r]
		return ok
	}
	return bytes.FieldsFunc(s, f)
}

// filterDuplicatedTokensInPlaceNoAlloc removes duplicate tokens from the input slice without
// allocating additional memory. It modifies the input slice in-place and returns a new slice
// that shares the same underlying array but only contains unique tokens.
// The function preserves the order of first occurrence of each token.
// After processing, the unused positions in the original slice are set to nil to help with garbage collection.
func filterDuplicatedTokensInPlaceNoAlloc(tokens [][]byte) [][]byte {
	n := 0
MainLoop:
	for _, token := range tokens {
		for j := 0; j < n; j++ {
			if bytes.Equal(token, tokens[j]) {
				continue MainLoop
			}
		}

		tokens[n] = token
		n++
	}

	for i := n; i < len(tokens); i++ {
		tokens[i] = nil // gc
	}

	return tokens[:n]
}
