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

func Tokenize(input []byte, minSize, maxSize int) [][]byte {

	if minSize <= 0 {
		log.Fatalf("tokenizer min token size is set to %d", minSize)
	}
	if minSize > maxSize {
		log.Fatalf("tokenizer wrong min/max: %d/%d", minSize, maxSize)
	}

	input = bytes.ToLower(input) // alloc, it forgets the original pointer here
	tokens := split(input)
	tokens = filterShortTokensInPlaceCutLongTokens(tokens, minSize, maxSize)

	return tokens
}

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
