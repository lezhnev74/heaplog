package tokenizer

import (
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
	"heaplog/common"
	"log"
	"regexp"
	"strings"
	"unicode"
)

var (
	removeSymbols      = regexp.MustCompile(`[^\p{L}\p{N}]+`)
	removeDoubleSpaces = regexp.MustCompile(`\s{2,}`)
)

// Tokenize normalizes the input string (punctuations, diacritics,...)
// Splits by spaces and produces tokens of sizes [minSize,maxSize]
// if a string piece is less than minSize -> no token is generated
func Tokenize(input string, minSize, maxSize int) []string {

	if minSize <= 0 {
		log.Fatalf("tokenizer min token size is set to %d", minSize)
	}

	// 0. Normalize
	input = strings.ToLower(input)
	input = removeDiacritics(input)

	// 1. Remove anything but letters and numbers
	input = removeSymbols.ReplaceAllString(input, " ")

	// 2. Remove multiple space-like characters
	input = removeDoubleSpaces.ReplaceAllString(input, " ")

	tokens := strings.Split(input, " ")

	// 3. Filters
	tokens = filterShortTokensInPlace(tokens, minSize)
	tokens = cutLongTokens(tokens, maxSize)
	tokens = filterDuplicatedTokensInPlace(tokens)

	return tokens
}

// TokenizeF works as Tokenize but do not use regexp and thus does not remove diacritics and is less "picky".
// It only uses string manipulations to greatly reduce the cost of the call.
func TokenizeF(input string, minSize, maxSize int) []string {

	if minSize <= 0 {
		log.Fatalf("tokenizer min token size is set to %d", minSize)
	}

	input = strings.ToLower(input)
	tokens := splitString(input, "\r\n!()-[]{};:'\"\\,<>./?@#$%^&*_~ ")

	tokens = filterShortTokensInPlace(tokens, minSize)
	tokens = cutLongTokens(tokens, maxSize)
	tokens = filterDuplicatedTokensInPlace(tokens)

	return tokens
}

func splitString(s string, separators string) []string {
	f := func(r rune) bool {
		for _, s := range separators {
			if r == s {
				return true
			}
		}
		return false
	}
	return strings.FieldsFunc(s, f)

}

func removeDiacritics(input string) string {
	// note: https://pinyin.info/unicode/diacritics.html

	// Normalize the input string using NFC (Normalization Form C).
	input = norm.NFC.String(input)

	// Create a transformer that removes diacritic symbols.
	t := transform.Chain(norm.NFD, transform.RemoveFunc(func(r rune) bool {
		return unicode.IsOneOf([]*unicode.RangeTable{
			unicode.Mn, // Mn: Nonspacing marks (diacritics)
		}, r)
	}), norm.NFC)

	// Apply the transformer to remove diacritic symbols.
	result, _, err := transform.String(t, input)
	if err != nil {
		log.Fatalf("unable to remove diacritics: %s", err)
	}

	return result
}

func filterDuplicatedTokensInPlace(tokens []string) []string {
	found := map[string]int{}
	for i, token := range tokens {
		if _, ok := found[token]; !ok {
			found[token] = i
		}
	}

	filter := func(token string) bool {
		_, ok := found[token]
		delete(found, token)
		return ok
	}
	return common.FilterSliceInPlace(tokens, filter)
}

func filterShortTokensInPlace(tokens []string, minLen int) []string {
	n := 0
	for _, token := range tokens {
		if len(token) >= minLen {
			tokens[n] = token
			n++
		}
	}

	return tokens[:n]
}

func cutLongTokens(tokens []string, maxLen int) []string {
	for i, token := range tokens {
		if len(token) > maxLen {
			tokens[i] = token[:maxLen]
		}
	}

	return tokens
}

//
// // tokenizeExpression splits literals into tokens "1.3 AND 3:4" becomes "1 AND 2 AND 3 AND 4"
// func tokenizeExpression(e *Expression) *Expression {
//
//	tokenizedOperands := []any{}
//	for _, operand := range e.operands {
//		if opExpr, ok := operand.(*Expression); ok {
//			tokenizedOperands = append(tokenizedOperands, tokenizeExpression(opExpr))
//			continue
//		}
//		// else it is a literal
//		if opString, ok := operand.(string); ok {
//			for _, token := range Tokenize(opString) {
//				tokenizedOperands = append(tokenizedOperands, token)
//			}
//			continue
//		}
//		// otherwise panic
//		log.Panicf("unable To Tokenize operand %v", operand)
//	}
//
//	return &Expression{e.operator, tokenizedOperands}
// }
