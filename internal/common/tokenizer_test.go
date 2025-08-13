package common

import (
	"bytes"
	"fmt"
	"log"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFilterShortTokensInPlaceCutLongTokens(t *testing.T) {
	type test struct {
		src                [][]byte
		expected           [][]byte
		minRunes, maxRunes int
	}

	tests := []test{
		{
			[][]byte{[]byte("привет"), []byte("как"), []byte("дела")},
			[][]byte{[]byte("приве"), []byte("дела")},
			4, 5,
		},
	}

	for i, tt := range tests {
		t.Run(
			fmt.Sprintf("test_util %d", i), func(t *testing.T) {
				actualTokens := filterShortTokensInPlaceCutLongTokens(tt.src, tt.minRunes, tt.maxRunes)
				require.Equal(t, tt.expected, actualTokens)
			},
		)
	}
}

func TestTokenizer(t *testing.T) {

	type test struct {
		input                      []byte
		minTokenSize, maxTokenSize int
		expectedTokens             [][]byte
	}

	tests := []test{
		{ // to lower
			input:          []byte("hello Hello HELLO"),
			minTokenSize:   4,
			maxTokenSize:   10,
			expectedTokens: [][]byte{[]byte("hello")},
		},
		{ // punctuation
			input:          []byte(`"label":"6f85c55a-4f23","ipaddr":"192.168.1.0"`),
			minTokenSize:   4,
			maxTokenSize:   10,
			expectedTokens: [][]byte{[]byte("label"), []byte("6f85c55a"), []byte("4f23"), []byte("ipaddr")},
		},
		{ // min/max respect
			input:          []byte("a ab abc abcd abcde abcdef"),
			minTokenSize:   4,
			maxTokenSize:   5,
			expectedTokens: [][]byte{[]byte("abcd"), []byte("abcde")},
		},
		{ // unicode
			input:          []byte("привет, как дела? Привет!"),
			minTokenSize:   4,
			maxTokenSize:   5,
			expectedTokens: [][]byte{[]byte("дела"), []byte("приве")},
		},
		{ // punctuation
			input:        []byte("trace: f6d2b151-0ee8-4182-968b-20d41b869d18/2b3b2e47-0ab6-46f6-abba-4fe07b4b3279"),
			minTokenSize: 4,
			maxTokenSize: 8,
			expectedTokens: [][]byte{
				[]byte("trace"),
				[]byte("f6d2b151"),
				[]byte("0ee8"),
				[]byte("4182"),
				[]byte("968b"),
				[]byte("20d41b86"),
				[]byte("2b3b2e47"),
				[]byte("0ab6"),
				[]byte("46f6"),
				[]byte("abba"),
				[]byte("4fe07b4b"),
			},
		},
	}

	for i, tt := range tests {
		t.Run(
			fmt.Sprintf("test_util %d", i), func(t *testing.T) {
				actualTokens := Tokenize(tt.input, tt.minTokenSize, tt.maxTokenSize)
				actualTokens = filterDuplicatedTokensInPlaceNoAlloc(actualTokens)
				slices.SortFunc(tt.expectedTokens, bytes.Compare)
				slices.SortFunc(actualTokens, bytes.Compare)
				require.Equal(t, tt.expectedTokens, actualTokens)
			},
		)
	}
}

var tokens [][]byte

func BenchmarkTokenize(b *testing.B) {
	input := []byte(`
There is no way to avoid or replace the hard work of thinking. When you write a test_util you are thinking about how to specify behavior. When you make the test_util pass you are thinking about how to implement that specification. When you refactor you are thinking about how to communicate both the specification and implementation to others.
You cannot replace any of these thought processes with tools. You cannot generate the code from tests, or the tests from code, because that would cause you to abandon a critical thought process. And may God help you if you use a tool to do the refactoring for you.
The purpose of a tool is to enable and facilitate thought; not to replace it.
`)
	input = bytes.Repeat(input, 10_000)
	var xtokens [][]byte

	b.Run(
		"x", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				xtokens = Tokenize(input, 4, 6)
			}
			tokens = xtokens
		},
	)

	log.Printf("found %d tokens", len(tokens))
}
