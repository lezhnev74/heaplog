package tokenizer

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"sort"
	"strings"
	"testing"
)

var tokens []string

func BenchmarkTokenizers(b *testing.B) {
	input := `
There is no way to avoid or replace the hard work of thinking. When you write a test you are thinking about how to specify behavior. When you make the test pass you are thinking about how to implement that specification. When you refactor you are thinking about how to communicate both the specification and implementation to others.
You cannot replace any of these thought processes with tools. You cannot generate the code from tests, or the tests from code, because that would cause you to abandon a critical thought process. And may God help you if you use a tool to do the refactoring for you.
The purpose of a tool is to enable and facilitate thought; not to replace it.
`
	bigInput := strings.Repeat(input, 1)

	benchmarks := []struct {
		name      string
		tokenizer func(input string, min, max int) []string
	}{
		{"Tokenize", Tokenize},
		{"TokenizeS", TokenizeS},
		{"TokenizeS2", TokenizeS2},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			var lTokens []string
			for i := 0; i < b.N; i++ {
				lTokens = bm.tokenizer(bigInput, 4, 20)
			}
			tokens = lTokens
		})
	}
}

func TestTokenizer(t *testing.T) {

	type test struct {
		input                      string
		minTokenSize, maxTokenSize int
		expectedTokens             []string
	}

	tests := []test{
		{ // to lower
			input:          "hello Hello HELLO",
			minTokenSize:   4,
			maxTokenSize:   10,
			expectedTokens: []string{"hello"},
		},
		{ // diacritics
			input:          "hale ĤÄḸÉ protégé 我的",
			minTokenSize:   4,
			maxTokenSize:   10,
			expectedTokens: []string{"hale", "protege", "我的"},
		},
		{ // punctuation
			input:          `"label":"6f85c55a-4f23","ipaddr":"192.168.1.0"`,
			minTokenSize:   4,
			maxTokenSize:   10,
			expectedTokens: []string{"label", "6f85c55a", "4f23", "ipaddr"},
		},
		{ // min/max respect
			input:          "a ab abc abcd abcde abcdef",
			minTokenSize:   4,
			maxTokenSize:   5,
			expectedTokens: []string{"abcd", "abcde"},
		},
		{ // big message
			input: `
[2023-01-05 23:46:22.234123] testing.DEBUG: BING ADS API #0: 我的 L’orange protégé
BING ADS response (recorded):
{
    "ReportRequestStatus": {
        "ReportDownloadUrl": null,
        "Status": "Success"
    }
}
{"exec":{"label":"6f85c55a-4f23-45cc-8a3c-c814cc1a1d98","environment":"testing","started_at":1678491979534005,"user_id":null,"channel":{"type":"console"},"extras":[]}}
`,
			minTokenSize: 4,
			maxTokenSize: 40,
			expectedTokens: []string{
				"2023", "234123", "testing", "debug", "bing", "我的", "orange", "protege", "response", "recorded",
				"reportrequeststatus", "reportdownloadurl", "null", "status", "success",
				"exec", "label", "6f85c55a", "4f23", "45cc", "8a3c", "c814cc1a1d98", "environment",
				"started", "1678491979534005", "user", "channel", "type", "console", "extras",
			},
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			actualTokens := Tokenize(tt.input, tt.minTokenSize, tt.maxTokenSize)

			sort.Strings(tt.expectedTokens)
			sort.Strings(actualTokens)
			require.Equal(t, tt.expectedTokens, actualTokens)
		})
	}
}

func TestTokenizerFx(t *testing.T) {

	type test struct {
		input                      string
		minTokenSize, maxTokenSize int
		expectedTokens             []string
	}

	tests := []test{
		{ // to lower
			input:          "hello Hello HELLO",
			minTokenSize:   4,
			maxTokenSize:   10,
			expectedTokens: []string{"hello"},
		},
		{ // punctuation
			input:          `"label":"6f85c55a-4f23","ipaddr":"192.168.1.0"`,
			minTokenSize:   4,
			maxTokenSize:   10,
			expectedTokens: []string{"label", "6f85c55a", "4f23", "ipaddr"},
		},
		{ // min/max respect
			input:          "a ab abc abcd abcde abcdef",
			minTokenSize:   4,
			maxTokenSize:   5,
			expectedTokens: []string{"abcd", "abcde"},
		},
		{ // big message
			input: `
[2023-01-05 23:46:22.234123] testing.DEBUG: BING ADS API #0: 我的 L’orange protégé
BING ADS response (recorded):
{
    "ReportRequestStatus": {
        "ReportDownloadUrl": null,
        "Status": "Success"
    }
}
{"exec":{"label":"6f85c55a-4f23-45cc-8a3c-c814cc1a1d98","environment":"testing","started_at":1678491979534005,"user_id":null,"channel":{"type":"console"},"extras":[]}}
`,
			minTokenSize: 4,
			maxTokenSize: 40,
			expectedTokens: []string{
				"2023", "234123", "testing", "debug", "bing", "我的", "l’orange", "protégé", "response", "recorded",
				"reportrequeststatus", "reportdownloadurl", "null", "status", "success",
				"exec", "label", "6f85c55a", "4f23", "45cc", "8a3c", "c814cc1a1d98", "environment",
				"started", "1678491979534005", "user", "channel", "type", "console", "extras",
			},
		},
	}

	tokenizerFuncs := map[string]Tokenizer{
		"TokenizeS":  TokenizeS,
		"TokenizeS2": TokenizeS2,
	}

	for i, tt := range tests {
		for tn, tf := range tokenizerFuncs {
			t.Run(fmt.Sprintf("test %d - %s", i, tn), func(t *testing.T) {
				actualTokens := tf(tt.input, tt.minTokenSize, tt.maxTokenSize)
				sort.Strings(tt.expectedTokens)
				sort.Strings(actualTokens)
				require.Equal(t, tt.expectedTokens, actualTokens)
			})
		}
	}
}

func TestSplit(t *testing.T) {
	sep := " .-"

	tests := []struct {
		input    string
		expected []string
	}{
		{"", nil},
		{"a", []string{"a"}},
		{" a", []string{"a"}},
		{"    a", []string{"a"}},
		{" a ", []string{"a"}},
		{" a    ", []string{"a"}},
		{"a b-c.d", []string{"a", "b", "c", "d"}},
		{"a - b", []string{"a", "b"}},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("test: %s", tt.input), func(t *testing.T) {
			require.Equal(t, tt.expected, splitString(tt.input, sep))
			require.Equal(t, tt.expected, splitStringNoAlloc(tt.input, sep))
		})
	}
}

var splits []string

func BenchmarkSplit(b *testing.B) {

	sep := " .-"
	input := `
There is no way to avoid or replace the hard work of thinking. When you write a test you are thinking about how to specify behavior. When you make the test pass you are thinking about how to implement that specification. When you refactor you are thinking about how to communicate both the specification and implementation to others.
You cannot replace any of these thought processes with tools. You cannot generate the code from tests, or the tests from code, because that would cause you to abandon a critical thought process. And may God help you if you use a tool to do the refactoring for you.
The purpose of a tool is to enable and facilitate thought; not to replace it.
`
	input = strings.Repeat(input, 10_000)
	var lSplits []string

	benchmarks := []struct {
		name      string
		splitFunc func(input string, sep string) []string
	}{
		{"splitString", splitString},
		{"splitStringNoAlloc", splitStringNoAlloc},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				lSplits = bm.splitFunc(input, sep)
			}
			splits = lSplits
		})
	}
}
