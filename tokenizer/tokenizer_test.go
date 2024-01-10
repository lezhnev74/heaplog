package tokenizer

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"sort"
	"testing"
)

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

func TestTokenizerF(t *testing.T) {

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

	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			actualTokens := TokenizeF(tt.input, tt.minTokenSize, tt.maxTokenSize)

			sort.Strings(tt.expectedTokens)
			sort.Strings(actualTokens)
			require.Equal(t, tt.expectedTokens, actualTokens)
		})
	}
}
