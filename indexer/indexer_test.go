package indexer

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/require"
	"heaplog/common"
	"heaplog/scanner"
	tokenizer2 "heaplog/tokenizer"
	"io"
	"regexp"
	"slices"
	"testing"
)

var (
	// these are default formats used in my tests:
	messageStartPattern = regexp.MustCompile(`(?m)^\[(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}\.?(\d{6}([+-]\d\d:\d\d)?)?)]`)
	dateLayout          = "2006-01-02 15:04:05.000000"
	tokenizer           = func(input string) []string {
		return tokenizer2.Tokenize(string(input), 4, 40)
	}
	s       = scanner.NewScanner(dateLayout, messageStartPattern, 100, 1000)
	indexer = NewIndexer(s, tokenizer)
)

func TestScanSegment(t *testing.T) {

	srcStream := bytes.NewReader([]byte(`
[2023-01-01 00:00:00.000000] Hello, friend. Hello, friend?
[2023-01-01 00:00:02.111111] That's lame. Maybe I should give you a name, but that's a slippery slope
[2023-01-01 00:00:03.222222] You're only in my head. We have to remember that. Shit.
`))

	type test struct {
		loc                  common.Location
		expectedSegment      common.IndexedSegment
		expectedTerms        []string
		expectedMessageTexts []string
	}

	tests := []test{
		{ // it aligns the segment location to message boundaries (date is not indexed)
			loc: common.Location{Min: 1, Max: 10}, // contains 1st message date
			expectedSegment: common.IndexedSegment{
				Messages: []common.IndexedMessage{
					{-1, common.Location{Min: 1, Max: 60}, common.MakeTime(dateLayout, "2023-01-01 00:00:00.000000"), false},
				},
			},
			expectedTerms: []string{"hello", "friend"},
			expectedMessageTexts: []string{
				"[2023-01-01 00:00:00.000000] Hello, friend. Hello, friend?\n",
			},
		},
		{ // it detects 2 messages
			loc: common.Location{Min: 0, Max: 100}, // fits 2 messages' starts
			expectedSegment: common.IndexedSegment{
				Messages: []common.IndexedMessage{
					{-1, common.Location{Min: 1, Max: 60}, common.MakeTime(dateLayout, "2023-01-01 00:00:00.000000"), false},
					{-1, common.Location{Min: 60, Max: 162}, common.MakeTime(dateLayout, "2023-01-01 00:00:02.111111"), false},
				},
			},
			expectedTerms: []string{"hello", "friend", "that", "lame", "maybe", "should", "give", "name", "slippery", "slope"},
			expectedMessageTexts: []string{
				"[2023-01-01 00:00:00.000000] Hello, friend. Hello, friend?\n",
				"[2023-01-01 00:00:02.111111] That's lame. Maybe I should give you a name, but that's a slippery slope\n",
			},
		},
		{ // it detects all messages
			loc: common.Location{Min: 0, Max: 999_999}, // fits 2 messages' starts
			expectedSegment: common.IndexedSegment{
				Messages: []common.IndexedMessage{
					{-1, common.Location{Min: 1, Max: 60}, common.MakeTime(dateLayout, "2023-01-01 00:00:00.000000"), false},
					{-1, common.Location{Min: 60, Max: 162}, common.MakeTime(dateLayout, "2023-01-01 00:00:02.111111"), false},
					{-1, common.Location{Min: 162, Max: 247}, common.MakeTime(dateLayout, "2023-01-01 00:00:03.222222"), true},
				},
			},
			expectedTerms: []string{
				"hello", "friend",
				"that", "lame", "maybe", "should", "give", "name", "slippery", "slope",
				"only", "head", "have", "remember", "shit",
			},
			expectedMessageTexts: []string{
				"[2023-01-01 00:00:00.000000] Hello, friend. Hello, friend?\n",
				"[2023-01-01 00:00:02.111111] That's lame. Maybe I should give you a name, but that's a slippery slope\n",
				"[2023-01-01 00:00:03.222222] You're only in my head. We have to remember that. Shit.\n",
			},
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			segment, terms, err := indexer.IndexSegment(srcStream, tt.loc)
			require.NoError(t, err)
			require.EqualValues(t, tt.expectedSegment, segment)

			slices.Sort(terms)
			slices.Sort(tt.expectedTerms)
			require.EqualValues(t, tt.expectedTerms, terms)

			require.Equal(t, len(tt.expectedSegment.Messages), len(segment.Messages))
			for i, m := range segment.Messages {
				actualMessage := make([]byte, m.Loc.Max-m.Loc.Min)
				srcStream.Seek(m.Loc.Min, io.SeekStart)
				srcStream.Read(actualMessage)

				require.Equal(t, tt.expectedMessageTexts[i], string(actualMessage))
			}

		})
	}
}
