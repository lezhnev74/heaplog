package scanner

import (
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDetectMessages(t *testing.T) {
	// given an arbitrary log file, it can detect the messages beginning sequences
	// as well as data format. Assuming that data is present at the beginning of the message.
	type test struct {
		input, extractedDate  string
		reMessageBeginPattern string // includes Date in the first group
		dateFormat            string
	}

	tests := []test{
		{
			input:                 `[2023-01-05 23:40:20.779604] testing.DEBUG: message 1`,
			extractedDate:         "2023-01-05 23:40:20.779604",
			reMessageBeginPattern: `(?m)^\[(\d{4}\-\d{2}\-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d{6})`,
			dateFormat:            "2006-01-02 15:04:05.000000",
		},
		{
			input:                 `[Fri Dec 16 01:46:23 2005] [error] [client 1.2.3.4] Directory index forbidden by rule: /home/test_util/`,
			extractedDate:         "Dec 16 01:46:23 2005",
			reMessageBeginPattern: `(?m)^\[Fri (\w{3}\s+\d{2}\s+\d{2}:\d{2}:\d{2}\s+\d{4})`,
			dateFormat:            "Jan 02 15:04:05 2006",
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("test_util %d", i), func(t *testing.T) {
			msgStartPattern, dateFormat, err := DetectMessageLine([]byte(tt.input))
			require.NoError(t, err)
			require.EqualValues(t, tt.dateFormat, dateFormat)
			require.EqualValues(t, tt.reMessageBeginPattern, msgStartPattern)

			r := regexp.MustCompile(tt.reMessageBeginPattern)
			matches := r.FindStringSubmatch(tt.input)
			require.Len(t, matches, 2)
			require.EqualValues(t, tt.extractedDate, matches[1])
		})
	}
}

func TestTimeLayoutToRegexp(t *testing.T) {
	type test struct {
		format  string
		pattern string
	}
	tests := []test{
		{
			`01/02 03:04:05PM '06 -0700`,
			`\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}\w{2}\s+'\d{2}\s+[+-]\d{4}`,
		},
		{
			`Mon Jan _2 15:04:05 2006`,
			`\w{3}\s+\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\d{4}`,
		},
		{
			`2006-01-02T15:04:05.999999999Z07:00 __2`,
			`\d{4}\-\d{2}\-\d{2}\w\d{2}:\d{2}:\d{2}\.\d{9}(?:\w)|(?:[+-]\d{2}):\d{2}\s+\d{1,3}`,
		},
		{
			`2006-01-02 15:04:05`,
			`\d{4}\-\d{2}\-\d{2}\s+\d{2}:\d{2}:\d{2}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.format, func(t *testing.T) {
			pattern := TimeFormatToRegexp(tt.format)
			require.EqualValues(t, tt.pattern, pattern)
			// fmt.Printf("F:%s\nN:%s\nP:%s", tt.format, time.Now().Format(tt.format), pattern)
			re := regexp.MustCompile(pattern)
			require.True(t, re.MatchString(time.Now().Format(tt.format)))
		})
	}
}
