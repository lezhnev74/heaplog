package scanner

import (
	"bytes"
	_ "embed"
	"fmt"
	"github.com/stretchr/testify/require"
	"heaplog/common"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

var messageStartPattern = regexp.MustCompile(`(?m)^\[(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}\.?(\d{6}([+-]\d\d:\d\d)?)?)]`)

func TestScanner(t *testing.T) {

	t.Run("it finds 1 ScannedMessage", func(t *testing.T) {
		sc := NewScanner("2006-01-02 15:04:05.000000", messageStartPattern, 100, 1000)

		// use the sample content as a buffer
		logBuf := []byte("[2023-01-05 23:46:20.779604] testing.DEBUG: ScannedMessage 1\n")
		messages, err := sc.ScanAll(bytes.NewReader(logBuf))
		require.NoError(t, err)

		require.Equal(t, 1, len(messages))
		require.Equal(t, []byte("[2023-01-05 23:46:20.779604] testing.DEBUG: ScannedMessage 1\n"), logBuf[messages[0].Pos:messages[0].Pos+messages[0].Len])
		require.Equal(t, 0, messages[0].Pos)
		require.Equal(t, common.MakeTime(sc.dateLayout, "2023-01-05 23:46:20.779604"), messages[0].DateTime)
		require.True(t, messages[0].IsTail)
	})

	t.Run("it finds 3 messages with a small buffer", func(t *testing.T) {
		sc := NewScanner("2006-01-02 15:04:05.000000", messageStartPattern, 30, 1000)

		// use the sample content as a buffer
		logBuf := []byte(`
[2023-01-05 23:40:20.779604] testing.DEBUG: message 1
[2023-01-05 23:42:20.779604] testing.DEBUG: message 2
[2023-01-05 23:45:11.324153] testing.DEBUG: message 3
`)
		messages, err := sc.ScanAll(bytes.NewReader(logBuf))
		require.NoError(t, err)

		require.Equal(t, 3, len(messages))

		require.Equal(t, []byte("[2023-01-05 23:40:20.779604] testing.DEBUG: message 1\n"), logBuf[messages[0].Pos:messages[0].Pos+messages[0].Len])
		require.Equal(t, []byte("2023-01-05 23:40:20.779604"), messages[0].Date)
		require.Equal(t, 1, messages[0].Pos)
		require.False(t, messages[0].IsTail)

		require.Equal(t, []byte("[2023-01-05 23:42:20.779604] testing.DEBUG: message 2\n"), logBuf[messages[1].Pos:messages[1].Pos+messages[1].Len])
		require.Equal(t, []byte("2023-01-05 23:42:20.779604"), messages[1].Date)
		require.Equal(t, 55, messages[1].Pos)
		require.False(t, messages[1].IsTail)

		require.Equal(t, []byte("[2023-01-05 23:45:11.324153] testing.DEBUG: message 3\n"), logBuf[messages[2].Pos:messages[2].Pos+messages[2].Len])
		require.Equal(t, []byte("2023-01-05 23:45:11.324153"), messages[2].Date)
		require.Equal(t, 109, messages[2].Pos)
		require.True(t, messages[2].IsTail)
	})

	t.Run("it finds 2 multiline messages", func(t *testing.T) {
		sc := NewScanner("2006-01-02 15:04:05.000000", messageStartPattern, 100, 1000)

		// use the sample content as a buffer
		logBuf := []byte(`
[2023-01-05 23:46:22.234123] testing.DEBUG: BING ADS API #0:
BING ADS response (recorded):
{
    "ReportRequestStatus": {
        "ReportDownloadUrl": null,
        "Status": "Success"
    }
}
{"exec":{"label":"6f85c55a-4f23-45cc-8a3c-c814cc1a1d98","environment":"testing","started_at":1678491979534005,"user_id":null,"channel":{"type":"console"},"extras":[]}}
[2023-01-07 00:00:04.452670] production.INFO: start reading tasks for App\Infrastructure\Platforms\GoogleShopping\Queue\LinkAdsMerchantTask\LinksAdsMerchantWorker
`)
		messages, err := sc.ScanAll(bytes.NewReader(logBuf))
		require.NoError(t, err)

		require.Equal(t, 2, len(messages))

		expectedMessage := `[2023-01-05 23:46:22.234123] testing.DEBUG: BING ADS API #0:
BING ADS response (recorded):
{
    "ReportRequestStatus": {
        "ReportDownloadUrl": null,
        "Status": "Success"
    }
}
{"exec":{"label":"6f85c55a-4f23-45cc-8a3c-c814cc1a1d98","environment":"testing","started_at":1678491979534005,"user_id":null,"channel":{"type":"console"},"extras":[]}}
`
		require.Equal(t, []byte(expectedMessage), logBuf[messages[0].Pos:messages[0].Pos+messages[0].Len])
		require.Equal(t, common.MakeTime(sc.dateLayout, "2023-01-05 23:46:22.234123"), messages[0].DateTime)

		require.Equal(
			t,
			[]byte("[2023-01-07 00:00:04.452670] production.INFO: start reading tasks for App\\Infrastructure\\Platforms\\GoogleShopping\\Queue\\LinkAdsMerchantTask\\LinksAdsMerchantWorker\n"),
			logBuf[messages[1].Pos:messages[1].Pos+messages[1].Len],
		)
		require.Equal(t, common.MakeTime(sc.dateLayout, "2023-01-07 00:00:04.452670"), messages[1].DateTime)
	})

	t.Run("it respects ScannedMessage starts", func(t *testing.T) {
		sc := NewScanner("2006-01-02 15:04:05.000000", messageStartPattern, 100, 1000)

		// use the sample content as a buffer
		logBuf := []byte(`
[2023-01-05 23:46:22.234123] testing.DEBUG: [2023-01-05 23:46:22.234123]
[2023-01-07 00:00:04.452670] testing.DEBUG: [2023-01-07 00:00:04.452670]
`)
		messages, err := sc.ScanAll(bytes.NewReader(logBuf))
		require.NoError(t, err)

		require.Equal(t, 2, len(messages))
		require.Equal(
			t,
			[]byte("[2023-01-05 23:46:22.234123] testing.DEBUG: [2023-01-05 23:46:22.234123]\n"),
			logBuf[messages[0].Pos:messages[0].Pos+messages[0].Len],
		)
		require.Equal(
			t,
			[]byte("[2023-01-07 00:00:04.452670] testing.DEBUG: [2023-01-07 00:00:04.452670]\n"),
			logBuf[messages[1].Pos:messages[1].Pos+messages[1].Len],
		)
	})

	t.Run("it respects max buffer size when a message it too long", func(t *testing.T) {
		sc := NewScanner("2006-01-02 15:04:05.000000", messageStartPattern, 100, 1000)

		// not possible To test as the procedure is performed in a separate go-routine
		logBuf := bytes.NewBufferString(`[2023-01-05 23:46:22.234123]` + strings.Repeat("A-", 1000))

		_, err := sc.ScanAll(logBuf)
		require.ErrorIs(t, err, MaxBufSizeReached)
	})

	t.Run("it respects max buffer size when no message can be found", func(t *testing.T) {
		sc := NewScanner("2006-01-02 15:04:05.000000", messageStartPattern, 100, 1000)

		// not possible To test as the procedure is performed in a separate go-routine
		logBuf := bytes.NewBufferString(strings.Repeat("TRASH", 1000))

		_, err := sc.ScanAll(logBuf)
		require.ErrorIs(t, err, NoMessageStartFound)
	})

	t.Run("it stops scanning", func(t *testing.T) {
		sc := NewScanner("2006-01-02 15:04:05.000000", messageStartPattern, 100, 1000)

		src := []byte(`
[2023-01-05 23:40:20.779604] testing.DEBUG: ScannedMessage 1
[2023-01-05 23:42:20.779604] testing.DEBUG: ScannedMessage 2
[2023-01-05 23:45:11.324153] testing.DEBUG: ScannedMessage 3
`)
		// stop after the first message scanned
		messages := make([]*ScannedMessage, 0)
		err := sc.Scan(bytes.NewReader(src), func(m *ScannedMessage) bool {
			if m.Pos > 1 {
				return true
			}
			messages = append(messages, m)
			return false
		})
		require.NoError(t, err)

		require.Equal(t, 1, len(messages))

		require.Equal(t, 1, messages[0].Pos)
		require.Equal(
			t,
			[]byte("[2023-01-05 23:40:20.779604] testing.DEBUG: ScannedMessage 1\n"),
			src[messages[0].Pos:messages[0].Pos+messages[0].Len],
		)
		// require.Equal(t, []byte("2023-01-05 23:40:20.779604"), messages[0].Date)
	})
}

func TestScanAllMessagesThroughput(t *testing.T) {
	sc := NewScanner("2006-01-02 15:04:05.000000", messageStartPattern, 100, 1000)

	type test int
	tests := []test{
		1,
		2,
		3,
		5,
		10,
		10_000,
		100_000,
	}
	for _, N := range tests {
		t.Run(fmt.Sprintf("test %d", N), func(t *testing.T) {
			// generate N messages To a buffer
			msgDateLayout := "2006-01-02 15:04:05.000000"
			text := make([]byte, 0)
			date := time.Now()
			for i := 0; i < int(N); i++ {
				letter := strconv.Itoa(i % int(N))
				newMsg := []byte(fmt.Sprintf("[%s] testing.DEBUG: %s\n", date.Format(msgDateLayout), strings.Repeat(letter, 5)))
				text = append(text, newMsg...)
				date = date.Add(time.Microsecond * time.Duration(rand.Int()%20))
			}
			logBuf := bytes.NewBuffer(text)
			// Run scanning and measure time

			start := time.Now()
			messages, err := sc.ScanAll(logBuf)
			require.NoError(t, err)
			d := time.Now().Sub(start).Seconds()
			throughput := float64(N) / d
			require.EqualValues(t, N, len(messages))
			fmt.Printf("N: %d, msgs/s: %f\n", N, throughput)
		})
	}
}

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
			input:                 `[Fri Dec 16 01:46:23 2005] [error] [client 1.2.3.4] Directory index forbidden by rule: /home/test/`,
			extractedDate:         "Dec 16 01:46:23 2005",
			reMessageBeginPattern: `(?m)^\[Fri (\w{3}\s+\d{2}\s+\d{2}:\d{2}:\d{2}\s+\d{4})`,
			dateFormat:            "Jan 02 15:04:05 2006",
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
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
