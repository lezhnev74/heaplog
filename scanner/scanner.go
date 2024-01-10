package scanner

import (
	"fmt"
	"github.com/araddon/dateparse"
	"golang.org/x/xerrors"
	"io"
	"log"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

/*
This code is responsible for extracting messages from a source stream of bytes (a reader).
It finds individual messages (sequences of bytes) in the stream and reports them.
*/

var (
	// NoMessageStartFound means the scanner failed to locate the start of a message in the stream
	NoMessageStartFound = xerrors.Errorf("unable to find a message in the stream")
	// MaxBufSizeReached means the scanner found the start of a message, but failed to find its end (EOF or the next message start)
	MaxBufSizeReached = xerrors.Errorf("max scanning buffer size reached")
)

type ScannedMessage struct {
	Body, Date []byte
	Pos        int // position in the stream
	DateTime   time.Time
	IsTail     bool // sets to true if message ended with EOF
	Err        error
}

type Scanner struct {
	// dateLayout is a go layout that used to parse message dates
	dateLayout string
	// messageStart recognizes each new message beginning
	messageStart *regexp.Regexp
	// readSize is how many bytes it will read at once to find a message
	// if no message found it will read again and again until a message found or maxBufSize reached.
	// After each read it runs a RE to find the next message start.
	readSize, maxBufSize int
}

func NewScanner(
	dateLayout string,
	messageStart *regexp.Regexp,
	readSize, maxBufSize int,
) *Scanner {
	return &Scanner{
		dateLayout:   dateLayout,
		messageStart: messageStart,
		readSize:     readSize,
		maxBufSize:   maxBufSize,
	}
}

func (sc *Scanner) newError(err error) *ScannedMessage { return &ScannedMessage{Err: err} }

func (sc *Scanner) newMessage(body []byte, pos int, date []byte, isTail bool) *ScannedMessage {
	d, err := sc.parseDate(date)
	if err != nil {
		return sc.newError(xerrors.Errorf("date parsing fail: %w", err))
	}

	return &ScannedMessage{
		Body:     append([]byte{}, body...), // body must be copied as it points to the shared buf
		Date:     date,                      // while the Date is copied already to a dedicated buf
		Pos:      pos,
		DateTime: d,
		IsTail:   isTail,
	}
}

// parseDate is a configured function that recognizes formatted date string
func (sc *Scanner) parseDate(date []byte) (time.Time, error) {
	return time.Parse(sc.dateLayout, string(date))
}

// scan is the main code that extracts messages. It is meant to be called in a separate goroutine.
// a function is provided to stop the execution earlier.
func (sc *Scanner) scan(
	src io.Reader,
	messages chan<- *ScannedMessage,
	shouldStop func(*ScannedMessage) bool,
) {
	defer close(messages)
	// Allocate a buffer for copying data From the reader
	buf := make([]byte, sc.readSize)

	// workable area is buf's area that contains unprocessed data
	var msgStartPos, // last detected message start in the workable area
		msgStartMatchEnd, // detected msg's pattern match end
		bufLen, // len of workable area
		bytesProcessed, // bytes that were processed before the workable area
		msgsProcessed int

	// 1. Read initial data To the buffer
	bufLen, err := src.Read(buf)
	if bufLen == 0 && err != nil { // (bufLen == 0 and err == nil)?
		messages <- sc.newError(err)
		return
	}

	// 2. Find the start of a message in the workable space
	m := sc.messageStart.FindSubmatchIndex(buf[:bufLen])
	if m == nil {
		err = xerrors.Errorf("%w: checked %d bytes", NoMessageStartFound, bufLen)
		messages <- sc.newError(err)
		return
	}
	msgStartPos, msgStartMatchEnd = m[0], m[1]
	msgDate := append([]byte{}, buf[m[2]:m[3]]...)
	for {
		// Find the start of a message since msgStart+msgStartMatchEnd
		m = sc.messageStart.FindSubmatchIndex(buf[msgStartMatchEnd:bufLen])
		if m != nil {
			detectedMessage := sc.newMessage(
				buf[msgStartPos:msgStartMatchEnd+m[0]],
				bytesProcessed+msgStartPos,
				msgDate,
				false,
			)
			if shouldStop(detectedMessage) {
				return // halt scanning BEFORE returning a message
			}
			messages <- detectedMessage                                                     // found
			msgDate = append([]byte{}, buf[msgStartMatchEnd+m[2]:msgStartMatchEnd+m[3]]...) // copy Date
			msgStartPos, msgStartMatchEnd = msgStartMatchEnd+m[0], msgStartMatchEnd+m[1]
			msgsProcessed++
			continue
		}
		// next message not found
		// before reading more data, GC processed buf space
		// shift the workable area To the left
		if msgStartPos > 0 {
			bytesProcessed += msgStartPos
			bufLen -= msgStartPos
			copy(buf[:bufLen], buf[msgStartPos:msgStartPos+bufLen])
			copy(buf[bufLen:], make([]byte, len(buf)-bufLen)) // reset memory
			msgStartPos, msgStartMatchEnd = 0, msgStartMatchEnd-msgStartPos
		}
		// if buf has no spare space after the workable area - extend the buffer
		if bufLen == cap(buf) {
			// respect max buf size
			if len(buf) >= sc.maxBufSize {
				err = xerrors.Errorf("%w: %d bytes (consider increasing the max buffer size)", MaxBufSizeReached, sc.maxBufSize)
				log.Printf("%v", err)
				messages <- sc.newError(err)
				return
			}
			// Extend the buf
			buf2 := make([]byte, len(buf)+sc.readSize)
			copy(buf2, buf)
			buf = buf2
		}
		// try To extend the workable area with new data
		readBytes, err := src.Read(buf[bufLen:])
		if readBytes == 0 && err == io.EOF { // (bufLen == 0 and err == nil)?
			// here if EOF found it will be used as the right ScannedMessage boundary,
			// however if the writing is not atomic, we could only capture part of the ScannedMessage
			// due To seeing EOF between writes
			lastMessage := sc.newMessage(buf[msgStartPos:bufLen], bytesProcessed+msgStartPos, msgDate, true)
			if shouldStop(lastMessage) {
				return // halt scanning BEFORE returning a message
			}
			messages <- lastMessage // found
			msgsProcessed++
			return
		}
		bufLen += readBytes
		continue
	}
}

// ScanAllMessages reads out all data From the reader and returns matching messages
// it uses messageStart To delimit messages in the stream
// it stops when no more bytes available in the reader
func (sc *Scanner) ScanAllMessages(s io.Reader) <-chan *ScannedMessage {
	messages := make(chan *ScannedMessage)

	// Start a go-routine To keep reading From the reader and finding messages
	go sc.scan(s, messages, func(*ScannedMessage) bool { return false })

	return messages
}

// ScanMessagesCond does the same as ScanAllMessages but has a mean to stop scanning
// this is used when only a part of the stream must be scanned.
func (sc *Scanner) ScanMessagesCond(s io.Reader, shouldStop func(*ScannedMessage) bool) <-chan *ScannedMessage {
	messages := make(chan *ScannedMessage)

	// Start a go-routine To keep reading From the reader and finding messages
	go sc.scan(s, messages, shouldStop)

	return messages
}

// DetectMessageLine accepts a line with a known Date and extracts the settings
// return a regex pattern for the line beginning that includes Date in the 1st matching group
func DetectMessageLine(text []byte) (startPattern string, dateFormat string, err error) {
	nonDateChars := `[^\d\w\.,\s:\(\)/+-]`
	p := regexp.MustCompile(nonDateChars)
	for i := 0; i < len(text); i++ {
		m := p.FindIndex(text[i+1:])
		if m == nil {
			break
		}
		subline := text[i : m[0]+i+1]
		msgDate, err := dateparse.ParseAny(string(subline))
		if err != nil {
			continue
		}
		dateFormat, err = dateparse.ParseFormat(string(subline))
		if err != nil {
			continue
		}
		// use the found Date To detect prefix
		dateRestored := msgDate.Format(dateFormat)
		pos := strings.Index(string(text), dateRestored)
		if pos == -1 {
			continue
		}
		prefix := string(text[:pos])
		// put the Date To the 1st matching group
		escapedPrefix := ""
		for _, r := range prefix {
			if strings.ContainsRune(`.,+*?^$()[]{}|\-`, r) {
				escapedPrefix += `\`
			}
			escapedPrefix += string(r)
		}
		startPattern = fmt.Sprintf("(?m)^%s(%s)", escapedPrefix, TimeFormatToRegexp(dateFormat))
		return startPattern, dateFormat, nil
	}
	return "", "", xerrors.Errorf("unable To detect messages")
}

// TimeFormatToRegexp returns a regexp pattern that can recognize any Date in the given Time Format
func TimeFormatToRegexp(format string) (pattern string) {
	type unit struct {
		unitPattern string
		unitCount   int
	}
	groups := make([]unit, 0)
	addUnit := func(unitPattern string) {
		if len(groups) == 0 || groups[len(groups)-1].unitPattern != unitPattern {
			groups = append(groups, unit{unitPattern, 0})
		}
		groups[len(groups)-1].unitCount++
	}

	prefixMap := [][]string{
		{"__2", `\d{1,3}`},
		{"_2", `\d{1,2}`},
		// Numeric time zone offsets
		{"-070000", `[+-]\d{6}`},
		{"-0700", `[+-]\d{4}`},
		{"-07", `[+-]\d{2}`},
		// timezone
		{"Z070000", `(?:\w)|(?:[+-]\d{6})`},
		{"Z0700", `(?:\w)|(?:[+-]\d{4})`},
		{"Z07", `(?:\w)|(?:[+-]\d{2})`},
	}

	i := 0
mainloop:
	for i < len(format) {
		for _, prefix := range prefixMap {
			if strings.HasPrefix(format[i:], prefix[0]) {
				addUnit(prefix[1])
				i += len(prefix[0])
				continue mainloop
			}
		}

		r, rSize := utf8.DecodeRuneInString(format[i:])
		switch true {
		case unicode.IsSpace(r):
			addUnit(`\s+`)
		case unicode.IsDigit(r):
			addUnit(`\d`)
		case unicode.IsLetter(r):
			addUnit(`\w`)
		case strings.ContainsRune(`.,+*?^$()[]{}|\-`, r):
			addUnit(`\` + string(r))
		default:
			addUnit(string(r))
		}
		i += rSize
	}

	for _, unitGroup := range groups {
		pattern += unitGroup.unitPattern
		if unitGroup.unitCount > 1 {
			pattern += fmt.Sprintf(`{%d}`, unitGroup.unitCount)
		}
	}
	return
}
