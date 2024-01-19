package scanner

import (
	"fmt"
	"github.com/araddon/dateparse"
	"golang.org/x/xerrors"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
	"unsafe"
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
	Body, Date       []byte
	Pos, Len         int // body in the stream
	DateFrom, DateTo int // [from,to) in the body
	DateTime         time.Time
	IsTail           bool // sets to true if message ended with EOF
	Err              error
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

	// buffers are used in concurrent environment.
	// allows reusing memory for multiple reads
	bufPool sync.Pool
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

		bufPool: sync.Pool{
			New: func() any { return make([]byte, readSize) },
		},
	}
}

func (sc *Scanner) newMessage(body []byte, pos int, dateFrom, dateTo int, isTail bool) (*ScannedMessage, error) {
	// avoid allocation for the date string:
	dateSlice := body[dateFrom:dateTo]
	dateString := unsafe.String(unsafe.SliceData(dateSlice), len(dateSlice))
	d, err := time.Parse(sc.dateLayout, dateString)
	if err != nil {
		return nil, xerrors.Errorf("date parsing fail: %w", err)
	}

	return &ScannedMessage{
		// note that the body and the date are pointing to the shared buf
		// must be used before the scanner moves on and reuses the buffer for other data
		Body: body,
		Date: dateSlice,

		DateFrom: dateFrom,
		DateTo:   dateTo,
		Pos:      pos,
		Len:      len(body),
		DateTime: d,
		IsTail:   isTail,
	}, nil
}

// Scan is the main code that extracts messages.
func (sc *Scanner) Scan(
	src io.Reader,
	found func(message *ScannedMessage) (shouldStop bool), // call on each found message, if returns false -> iteration stops
) error {
	buf := sc.bufPool.Get().([]byte)
	defer func() { sc.bufPool.Put(buf) }()
	buf = buf[:sc.readSize]

	var (
		// workable area is buf's area that contains unprocessed data
		msgStartPos, // last detected message start in the workable area
		msgStartMatchEnd, // detected msg's pattern match end
		dateStartPos, // last message's date start relatively to the message
		dateStartMatchEnd, // last message's date end relatively to the message
		bufLen, // len of workable area
		bytesProcessed, // bytes that were processed before the workable area
		msgsProcessed, readBytes int
		detectedMessage *ScannedMessage
	)

	// 1. Read initial data To the buffer
	bufLen, err := src.Read(buf)
	if bufLen == 0 && err != nil { // (bufLen == 0 and err == nil)?
		return err
	}

	// 2. Find the start of a message in the workable space
	m := sc.messageStart.FindSubmatchIndex(buf[:bufLen])
	if m == nil {
		err = xerrors.Errorf("%w: checked %d bytes", NoMessageStartFound, bufLen)
		return err
	}
	msgStartPos, msgStartMatchEnd = m[0], m[1]
	dateStartPos, dateStartMatchEnd = m[2]-m[0], m[3]-m[0]
	// msgDate := append([]byte{}, buf[m[2]:m[3]]...)
	for {
		// Find the start of a message since msgStart+msgStartMatchEnd
		m = sc.messageStart.FindSubmatchIndex(buf[msgStartMatchEnd:bufLen])
		if m != nil {
			detectedMessage, err = sc.newMessage(
				buf[msgStartPos:msgStartMatchEnd+m[0]],
				bytesProcessed+msgStartPos,
				dateStartPos,
				dateStartMatchEnd,
				false,
			)
			if err != nil {
				return xerrors.Errorf("message make: %w", err)
			}
			if found(detectedMessage) { // should stop?
				return nil // halt scanning BEFORE returning a message
			}

			dateStartPos, dateStartMatchEnd = m[2]-m[0], m[3]-m[0]
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
		if bufLen == len(buf) {
			// respect max buf size
			if len(buf) >= sc.maxBufSize {
				return xerrors.Errorf("%w: %d bytes (consider increasing the max buffer size)", MaxBufSizeReached, sc.maxBufSize)
			}
			// Extend the buf
			buf2 := make([]byte, len(buf)+sc.readSize)
			copy(buf2, buf)
			buf = buf2
		}
		// try To extend the workable area with new data
		readBytes, err = src.Read(buf[bufLen:])
		if readBytes == 0 && err == io.EOF { // (bufLen == 0 and err == nil)?
			// here if EOF found it will be used as the right ScannedMessage boundary,
			// however if the writing is not atomic, we could only capture part of the ScannedMessage
			// due To seeing EOF between writes
			detectedMessage, err = sc.newMessage(
				buf[msgStartPos:bufLen],
				bytesProcessed+msgStartPos,
				dateStartPos,
				dateStartMatchEnd,
				true,
			)
			if err != nil {
				return xerrors.Errorf("message make: %w", err)
			}
			if found(detectedMessage) { // should stop?
				return nil // halt scanning BEFORE returning a message
			}
			msgsProcessed++
			return nil
		}
		bufLen += readBytes
		continue
	}
}

// ScanAll reads out all data From the reader and returns matching messages
// it uses messageStart pattern To delimit messages in the stream
// it stops when no more bytes available in the reader (EOF)
// note: used for testing only as it allocates a lot
func (sc *Scanner) ScanAll(s io.Reader) (messages []*ScannedMessage, err error) {
	err = sc.Scan(s, func(m *ScannedMessage) bool {
		m.Body = append([]byte{}, m.Body...)
		m.Date = append([]byte{}, m.Date...)
		messages = append(messages, m)
		return false
	})
	return
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
