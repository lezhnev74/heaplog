package common

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const MessageStartPattern = `(?m)^\[(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}\.?(\d{6}([+-]\d\d:\d\d)?)?)]`
const TimeFormat = "2006-01-02T15:04:05.000000-07:00"
const SampleLog1 = `
[2024-07-29T00:02:49.231231+00:00] User login successful
src: 127.0.0.1:5000
[2024-07-29T01:07:21.923832+00:00] Connection timed out
[2024-07-29T01:11:38.258712+00:00] File uploaded successfully
{filesize: 1024, filename: "test.txt"}
[2024-07-30T00:12:22.799234+00:00] Database connection established
[2024-07-30T01:16:57.293873+00:00] Error reading configuration
[2024-07-30T02:20:36.908172+00:00] Service restarted
[2024-07-30T03:24:47.245671+00:00] User logged out
[2024-07-30T04:25:27.789664+00:00] Permission denied
[2024-07-30T05:28:56.918273+00:00] Cache cleared
[2024-07-30T06:29:23.685562+00:00] Backup completed
`
const SampleLog2 = `
[2024-07-31T00:02:49.231231+00:00] New user registration completed
User: john_doe
IP: 192.168.1.100
[2024-07-31T01:07:21.923832+00:00] System maintenance started
Details: Upgrading database to version 2.1.0
[2024-07-31T01:11:38.258712+00:00] Critical error detected
Stack trace: NullPointerException at line 127
Service: AuthService
[2024-07-31T02:15:22.799234+00:00] Batch processing completed
Items processed: 1500
Success rate: 99.9%
[2024-07-31T03:20:57.293873+00:00] Security alert
Multiple failed login attempts detected from IP: 10.0.0.55
[2024-07-31T04:25:36.908172+00:00] Cache invalidation triggered
Affected keys: users:*, sessions:*
[2024-07-31T05:30:47.245671+00:00] Backup process initiated
Target: full system backup
Estimated duration: 30 minutes
`

var LayoutsSampleLog1 = []Message{
	{MessageLayout{Loc: Location{1, 78}, DateLoc: Location{2, 34}}, MakeTimeV("2024-07-29T00:02:49.231231+00:00")},
	{MessageLayout{Loc: Location{78, 134}, DateLoc: Location{79, 111}}, MakeTimeV("2024-07-29T01:07:21.923832+00:00")},
	{MessageLayout{Loc: Location{134, 235}, DateLoc: Location{135, 167}}, MakeTimeV("2024-07-29T01:11:38.258712+00:00")},
	{MessageLayout{Loc: Location{235, 302}, DateLoc: Location{236, 268}}, MakeTimeV("2024-07-30T00:12:22.799234+00:00")},
	{MessageLayout{Loc: Location{302, 365}, DateLoc: Location{303, 335}}, MakeTimeV("2024-07-30T01:16:57.293873+00:00")},
	{MessageLayout{Loc: Location{365, 418}, DateLoc: Location{366, 398}}, MakeTimeV("2024-07-30T02:20:36.908172+00:00")},
	{MessageLayout{Loc: Location{418, 469}, DateLoc: Location{419, 451}}, MakeTimeV("2024-07-30T03:24:47.245671+00:00")},
	{MessageLayout{Loc: Location{469, 522}, DateLoc: Location{470, 502}}, MakeTimeV("2024-07-30T04:25:27.789664+00:00")},
	{MessageLayout{Loc: Location{522, 571}, DateLoc: Location{523, 555}}, MakeTimeV("2024-07-30T05:28:56.918273+00:00")},
	{MessageLayout{Loc: Location{571, 623}, DateLoc: Location{572, 604}}, MakeTimeV("2024-07-30T06:29:23.685562+00:00")},
}
var LayoutsSampleLog2 = []Message{
	{MessageLayout{Loc: Location{1, 101}, DateLoc: Location{2, 34}}, MakeTimeV("2024-07-31T00:02:49.231231+00:00")},
	{MessageLayout{Loc: Location{101, 208}, DateLoc: Location{102, 134}}, MakeTimeV("2024-07-31T01:07:21.923832+00:00")},
	{MessageLayout{Loc: Location{208, 334}, DateLoc: Location{209, 241}}, MakeTimeV("2024-07-31T01:11:38.258712+00:00")},
	{MessageLayout{Loc: Location{334, 438}, DateLoc: Location{335, 367}}, MakeTimeV("2024-07-31T02:15:22.799234+00:00")},
	{MessageLayout{Loc: Location{438, 547}, DateLoc: Location{439, 471}}, MakeTimeV("2024-07-31T03:20:57.293873+00:00")},
	{MessageLayout{Loc: Location{547, 646}, DateLoc: Location{548, 580}}, MakeTimeV("2024-07-31T04:25:36.908172+00:00")},
	{MessageLayout{Loc: Location{646, 764}, DateLoc: Location{647, 679}}, MakeTimeV("2024-07-31T05:30:47.245671+00:00")},
}

func MakeFileMessages(file string, messages []Message) (fm []FileMessage) {
	for _, m := range messages {
		fm = append(fm, FileMessage{file, m})
	}
	return
}

// MakeTestFile creates a temporary log file with sample log data for testing purposes.
// It returns the path to the created file.
func MakeTestFile(t *testing.T) (string, []byte) {
	dir := t.TempDir()
	testFile := filepath.Join(dir, "test.log")
	err := PopulateFiles(
		map[string][]byte{
			testFile: []byte(SampleLog1),
		},
	)
	require.NoError(t, err)
	return testFile, []byte(SampleLog1)
}

// PopulateFiles writes the provided content to files specified in the map.
// The map key is the file path and the value is the content to write.
// Returns an error if any file operation fails.
func PopulateFiles(c map[string][]byte) error {
	for p, content := range c {
		err := os.WriteFile(p, content, os.ModePerm)
		if err != nil {
			return err
		}
	}
	return nil
}

// MakeTimeV parses a time string in the specified TimeFormat and returns a time.Time value.
// The function assumes UTC timezone and panics if the time string cannot be parsed.
func MakeTimeV(value string) time.Time {
	t, err := time.ParseInLocation(TimeFormat, value, time.UTC)
	if err != nil {
		panic(err)
	}
	return t
}

func MakeTimeP(value string) *time.Time {
	t := MakeTimeV(value)
	return &t
}
