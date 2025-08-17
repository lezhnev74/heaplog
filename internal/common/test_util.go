package common

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

const MessageStartPattern = `(?m)^\[(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}\.?(\d{6}([+-]\d\d:\d\d)?)?)]`
const TimeFormat = "2006-01-02T15:04:05.000000-07:00"
const SampleLog = `
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

var SampleLayouts = []Message{
	{MessageLayout{Loc: Location{1, 78}}, MakeTimeV("2024-07-29T00:02:49.231231+00:00")},
	{MessageLayout{Loc: Location{78, 134}}, MakeTimeV("2024-07-29T01:07:21.923832+00:00")},
	{MessageLayout{Loc: Location{134, 235}}, MakeTimeV("2024-07-29T01:11:38.258712+00:00")},
	{MessageLayout{Loc: Location{235, 302}}, MakeTimeV("2024-07-30T00:12:22.799234+00:00")},
	{MessageLayout{Loc: Location{302, 365}}, MakeTimeV("2024-07-30T01:16:57.293873+00:00")},
	{MessageLayout{Loc: Location{365, 418}}, MakeTimeV("2024-07-30T02:20:36.908172+00:00")},
	{MessageLayout{Loc: Location{418, 469}}, MakeTimeV("2024-07-30T03:24:47.245671+00:00")},
	{MessageLayout{Loc: Location{469, 522}}, MakeTimeV("2024-07-30T04:25:27.789664+00:00")},
	{MessageLayout{Loc: Location{522, 571}}, MakeTimeV("2024-07-30T05:28:56.918273+00:00")},
	{MessageLayout{Loc: Location{571, 623}}, MakeTimeV("2024-07-30T06:29:23.685562+00:00")},
}

// MakeTestFile creates a temporary log file with sample log data for testing purposes.
// It returns the path to the created file.
func MakeTestFile(t *testing.T) (string, []byte) {
	dir := t.TempDir()
	testFile := filepath.Join(dir, "test.log")
	err := PopulateFiles(
		map[string][]byte{
			testFile: []byte(SampleLog),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return testFile, []byte(SampleLog)
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
