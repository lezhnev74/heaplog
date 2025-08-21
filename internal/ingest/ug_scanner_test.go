package ingest

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"heaplog_2024/internal/common"
)

const MsgStartRe = `^\[([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}[+-][0-9]{2}:[0-9]{2})]`

func TestUgScannerLocations(t *testing.T) {
	sourceStream := []byte(`
[2024-07-30T00:00:04.769958+00:00] production.DEBUG: Shuffle jobs on the queue

trace: fe0fef7b-7770-42f8-8197-86e56bd87842
[2024-07-30T00:00:12.285087+00:00] production.INFO: Event dispatched: App\Domain\Account\Events\UserWasSeen
payload:
    user_id: 1216302
context:
    label: 386ddf2b-bd13-4f12-ac19-92262cbf9b63
    environment: production
    started_at: 1722297612274467
    user_id: null
    channel: { type: http, details: { method: get, url: 'https://abcde.io/api/user' } }
    extras: { ip: 185.202.221.74 }
event_emitted_at_format: '2024-07-30T00:00:12+00:00'

trace: 386ddf2b-bd13-4f12-ac19-92262cbf9b63
[2024-07-30T00:00:12.967490+00:00] production.DEBUG: analytics event result:  for data {"client_id":"237338923.1722297170","user_id":"1216302","events":[{"name":"be_user_created","params":{"value":1,"currency":"EUR"}}]}

trace: 80847f4b-c06e-4f2b-9b77-80c6428d925b
`)

	storageRoot, _ := os.MkdirTemp("", "")
	defer func() { _ = os.RemoveAll(storageRoot) }()
	filePath := filepath.Join(storageRoot, "sample.log")
	fileMap := map[string][]byte{
		filePath: sourceStream,
	}
	require.NoError(t, common.PopulateFiles(fileMap))

	type Test struct {
		locations       []common.Location
		expectedLayouts []ScannedMessage
	}
	tests := []Test{
		{ // all
			locations: nil,
			expectedLayouts: []ScannedMessage{
				{
					MessageLayout: common.MessageLayout{
						Loc:     common.Location{From: 1, To: 125},
						DateLoc: common.Location{From: 2, To: 34},
					},
				},
				{
					MessageLayout: common.MessageLayout{
						Loc:     common.Location{From: 125, To: 620},
						DateLoc: common.Location{From: 126, To: 158},
					},
				},
				{
					MessageLayout: common.MessageLayout{
						Loc:     common.Location{From: 620, To: 885},
						DateLoc: common.Location{From: 621, To: 653},
					},
					IsTail: true,
				},
			},
		},
		{ // no message start
			locations: []common.Location{
				{From: 0, To: 1},
			},
			expectedLayouts: nil,
		},
		{ // wrong locations
			locations: []common.Location{
				{From: 2000, To: 10000},
			},
			expectedLayouts: nil,
		},
		{ // All file as a single location
			locations: []common.Location{
				{From: 0, To: 10000},
			},
			expectedLayouts: []ScannedMessage{
				{
					MessageLayout: common.MessageLayout{
						Loc:     common.Location{From: 1, To: 125},
						DateLoc: common.Location{From: 2, To: 34},
					},
				},
				{
					MessageLayout: common.MessageLayout{
						Loc:     common.Location{From: 125, To: 620},
						DateLoc: common.Location{From: 126, To: 158},
					},
				},
				{
					MessageLayout: common.MessageLayout{
						Loc:     common.Location{From: 620, To: 885},
						DateLoc: common.Location{From: 621, To: 653},
					},
					IsTail: true,
				},
			},
		},
		{ // Location that contains only part of the date
			locations: []common.Location{
				{From: 0, To: 20},
			},
			expectedLayouts: []ScannedMessage{
				{
					MessageLayout: common.MessageLayout{
						Loc:     common.Location{From: 1, To: 125},
						DateLoc: common.Location{From: 2, To: 34},
					},
				},
			},
		},
		{ // Location that contains the date of the first message
			locations: []common.Location{
				{From: 0, To: 50},
			},
			expectedLayouts: []ScannedMessage{
				{
					MessageLayout: common.MessageLayout{
						Loc:     common.Location{From: 1, To: 125}, // right boundary is the next message or the eof
						DateLoc: common.Location{From: 2, To: 34},
					},
				},
			},
		},
		{ // Multiple location that contain messages
			locations: []common.Location{
				{From: 0, To: 50},
				{From: 610, To: 700},
			},
			expectedLayouts: []ScannedMessage{
				{
					MessageLayout: common.MessageLayout{
						Loc:     common.Location{From: 1, To: 125},
						DateLoc: common.Location{From: 2, To: 34},
					},
				},
				{
					MessageLayout: common.MessageLayout{
						Loc:     common.Location{From: 620, To: 885},
						DateLoc: common.Location{From: 621, To: 653},
					},
					IsTail: true,
				},
			},
		},
	}

	for i, tt := range tests {
		t.Run(
			fmt.Sprintf("Test %d", i), func(t *testing.T) {
				layouts, err := Scan(filePath, len(fileMap[filePath]), MsgStartRe, tt.locations)
				require.NoError(t, err)
				require.Equal(t, tt.expectedLayouts, slices.Collect(layouts))
			},
		)
	}
}

func TestUgScannerHuge(t *testing.T) {
	type ScannedMessage = common.MessageLayout
	sourceStream := []byte(`
[2024-07-30T00:00:04.769958+00:00] production.DEBUG: Shuffle jobs on the queue

trace: fe0fef7b-7770-42f8-8197-86e56bd87842
[2024-07-30T00:00:12.285087+00:00] production.INFO: Event dispatched: App\Domain\Account\Events\UserWasSeen
payload:
   user_id: 1216302
context:
   label: 386ddf2b-bd13-4f12-ac19-92262cbf9b63
   environment: production
   started_at: 1722297612274467
   user_id: null
   channel: { type: ui, details: { method: get, url: 'https://abcde.io/api/user' } }
   extras: { ip: 185.202.221.74 }
event_emitted_at_format: '2024-07-30T00:00:12+00:00'

trace: 386ddf2b-bd13-4f12-ac19-92262cbf9b63
[2024-07-30T00:00:12.967490+00:00] production.DEBUG: analytics event result:  for data {"client_id":"237338923.1722297170","user_id":"1216302","events":[{"name":"be_user_created","params":{"value":1,"currency":"EUR"}}]}

trace: 80847f4b-c06e-4f2b-9b77-80c6428d925b
`)
	hugeStream := make([]byte, 0)
	for i := 0; i < 1000; i++ {
		hugeStream = append(hugeStream, sourceStream...)
	}

	storageRoot, _ := os.MkdirTemp("", "")
	defer func() { _ = os.RemoveAll(storageRoot) }()
	filePath := filepath.Join(storageRoot, "sample.log")
	fileMap := map[string][]byte{
		filePath: hugeStream,
	}
	require.NoError(t, common.PopulateFiles(fileMap))

	messages, err := Scan(
		filePath,
		len(fileMap[filePath]),
		MsgStartRe,
		[]common.Location{{From: 0, To: 1_000_000}},
	)
	require.NoError(t, err)
	require.Len(t, slices.Collect(messages), 3000)
}
