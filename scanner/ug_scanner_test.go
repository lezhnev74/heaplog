package scanner_test

import (
	go_iterators "github.com/lezhnev74/go-iterators"
	"github.com/stretchr/testify/require"
	"heaplog_2024/scanner"
	"heaplog_2024/test_util"
	"os"
	"testing"
)

func TestUgScanner(t *testing.T) {
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
	defer os.RemoveAll(storageRoot)
	file := test_util.PopulateFile(storageRoot, sourceStream)

	re := `^\[([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}[+-][0-9]{2}:[0-9]{2})]`
	it, err := scanner.UgScan(file, re)
	require.NoError(t, err)

	messages := go_iterators.ToSlice(it)
	require.NoError(t, it.Close())
	expectedMessages := []scanner.MessageLayout{
		{
			From:     1,
			To:       125,
			DateFrom: 2,
			DateTo:   34,
		},
		{
			From:     125,
			To:       620,
			DateFrom: 126,
			DateTo:   158,
		},
		{
			From:     620,
			To:       885,
			DateFrom: 621,
			DateTo:   653,
			IsTail:   true,
		},
	}
	require.Equal(t, expectedMessages, messages)
}