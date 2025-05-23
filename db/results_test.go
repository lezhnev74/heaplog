package db_test

import (
	"context"
	"os"
	"slices"
	"testing"
	"time"

	"heaplog_2024/common"
	"heaplog_2024/db"
	"heaplog_2024/test_util"

	"github.com/stretchr/testify/require"
)

func cmpResults(a, b []db.Message) bool {
	// results use the same data structure as Message but not all fields
	eq := slices.CompareFunc(a, b, func(m1, m2 db.Message) int {
		m1.Date = nil
		m2.Date = nil
		if m1.FileId != m2.FileId {
			return -1
		}
		if m1.Loc.From != m2.Loc.From && m1.Loc.To != m2.Loc.To {
			return -1
		}
		return 0
	})
	return eq == 0
}

func TestResultsRead(t *testing.T) {
	// Test plan:
	// 1. Imitate slow message ingestion
	// 2. Read query page:
	// 2.1. before first result
	// 2.2. in the middle of results
	// 2.3. after Finished
	// 3. Test the query state

	// Exec:
	dbContainer, storageRoot := test_util.PrepareTestDb(t)
	defer func() { _ = os.RemoveAll(storageRoot) }()

	// 1. Imitate slow message ingestion
	ticks := make(chan bool)
	t0 := time.Now().Round(time.Microsecond)
	t1 := t0.Add(time.Second)
	t2 := t1.Add(time.Second)
	messages := []db.Message{
		{1, common.Location{From: 0, To: 10}, common.Location{}, 1, &t0},
		{2, common.Location{From: 10, To: 20}, common.Location{}, 1, &t1},
		{3, common.Location{From: 2, To: 4}, common.Location{}, 2, &t2},
	}
	it := func(yield func(val common.ErrVal[db.Message]) bool) {
		for _, m := range messages {
			<-ticks // hold until tick is allowed
			yield(common.ErrVal[db.Message]{Val: m})
		}
	}

	r, err := dbContainer.QueryDB.CheckinQuery(context.Background(), "sample", &t0, &t2, it)
	require.NoError(t, err)

	require.False(t, r.Finished)
	require.Equal(t, 0, r.Messages)
	require.Equal(t, &t0, r.Min)
	require.Equal(t, &t2, r.Max)

	dbContainer.QueryDB.Flush()

	// 2. Read query page:
	// 2.1. before first result
	page, err := dbContainer.QueryDB.Page(r.Id, nil, nil, 0, 100)
	require.NoError(t, err)
	require.Equal(t, []db.Message(nil), page)

	// 2.2. in the middle of results
	ticks <- true // allow one message to pass in
	time.Sleep(200 * time.Millisecond)
	dbContainer.QueryDB.Flush() // see the message
	time.Sleep(200 * time.Millisecond)

	page, err = dbContainer.QueryDB.Page(r.Id, nil, nil, 0, 100)
	require.NoError(t, err)
	require.Equal(t, true, cmpResults(messages[0:1], page))

	// 2.3. after Finished
	close(ticks)
	time.Sleep(200 * time.Millisecond)
	dbContainer.QueryDB.Flush() // see the message

	page, err = dbContainer.QueryDB.Page(r.Id, nil, nil, 0, 100)
	require.NoError(t, err)
	require.Equal(t, true, cmpResults(messages, page))

	// 3. Test the query state
	r, err = dbContainer.QueryDB.FindQuery(r.Id)
	require.NoError(t, err)
	require.Equal(t, true, r.Finished)
	require.Equal(t, 3, r.Messages)
	require.Equal(t, t0, *r.Min)
	require.Equal(t, t2, *r.Max)
}

func TestStreamResults(t *testing.T) {
	// Test plan:
	// 1. Imitate slow message ingestion
	// 2. Stream query results
	// 3. Test the query state

	// Exec:
	dbContainer, storageRoot := test_util.PrepareTestDb(t)
	defer func() { _ = os.RemoveAll(storageRoot) }()

	// 1. Imitate slow message ingestion
	t0 := time.Now().Round(time.Microsecond)
	t1 := t0.Add(time.Second)
	t2 := t1.Add(time.Second)
	messages := []db.Message{
		{1, common.Location{From: 0, To: 10}, common.Location{}, 1, &t0},
		{2, common.Location{From: 10, To: 20}, common.Location{}, 1, &t1},
		{3, common.Location{From: 2, To: 4}, common.Location{}, 2, &t2},
	}
	it := func(yield func(val common.ErrVal[db.Message]) bool) {
		for _, m := range messages {
			yield(common.ErrVal[db.Message]{Val: m})
		}
	}

	r, err := dbContainer.QueryDB.CheckinQuery(context.Background(), "sample", &t0, &t2, it)
	require.NoError(t, err)

	dbContainer.QueryDB.Flush()

	// 2. Stream query results:
	streamIt := dbContainer.QueryDB.Stream(r.Id, nil, nil)
	require.NoError(t, err)
	streamedMessages := slices.Collect(streamIt)

	// 3. Test
	// normalize streamed results as they do not include the date column
	require.Len(t, streamedMessages, len(messages))
	for i := 0; i < len(messages); i++ {
		require.Equal(t, messages[i].FileId, streamedMessages[i].Val.FileId)
		require.Equal(t, messages[i].Loc.From, streamedMessages[i].Val.Loc.From)
	}
}
