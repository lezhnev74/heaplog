package db_test

import (
	"context"
	go_iterators "github.com/lezhnev74/go-iterators"
	"github.com/stretchr/testify/require"
	"heaplog_2024/common"
	"heaplog_2024/db"
	"heaplog_2024/test_util"
	"os"
	"slices"
	"testing"
	"time"
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
	defer os.RemoveAll(storageRoot)

	// 1. Imitate slow message ingestion
	ticks := make(chan bool)
	t0 := time.Now().Round(time.Microsecond)
	t1 := t0.Add(time.Second)
	t2 := t1.Add(time.Second)
	messages := []db.Message{
		{1, common.Location{0, 10}, common.Location{}, 1, &t0},
		{2, common.Location{10, 20}, common.Location{}, 1, &t1},
		{3, common.Location{2, 4}, common.Location{}, 2, &t2},
	}
	curMessage := 0
	it := go_iterators.NewCallbackIterator(func() (m db.Message, err error) {
		<-ticks // hold until tick is allowed
		if curMessage == len(messages) {
			err = go_iterators.EmptyIterator
		} else {
			m = messages[curMessage]
			curMessage++
		}
		return
	}, func() error { return nil })

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
