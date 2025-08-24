package persistence

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"heaplog_2024/internal"
	"heaplog_2024/internal/common"
	"heaplog_2024/internal/search"
)

func TestInterfaces(t *testing.T) {
	ctx := context.Background()
	logger, err := internal.NewLogger("test")
	require.NoError(t, err)
	db, err := NewDuckDB(ctx, "", logger)
	require.NoError(t, err)

	var _ search.ResultsStorage = db
}

func TestConcurrentPutResults(t *testing.T) {
	ctx := context.Background()
	logger, err := internal.NewLogger("test")
	require.NoError(t, err)
	db, err := NewDuckDB(ctx, "", logger)
	require.NoError(t, err)

	messages := []common.FileMessage{
		{
			File: "path1",
			Message: common.Message{
				MessageLayout: common.MessageLayout{
					Loc: common.Location{From: 0, To: 10},
				},
				Date: common.MakeTimeV("2024-01-01T00:00:00.000000+00:00"),
			},
		},
	}

	const numConcurrent = 1_000
	var results []common.SearchResult
	var doneChannels []<-chan struct{}

	// Start concurrent puts
	for i := 0; i < numConcurrent; i++ {
		result, done, err := db.PutResultsAsync(
			common.UserQuery{Query: "test query " + string(rune('A'+i)), FromDate: nil, ToDate: nil},
			slices.Values(messages),
		)
		require.NoError(t, err)
		results = append(results, result)
		doneChannels = append(doneChannels, done)
	}

	// Wait for all operations to complete
	for _, done := range doneChannels {
		<-done
	}

	// Verify results
	for _, result := range results {
		gotResult, err := db.GetResults([]int{result.Id})
		require.NoError(t, err)
		require.Equal(t, result.Query, gotResult[result.Id].Query)
		require.True(t, gotResult[result.Id].Finished)

		// Cleanup
		err = db.WipeResults(result.Id)
		require.NoError(t, err)
	}
}

func TestResults(t *testing.T) {
	ctx := context.Background()
	logger, err := internal.NewLogger("test")
	require.NoError(t, err)
	db, err := NewDuckDB(ctx, "", logger)
	require.NoError(t, err)

	messages := []common.FileMessage{
		{
			File: "path1",
			Message: common.Message{
				MessageLayout: common.MessageLayout{
					Loc: common.Location{From: 0, To: 10},
				},
				Date: common.MakeTimeV("2024-01-01T00:00:00.000000+00:00"),
			},
		},
		{
			File: "path1",
			Message: common.Message{
				MessageLayout: common.MessageLayout{
					Loc: common.Location{From: 10, To: 20},
				},
				Date: common.MakeTimeV("2024-01-01T00:00:01.000000+00:00"),
			},
		},
	}

	// Put results
	result, done, err := db.PutResultsAsync(
		common.UserQuery{Query: "test query", FromDate: nil, ToDate: nil},
		slices.Values(messages),
	)
	require.NoError(t, err)
	require.Equal(t, "test query", result.Query)
	require.False(t, result.Finished)

	<-done

	// Get results
	result.Messages = len(messages)
	result.Finished = true

	gotResult, err := db.GetResults([]int{result.Id})
	require.NoError(t, err)
	require.Equal(t, result, *gotResult[result.Id])

	// Get messages
	messagesSeq, err := db.GetResultMessages(result.Id, 0, 1000)
	require.NoError(t, err)
	gotMessages := slices.Collect(messagesSeq)
	require.Equal(t, messages, gotMessages)

	// Get messages + skip
	messagesSeq, err = db.GetResultMessages(result.Id, 1, 1000)
	require.NoError(t, err)
	gotMessages = slices.Collect(messagesSeq)
	require.Equal(t, messages[1:], gotMessages)

	// Get messages + limit
	messagesSeq, err = db.GetResultMessages(result.Id, 0, 1)
	require.NoError(t, err)
	gotMessages = slices.Collect(messagesSeq)
	require.Equal(t, messages[:1], gotMessages)

	// Wipe results
	err = db.WipeResults(result.Id)
	require.NoError(t, err)

	// Try to get wiped results
	r, err := db.GetResults([]int{result.Id})
	require.NoError(t, err)
	require.Nil(t, r[result.Id])

	// Try to get wiped messages
	messagesSeq, err = db.GetResultMessages(result.Id, 0, 1000)
	require.NoError(t, err)
	gotMessages = slices.Collect(messagesSeq)
	require.Empty(t, gotMessages)
}
