package persistence

import (
	"context"
	"database/sql"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"heaplog_2024/internal"
	"heaplog_2024/internal/common"
	"heaplog_2024/internal/search"
)

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
	var results []search.SearchResult
	var doneChannels []<-chan struct{}

	// Start concurrent puts
	for i := 0; i < numConcurrent; i++ {
		result, done, err := db.PutResultsAsync("test query "+string(rune('A'+i)), slices.Values(messages))
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
		gotResult, err := db.GetResults(result.Id)
		require.NoError(t, err)
		require.Equal(t, result.Query, gotResult.Query)
		require.True(t, gotResult.Finished)

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
	result, done, err := db.PutResultsAsync("test query", slices.Values(messages))
	require.NoError(t, err)
	require.Equal(t, "test query", result.Query)
	require.False(t, result.Finished)

	<-done

	// Get results
	result.Messages = len(messages)
	result.Finished = true

	gotResult, err := db.GetResults(result.Id)
	require.NoError(t, err)
	require.Equal(t, result, gotResult)

	// Get messages
	messagesSeq, err := db.GetResultMessages(result.Id)
	require.NoError(t, err)
	gotMessages := slices.Collect(messagesSeq)
	require.Equal(t, messages, gotMessages)

	// Wipe results
	err = db.WipeResults(result.Id)
	require.NoError(t, err)

	// Try to get wiped results
	_, err = db.GetResults(result.Id)
	require.ErrorIs(t, err, sql.ErrNoRows)

	// Try to get wiped messages
	messagesSeq, err = db.GetResultMessages(result.Id)
	require.NoError(t, err)
	gotMessages = slices.Collect(messagesSeq)
	require.Empty(t, gotMessages)
}
