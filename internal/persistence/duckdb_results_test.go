package persistence

import (
	"context"
	"database/sql"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"heaplog_2024/internal"
	"heaplog_2024/internal/common"
)

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
