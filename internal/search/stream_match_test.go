package search

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"heaplog_2024/internal/common"
)

func TestReadMatch(t *testing.T) {
	testFile, _ := common.MakeTestFile(t)
	defer func() { _ = os.Remove(testFile) }()

	type test struct {
		messages         []common.Message
		matcher          SearchMatcher
		expectedMessages []common.Message
		err              error
	}

	tests := []test{
		{ // MATCH ALL
			matcher:          func(m common.Message, body []byte) bool { return true },
			expectedMessages: common.SampleLayouts,
		},
		{ // MATCH NONE
			matcher:          func(m common.Message, body []byte) bool { return false },
			expectedMessages: nil,
		},
		{ // MATCH ONE
			matcher: func(m common.Message, body []byte) bool {
				return bytes.Contains(
					body,
					[]byte("uploaded"),
				)
			},
			expectedMessages: common.SampleLayouts[2:3],
		},
	}

	for i, tt := range tests {
		t.Run(
			fmt.Sprintf("test %d", i), func(t *testing.T) {
				matchedIt, err := StreamFileMatch(testFile, common.SampleLayouts, tt.matcher)
				require.NoError(t, err)

				var matchedMessages []common.Message
				for m, err := range matchedIt {
					if err != nil {
						require.ErrorIs(t, err, tt.err)
					}
					matchedMessages = append(matchedMessages, m)
				}

				require.Equal(t, tt.expectedMessages, matchedMessages)
			},
		)
	}
}
