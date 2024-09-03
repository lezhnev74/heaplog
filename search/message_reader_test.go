package search

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"heaplog_2024/common"
	"heaplog_2024/test_util"
	"os"
	"testing"
)

func TestReadMessages(t *testing.T) {
	sampleFile1 := `
[2024-07-30T00:00:10.100160+00:00] payment error: invalid card
[2024-07-30T00:01:10.100170+00:00] event triggered: signup (userid:12)
[2024-07-30T00:02:10.383227+00:00] api failure: quota reached
[2024-07-30T00:03:30.449222+00:00] file error: no disk space
[2024-07-30T00:04:20.082156+00:00] payment accepted
`
	sampleFile2 := `
[2024-08-01T00:01:01.285087+00:00] event triggered: login (userid:39)
[2024-08-01T00:02:02.967490+00:00] payment error: no funds
`

	storageRoot := test_util.PrepareTempDir(t)
	defer os.RemoveAll(storageRoot)
	file1 := test_util.PopulateFile(storageRoot, []byte(sampleFile1))
	file2 := test_util.PopulateFile(storageRoot, []byte(sampleFile2))

	type test struct {
		addrs          []MessageAddr
		expectedErr    error
		expectedBodies [][]byte
	}

	tests := []test{
		{
			addrs: []MessageAddr{
				{file1, common.Location{1, 64}},
				{file1, common.Location{197, 258}},
			},
			expectedBodies: [][]byte{
				[]byte("[2024-07-30T00:00:10.100160+00:00] payment error: invalid card\n"),
				[]byte("[2024-07-30T00:03:30.449222+00:00] file error: no disk space\n"),
			},
		},
		{
			addrs: []MessageAddr{
				{file1, common.Location{1, 64}},
				{file1, common.Location{197, 258}},
				{file2, common.Location{71, 130}},
			},
			expectedBodies: [][]byte{
				[]byte("[2024-07-30T00:00:10.100160+00:00] payment error: invalid card\n"),
				[]byte("[2024-07-30T00:03:30.449222+00:00] file error: no disk space\n"),
				[]byte("[2024-08-01T00:02:02.967490+00:00] payment error: no funds\n"),
			},
		},
		{ // read from a missing file
			addrs: []MessageAddr{
				{file1, common.Location{1, 64}},
				{"unknown", common.Location{197, 258}},
				{file2, common.Location{71, 130}},
			},
			expectedBodies: [][]byte{
				[]byte("[2024-07-30T00:00:10.100160+00:00] payment error: invalid card\n"),
				[]byte("[2024-08-01T00:02:02.967490+00:00] payment error: no funds\n"),
			},
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			actual, err := ReadMessages(tt.addrs)
			if tt.expectedErr != nil {
				require.ErrorIs(t, err, tt.expectedErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expectedBodies, actual)
			}
		})
	}
}
