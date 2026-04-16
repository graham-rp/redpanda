// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package brokers

import (
	"bytes"
	"strings"
	"testing"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/stretchr/testify/require"
)

func textFormatter() config.OutFormatter {
	return config.OutFormatter{Kind: "text"}
}

func jsonFormatter() config.OutFormatter {
	return config.OutFormatter{Kind: "json"}
}

func TestBuildDecommissionStatus(t *testing.T) {
	t.Run("basic partitions", func(t *testing.T) {
		dbs := rpadmin.DecommissionStatusResponse{
			Partitions: []rpadmin.DecommissionPartitions{
				{
					Ns:              "kafka",
					Topic:           "test",
					Partition:       0,
					MovingTo:        rpadmin.DecommissionMovingTo{NodeID: 3},
					PartitionSize:   1000,
					BytesMoved:      100,
					BytesLeftToMove: 900,
				},
			},
		}

		resp := buildDecommissionStatus(dbs, false)

		require.Len(t, resp.Partitions, 1)
		require.Equal(t, "kafka/test/0", resp.Partitions[0].Partition)
		require.Equal(t, 3, resp.Partitions[0].MovingTo)
		require.Equal(t, 10, resp.Partitions[0].CompletionPercent)
		require.Equal(t, 1000, resp.Partitions[0].PartitionSize)
		require.Nil(t, resp.Partitions[0].BytesMoved)
		require.Nil(t, resp.Partitions[0].BytesRemaining)
	})

	t.Run("detailed partitions", func(t *testing.T) {
		dbs := rpadmin.DecommissionStatusResponse{
			Partitions: []rpadmin.DecommissionPartitions{
				{
					Ns:              "kafka",
					Topic:           "test",
					Partition:       1,
					MovingTo:        rpadmin.DecommissionMovingTo{NodeID: 5},
					PartitionSize:   2000,
					BytesMoved:      500,
					BytesLeftToMove: 1500,
				},
			},
		}

		resp := buildDecommissionStatus(dbs, true)

		require.Len(t, resp.Partitions, 1)
		require.NotNil(t, resp.Partitions[0].BytesMoved)
		require.Equal(t, 500, *resp.Partitions[0].BytesMoved)
		require.NotNil(t, resp.Partitions[0].BytesRemaining)
		require.Equal(t, 1500, *resp.Partitions[0].BytesRemaining)
	})

	t.Run("zero partition size completion", func(t *testing.T) {
		dbs := rpadmin.DecommissionStatusResponse{
			Partitions: []rpadmin.DecommissionPartitions{
				{
					Ns:        "kafka",
					Topic:     "t",
					Partition: 0,
					MovingTo:  rpadmin.DecommissionMovingTo{NodeID: 1},
				},
			},
		}

		resp := buildDecommissionStatus(dbs, false)
		require.Equal(t, 0, resp.Partitions[0].CompletionPercent)
	})

	t.Run("reallocation failures", func(t *testing.T) {
		dbs := rpadmin.DecommissionStatusResponse{
			ReallocationFailureDetails: []rpadmin.ReallocationFailedPartition{
				{NS: "kafka", Topic: "foo", Partition: 1, Error: "not enough space"},
			},
		}

		resp := buildDecommissionStatus(dbs, false)

		require.Len(t, resp.ReallocationFailures, 1)
		require.Equal(t, "kafka/foo/1", resp.ReallocationFailures[0].Partition)
		require.Equal(t, "not enough space", resp.ReallocationFailures[0].Reason)
		require.Empty(t, resp.AllocationFailures)
	})

	t.Run("allocation failures", func(t *testing.T) {
		dbs := rpadmin.DecommissionStatusResponse{
			AllocationFailures: []string{"kafka/bar/0", "kafka/bar/1"},
		}

		resp := buildDecommissionStatus(dbs, false)

		require.Empty(t, resp.ReallocationFailures)
		require.Equal(t, []string{"kafka/bar/0", "kafka/bar/1"}, resp.AllocationFailures)
	})
}

func TestPrintDecommissionStatus(t *testing.T) {
	resp := decommissionStatusResponse{
		Partitions: []decommissionPartition{
			{Partition: "kafka/test/0", MovingTo: 3, CompletionPercent: 10, PartitionSize: 1000},
			{Partition: "kafka/test/1", MovingTo: 3, CompletionPercent: 50, PartitionSize: 2000},
		},
	}

	t.Run("text output has headers and rows", func(t *testing.T) {
		var buf bytes.Buffer
		printDecommissionStatus(textFormatter(), resp, false, false, &buf)
		output := buf.String()

		// Output starts with a section header (2 lines: title + underline)
		// followed by the table (header row + data rows).
		lines := strings.Split(strings.TrimSpace(output), "\n")
		require.GreaterOrEqual(t, len(lines), 5, "expected section header, underline, table header, separator, and data rows")
		headers := strings.Fields(lines[2])
		require.Equal(t, []string{"PARTITION", "MOVING-TO", "COMPLETION-%", "PARTITION-SIZE"}, headers)
		require.Contains(t, output, "kafka/test/0")
		require.Contains(t, output, "kafka/test/1")
	})

	t.Run("text output detailed has extra columns", func(t *testing.T) {
		moved := 100
		remaining := 900
		respDetailed := decommissionStatusResponse{
			Partitions: []decommissionPartition{
				{
					Partition:         "kafka/test/0",
					MovingTo:          3,
					CompletionPercent: 10,
					PartitionSize:     1000,
					BytesMoved:        &moved,
					BytesRemaining:    &remaining,
				},
			},
		}
		var buf bytes.Buffer
		printDecommissionStatus(textFormatter(), respDetailed, true, false, &buf)
		output := buf.String()

		// lines[0]=section title, lines[1]=underline, lines[2]=table headers
		lines := strings.Split(strings.TrimSpace(output), "\n")
		headers := strings.Fields(lines[2])
		require.Equal(t, []string{"PARTITION", "MOVING-TO", "COMPLETION-%", "PARTITION-SIZE", "BYTES-MOVED", "BYTES-REMAINING"}, headers)
	})

	t.Run("json output", func(t *testing.T) {
		var buf bytes.Buffer
		printDecommissionStatus(jsonFormatter(), resp, false, false, &buf)
		output := buf.String()

		require.Contains(t, output, `"partition"`)
		require.Contains(t, output, `"moving_to"`)
		require.Contains(t, output, `"completion_percent"`)
		require.Contains(t, output, `"partition_size"`)
		require.Contains(t, output, `"kafka/test/0"`)
	})

	t.Run("text output with reallocation failures", func(t *testing.T) {
		respFail := decommissionStatusResponse{
			ReallocationFailures: []reallocationFailure{
				{Partition: "kafka/foo/1", Reason: "no space"},
			},
			Partitions: []decommissionPartition{
				{Partition: "kafka/test/0", MovingTo: 3, CompletionPercent: 5, PartitionSize: 100},
			},
		}
		var buf bytes.Buffer
		printDecommissionStatus(textFormatter(), respFail, false, false, &buf)
		output := buf.String()

		require.Contains(t, output, "REALLOCATION FAILURE DETAILS")
		require.Contains(t, output, "kafka/foo/1")
		require.Contains(t, output, "no space")
		require.Contains(t, output, "DECOMMISSION PROGRESS")
	})

	t.Run("text output with allocation failures", func(t *testing.T) {
		respFail := decommissionStatusResponse{
			AllocationFailures: []string{"kafka/bar/0"},
			Partitions: []decommissionPartition{
				{Partition: "kafka/test/0", MovingTo: 3, CompletionPercent: 5, PartitionSize: 100},
			},
		}
		var buf bytes.Buffer
		printDecommissionStatus(textFormatter(), respFail, false, false, &buf)
		output := buf.String()

		require.Contains(t, output, "ALLOCATION FAILURES")
		require.Contains(t, output, "kafka/bar/0")
	})

	t.Run("human readable text output", func(t *testing.T) {
		var buf bytes.Buffer
		printDecommissionStatus(textFormatter(), resp, false, true, &buf)
		output := buf.String()

		// With human-readable, sizes should not be raw integers for large values.
		// For small values like 1000 bytes, it renders as "1.0 kB".
		require.NotContains(t, output, " 1000 ")
	})

	t.Run("json empty partitions", func(t *testing.T) {
		empty := decommissionStatusResponse{Partitions: []decommissionPartition{}}
		var buf bytes.Buffer
		printDecommissionStatus(jsonFormatter(), empty, false, false, &buf)
		require.Equal(t, `{"partitions":[]}`+"\n", buf.String())
	})
}
