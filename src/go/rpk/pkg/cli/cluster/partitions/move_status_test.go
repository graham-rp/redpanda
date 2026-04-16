// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package partitions

import (
	"strings"
	"testing"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/stretchr/testify/require"
)

func TestBuildMoveStatuses(t *testing.T) {
	input := []rpadmin.ReconfigurationsResponse{
		{
			Ns:            "kafka",
			Topic:         "foo",
			PartitionID:   0,
			PartitionSize: 1000,
			BytesMoved:    500,
			BytesLeft:     500,
			PreviousReplicas: []rpadmin.Replica{
				{NodeID: 1, Core: 0},
				{NodeID: 2, Core: 0},
			},
			NewReplicas: []rpadmin.Replica{
				{NodeID: 1, Core: 0},
				{NodeID: 3, Core: 0},
			},
		},
		{
			Ns:               "kafka",
			Topic:            "bar",
			PartitionID:      1,
			PartitionSize:    0, // zero partition size → completion stays 0
			BytesMoved:       0,
			BytesLeft:        200,
			PreviousReplicas: []rpadmin.Replica{{NodeID: 2, Core: 0}},
			NewReplicas:      []rpadmin.Replica{{NodeID: 4, Core: 1}},
		},
	}

	statuses := buildMoveStatuses(input)
	require.Len(t, statuses, 2)

	require.Equal(t, "kafka/foo", statuses[0].NamespaceTopic)
	require.Equal(t, 0, statuses[0].Partition)
	require.Equal(t, []int{1, 2}, statuses[0].MovingFrom)
	require.Equal(t, []int{1, 3}, statuses[0].MovingTo)
	require.Equal(t, 50, statuses[0].CompletionPercent)
	require.Equal(t, 1000, statuses[0].PartitionSize)
	require.Equal(t, 500, statuses[0].BytesMoved)
	require.Equal(t, 500, statuses[0].BytesRemaining)

	require.Equal(t, "kafka/bar", statuses[1].NamespaceTopic)
	require.Equal(t, 0, statuses[1].CompletionPercent) // zero partition size
}

func TestPrintMoveStatus(t *testing.T) {
	statuses := []partitionMoveStatus{
		{
			NamespaceTopic:    "kafka/foo",
			Partition:         0,
			MovingFrom:        []int{1, 2},
			MovingTo:          []int{1, 3},
			CompletionPercent: 50,
			PartitionSize:     1024,
			BytesMoved:        512,
			BytesRemaining:    512,
		},
		{
			NamespaceTopic:    "kafka/bar",
			Partition:         1,
			MovingFrom:        []int{2},
			MovingTo:          []int{4},
			CompletionPercent: 0,
			PartitionSize:     200,
			BytesMoved:        0,
			BytesRemaining:    200,
		},
	}

	header := []string{"NAMESPACE-TOPIC", "PARTITION", "MOVING-FROM", "MOVING-TO", "COMPLETION-%", "PARTITION-SIZE", "BYTES-MOVED", "BYTES-REMAINING"}
	// Fields splits "[1 2]" into "[1" and "2]" — an acceptable trade for
	// staying tabwriter-padding-agnostic.
	cases := []struct {
		name  string
		human bool
		want  [][]string
	}{
		{
			name: "bytes",
			want: [][]string{
				header,
				{"kafka/foo", "0", "[1", "2]", "[1", "3]", "50", "1024", "512", "512"},
				{"kafka/bar", "1", "[2]", "[4]", "0", "200", "0", "200"},
			},
		},
		{
			name:  "human",
			human: true,
			want: [][]string{
				header,
				{"kafka/foo", "0", "[1", "2]", "[1", "3]", "50", "1.024kB", "512B", "512B"},
				{"kafka/bar", "1", "[2]", "[4]", "0", "200B", "0B", "200B"},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := config.OutFormatter{Kind: "text"}
			b := &strings.Builder{}
			printMoveStatus(f, statuses, c.human, b)
			require.Equal(t, c.want, out.TableRows(b.String()))
		})
	}
}

func TestPrintMoveStatusEmpty(t *testing.T) {
	b := &strings.Builder{}
	printMoveStatus(config.OutFormatter{Kind: "json"}, []partitionMoveStatus{}, false, b)
	require.Equal(t, "[]\n", b.String())
}
