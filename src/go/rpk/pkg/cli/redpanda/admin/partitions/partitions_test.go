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
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestPrintAdminPartitionList(t *testing.T) {
	data := []partitionResponse{
		{Topic: "topic-a", Partition: 0, IsLeader: true},
		{Topic: "topic-a", Partition: 1, IsLeader: false},
		{Topic: "topic-b", Partition: 0, IsLeader: true},
	}

	t.Run("text output has correct headers and rows", func(t *testing.T) {
		var buf bytes.Buffer
		f := config.OutFormatter{Kind: "text"}
		printAdminPartitionList(f, data, &buf)
		output := buf.String()

		lines := strings.Split(strings.TrimSpace(output), "\n")
		require.GreaterOrEqual(t, len(lines), 4) // header + 3 rows

		header := strings.Fields(lines[0])
		require.Equal(t, []string{"TOPIC", "PARTITION", "IS-LEADER"}, header)

		row1 := strings.Fields(lines[1])
		require.Equal(t, []string{"topic-a", "0", "true"}, row1)

		row2 := strings.Fields(lines[2])
		require.Equal(t, []string{"topic-a", "1", "false"}, row2)

		row3 := strings.Fields(lines[3])
		require.Equal(t, []string{"topic-b", "0", "true"}, row3)
	})

	t.Run("json output has correct fields", func(t *testing.T) {
		var buf bytes.Buffer
		f := config.OutFormatter{Kind: "json"}
		printAdminPartitionList(f, data, &buf)

		var got []partitionResponse
		require.NoError(t, json.Unmarshal(buf.Bytes(), &got))
		require.Equal(t, data, got)
	})

	t.Run("empty data produces empty text table", func(t *testing.T) {
		var buf bytes.Buffer
		f := config.OutFormatter{Kind: "text"}
		printAdminPartitionList(f, nil, &buf)
		output := strings.TrimSpace(buf.String())
		// Only the header line should appear for an empty table.
		lines := strings.Split(output, "\n")
		require.Equal(t, 1, len(lines))
		header := strings.Fields(lines[0])
		require.Equal(t, []string{"TOPIC", "PARTITION", "IS-LEADER"}, header)
	})
}

func TestBuildPartitionList(t *testing.T) {
	cases := []struct {
		name       string
		leaderOnly bool
		brokerID   int
		// topic -> list of (partition, leader, replicas)
		want []partitionResponse
	}{
		{
			name:       "all partitions for broker 1",
			brokerID:   1,
			leaderOnly: false,
			want: []partitionResponse{
				{Topic: "t", Partition: 0, IsLeader: true},
				{Topic: "t", Partition: 1, IsLeader: false},
			},
		},
		{
			name:       "leader-only for broker 1",
			brokerID:   1,
			leaderOnly: true,
			want: []partitionResponse{
				{Topic: "t", Partition: 0, IsLeader: true},
			},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			// partition 0: leader=1, replicas=[1,2]
			// partition 1: leader=2, replicas=[1,2]
			type partData struct {
				partition int32
				leader    int32
				replicas  []int32
			}
			topicParts := []partData{
				{partition: 0, leader: 1, replicas: []int32{1, 2}},
				{partition: 1, leader: 2, replicas: []int32{1, 2}},
			}
			var got []partitionResponse
			for _, pd := range topicParts {
				for _, rs := range pd.replicas {
					if int(rs) == tt.brokerID {
						isLeader := int(pd.leader) == tt.brokerID
						if isLeader || !tt.leaderOnly {
							got = append(got, partitionResponse{
								Topic:     "t",
								Partition: pd.partition,
								IsLeader:  isLeader,
							})
						}
					}
				}
			}
			require.Equal(t, tt.want, got)
		})
	}
}
