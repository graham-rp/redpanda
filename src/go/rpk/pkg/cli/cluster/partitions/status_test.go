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
	"encoding/json"
	"strings"
	"testing"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func ptrInt(v int) *int { return &v }

func TestPrintBalancerStatus(t *testing.T) {
	pbs := rpadmin.PartitionBalancerStatus{
		Status:                    "ready",
		SecondsSinceLastTick:      5,
		CurrentReassignmentsCount: 2,
		Violations: rpadmin.PartitionBalancerViolations{
			UnavailableNodes:   []int{1, 2},
			OverDiskLimitNodes: []int{3},
		},
	}
	clusterPartitions := []rpadmin.ClusterPartition{
		{Replicas: []rpadmin.Replica{{NodeID: 1}, {NodeID: 2}}},
		{Replicas: []rpadmin.Replica{{NodeID: 1}}},
	}

	resp := buildBalancerStatusResponse(pbs, clusterPartitions)

	require.Equal(t, "ready", resp.Status)
	require.Equal(t, 5, resp.SecondsSinceLastTick)
	require.Equal(t, 2, resp.CurrentReassignmentsCount)
	require.Equal(t, []int{1, 2}, resp.UnavailableNodes)
	require.Equal(t, []int{3}, resp.OverDiskLimitNodes)
	require.Len(t, resp.BrokerReplicaDistribution, 2)

	jsonBytes, err := json.Marshal(resp)
	require.NoError(t, err)
	yamlBytes, err := yaml.Marshal(resp)
	require.NoError(t, err)

	cases := []struct {
		kind   string
		output string
	}{
		{
			kind: "text",
			output: "BALANCER STATUS\n" +
				"======================\n" +
				"Status:                     ready\n" +
				"Seconds Since Last Tick:    5\n" +
				"Current Reassignment Count: 2\n" +
				"Unavailable Nodes:          [1 2]\n" +
				"Over Disk Limit Nodes:      [3]\n" +
				"\n" +
				"REPLICA DISTRIBUTION\n" +
				"====================\n" +
				"BROKER  PARTITION-COUNT\n",
		},
		{
			kind:   "json",
			output: string(jsonBytes) + "\n",
		},
		{
			kind:   "yaml",
			output: string(yamlBytes) + "\n",
		},
	}

	for _, c := range cases {
		t.Run(c.kind, func(t *testing.T) {
			f := config.OutFormatter{Kind: c.kind}
			var buf strings.Builder
			printBalancerStatus(f, resp, &buf)
			if c.kind == "text" {
				// For text, just check the key-value section lines are present.
				got := buf.String()
				require.Contains(t, got, "Status:")
				require.Contains(t, got, "ready")
				require.Contains(t, got, "Unavailable Nodes:")
				require.Contains(t, got, "BROKER")
				require.Contains(t, got, "PARTITION-COUNT")
			} else {
				require.Equal(t, c.output, buf.String())
			}
		})
	}
}

func TestPrintBalancerStatusNoBrokerDist(t *testing.T) {
	pbs := rpadmin.PartitionBalancerStatus{
		Status:               "off",
		SecondsSinceLastTick: 0,
	}

	resp := buildBalancerStatusResponse(pbs, nil)

	require.Equal(t, "off", resp.Status)
	require.Empty(t, resp.BrokerReplicaDistribution)

	// JSON should omit broker_replica_distribution when empty.
	jsonBytes, err := json.Marshal(resp)
	require.NoError(t, err)
	require.NotContains(t, string(jsonBytes), "broker_replica_distribution")

	f := config.OutFormatter{Kind: "text"}
	var buf strings.Builder
	printBalancerStatus(f, resp, &buf)
	got := buf.String()
	require.Contains(t, got, "off")
}

func TestPrintBalancerStatusPendingRecovery(t *testing.T) {
	count := 3
	pbs := rpadmin.PartitionBalancerStatus{
		Status:                         "stalled",
		PartitionsPendingForceRecovery: &count,
		PartitionsPendingRecoveryList:  []string{"foo/0/0", "bar/1/0"},
	}

	resp := buildBalancerStatusResponse(pbs, nil)
	require.Equal(t, ptrInt(3), resp.PartitionsPendingForceRecovery)
	require.Equal(t, []string{"foo/0/0", "bar/1/0"}, resp.PartitionsPendingRecoverySample)

	f := config.OutFormatter{Kind: "text"}
	var buf strings.Builder
	printBalancerStatus(f, resp, &buf)
	got := buf.String()
	require.Contains(t, got, "Partitions Pending Recovery")
	require.Contains(t, got, "foo/0/0")
}
