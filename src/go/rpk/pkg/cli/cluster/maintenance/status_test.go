// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package maintenance

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestBuildMaintenanceStatuses(t *testing.T) {
	brokers := []rpadmin.Broker{
		{
			NodeID: 1,
			Maintenance: &rpadmin.MaintenanceStatus{
				Draining:     true,
				Finished:     new(true),
				Errors:       new(false),
				Partitions:   new(5),
				Eligible:     new(3),
				Transferring: new(1),
				Failed:       new(0),
			},
		},
		{
			NodeID:      2,
			Maintenance: &rpadmin.MaintenanceStatus{Draining: false},
		},
	}

	statuses := buildMaintenanceStatuses(brokers)
	require.Len(t, statuses, 2)

	require.Equal(t, 1, statuses[0].NodeID)
	require.True(t, statuses[0].Enabled)
	require.Equal(t, new(true), statuses[0].Finished)
	require.Equal(t, new(false), statuses[0].Errors)
	require.Equal(t, new(5), statuses[0].Partitions)

	require.Equal(t, 2, statuses[1].NodeID)
	require.False(t, statuses[1].Enabled)
	require.Nil(t, statuses[1].Finished)
}

func TestPrintMaintenanceStatus(t *testing.T) {
	statuses := []brokerMaintenanceStatus{
		{
			NodeID:       1,
			Enabled:      true,
			Finished:     new(true),
			Errors:       new(false),
			Partitions:   new(5),
			Eligible:     new(3),
			Transferring: new(1),
			Failed:       new(0),
		},
		{
			NodeID:  2,
			Enabled: false,
		},
	}

	jsonBytes, err := json.Marshal(statuses)
	require.NoError(t, err)
	yamlBytes, err := yaml.Marshal(statuses)
	require.NoError(t, err)

	cases := []struct {
		kind   string
		output string
	}{
		{
			kind: "text",
			output: "NODE-ID  ENABLED  FINISHED  ERRORS  PARTITIONS  ELIGIBLE  TRANSFERRING  FAILED\n" +
				"1        true     true      false   5           3         1             0\n" +
				"2        false    -         -       -           -         -             -\n",
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
			b := &strings.Builder{}
			printMaintenanceStatus(f, statuses, b)
			require.Equal(t, c.output, b.String())
		})
	}
}
