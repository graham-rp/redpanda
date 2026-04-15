// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package group

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestPrintGroupList(t *testing.T) {
	withState := []listedGroup{
		{Broker: 1, Group: "group-a", State: "Stable"},
		{Broker: 2, Group: "group-b", State: "Empty"},
	}
	withoutState := []listedGroup{
		{Broker: 1, Group: "group-a"},
		{Broker: 2, Group: "group-b"},
	}

	jsonWithState, err := json.Marshal(withState)
	require.NoError(t, err)
	yamlWithState, err := yaml.Marshal(withState)
	require.NoError(t, err)

	jsonWithoutState, err := json.Marshal(withoutState)
	require.NoError(t, err)
	yamlWithoutState, err := yaml.Marshal(withoutState)
	require.NoError(t, err)

	cases := []struct {
		name   string
		kind   string
		data   []listedGroup
		output string
	}{
		{
			name: "text with state",
			kind: "text",
			data: withState,
			output: "BROKER  GROUP    STATE\n" +
				"1       group-a  Stable\n" +
				"2       group-b  Empty\n",
		},
		{
			name: "text without state",
			kind: "text",
			data: withoutState,
			output: "BROKER  GROUP\n" +
				"1       group-a\n" +
				"2       group-b\n",
		},
		{
			name:   "json with state",
			kind:   "json",
			data:   withState,
			output: string(jsonWithState) + "\n",
		},
		{
			name:   "json without state",
			kind:   "json",
			data:   withoutState,
			output: string(jsonWithoutState) + "\n",
		},
		{
			name:   "yaml with state",
			kind:   "yaml",
			data:   withState,
			output: string(yamlWithState) + "\n",
		},
		{
			name:   "yaml without state",
			kind:   "yaml",
			data:   withoutState,
			output: string(yamlWithoutState) + "\n",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := config.OutFormatter{Kind: c.kind}
			b := &strings.Builder{}
			printGroupList(f, c.data, b)
			require.Equal(t, c.output, b.String())
		})
	}
}

func TestPrintGroupDelete(t *testing.T) {
	data := []deletedGroup{
		{Group: "group-a", Status: "OK"},
		{Group: "group-b", Status: "some error"},
	}

	jsonBytes, err := json.Marshal(data)
	require.NoError(t, err)
	yamlBytes, err := yaml.Marshal(data)
	require.NoError(t, err)

	cases := []struct {
		kind   string
		output string
	}{
		{
			kind: "text",
			output: "GROUP    STATUS\n" +
				"group-a  OK\n" +
				"group-b  some error\n",
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
			printGroupDelete(f, data, b)
			require.Equal(t, c.output, b.String())
		})
	}
}
