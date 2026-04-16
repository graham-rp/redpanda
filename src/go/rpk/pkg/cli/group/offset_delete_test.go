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
	"errors"
	"strings"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"gopkg.in/yaml.v3"
)

func TestBuildOffsetDeleteResults(t *testing.T) {
	tests := []struct {
		name      string
		responses kadm.DeleteOffsetsResponses
		wantOK    bool
		wantOrder []offsetDeleteResult
	}{
		{
			name:      "empty responses",
			responses: kadm.DeleteOffsetsResponses{},
			wantOK:    true,
			wantOrder: nil,
		},
		{
			name: "single topic single partition success",
			responses: kadm.DeleteOffsetsResponses{
				"foo": {0: nil},
			},
			wantOK: true,
			wantOrder: []offsetDeleteResult{
				{Topic: "foo", Partition: 0, Status: "OK"},
			},
		},
		{
			name: "single topic single partition error",
			responses: kadm.DeleteOffsetsResponses{
				"foo": {0: errors.New("some error")},
			},
			wantOK: false,
			wantOrder: []offsetDeleteResult{
				{Topic: "foo", Partition: 0, Status: "some error"},
			},
		},
		{
			name: "topics sorted alphabetically",
			responses: kadm.DeleteOffsetsResponses{
				"zebra": {0: nil},
				"apple": {0: nil},
				"mango": {0: nil},
			},
			wantOK: true,
			wantOrder: []offsetDeleteResult{
				{Topic: "apple", Partition: 0, Status: "OK"},
				{Topic: "mango", Partition: 0, Status: "OK"},
				{Topic: "zebra", Partition: 0, Status: "OK"},
			},
		},
		{
			name: "partitions sorted numerically",
			responses: kadm.DeleteOffsetsResponses{
				"foo": {3: nil, 1: nil, 0: nil, 2: nil},
			},
			wantOK: true,
			wantOrder: []offsetDeleteResult{
				{Topic: "foo", Partition: 0, Status: "OK"},
				{Topic: "foo", Partition: 1, Status: "OK"},
				{Topic: "foo", Partition: 2, Status: "OK"},
				{Topic: "foo", Partition: 3, Status: "OK"},
			},
		},
		{
			name: "mixed success and error marks ok=false",
			responses: kadm.DeleteOffsetsResponses{
				"foo": {
					0: nil,
					1: errors.New("bad partition"),
				},
			},
			wantOK: false,
			wantOrder: []offsetDeleteResult{
				{Topic: "foo", Partition: 0, Status: "OK"},
				{Topic: "foo", Partition: 1, Status: "bad partition"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := buildOffsetDeleteResults(tt.responses)
			require.Equal(t, tt.wantOK, ok)
			require.Equal(t, tt.wantOrder, got)
		})
	}
}

func TestPrintOffsetDeleteResults(t *testing.T) {
	results := []offsetDeleteResult{
		{Topic: "apple", Partition: 0, Status: "OK"},
		{Topic: "apple", Partition: 1, Status: "some error"},
		{Topic: "zebra", Partition: 0, Status: "OK"},
	}

	wantJSON, err := json.Marshal(results)
	require.NoError(t, err)
	wantYAML, err := yaml.Marshal(results)
	require.NoError(t, err)

	tests := []struct {
		kind string
		want string
	}{
		{
			kind: "text",
			want: "TOPIC  PARTITION  STATUS\napple  0          OK\napple  1          some error\nzebra  0          OK\n",
		},
		{
			kind: "json",
			want: string(wantJSON) + "\n",
		},
		{
			kind: "yaml",
			want: string(wantYAML) + "\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.kind, func(t *testing.T) {
			f := config.OutFormatter{Kind: tt.kind}
			b := &strings.Builder{}
			printOffsetDeleteResults(f, results, b)
			require.Equal(t, tt.want, b.String())
		})
	}
}
