// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package topic

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestPrintTopicDeleteResults(t *testing.T) {
	results := []topicDeleteResult{
		{Topic: "foo", Status: "OK"},
		{Topic: "bar", Status: "UNKNOWN_TOPIC_OR_PARTITION: topic not found"},
	}

	tests := []struct {
		name     string
		kind     string
		expected string
	}{
		{
			name: "text output",
			kind: "text",
			expected: "TOPIC  STATUS\n" +
				"foo    OK\n" +
				"bar    UNKNOWN_TOPIC_OR_PARTITION: topic not found\n",
		},
		{
			name: "json output",
			kind: "json",
		},
		{
			name: "yaml output",
			kind: "yaml",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := config.OutFormatter{Kind: tt.kind}
			b := &strings.Builder{}
			printTopicDeleteResults(f, results, b)

			switch tt.kind {
			case "text":
				require.Equal(t, tt.expected, b.String())
			case "json":
				var got []topicDeleteResult
				require.NoError(t, json.Unmarshal([]byte(b.String()), &got))
				require.Equal(t, results, got)
			case "yaml":
				var got []topicDeleteResult
				require.NoError(t, yaml.Unmarshal([]byte(b.String()), &got))
				require.Equal(t, results, got)
			}
		})
	}
}
