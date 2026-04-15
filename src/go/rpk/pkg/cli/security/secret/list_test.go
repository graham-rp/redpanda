// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package secret

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestPrintSecretList(t *testing.T) {
	tests := []struct {
		name     string
		format   string
		data     []secretListItem
		wantText []string // substrings expected in text output
	}{
		{
			name:   "text output with multiple scopes",
			format: "text",
			data: []secretListItem{
				{ID: "MY_SECRET", Scopes: []string{"redpanda_connect", "redpanda_cluster"}},
				{ID: "ANOTHER_SECRET", Scopes: []string{"redpanda_connect"}},
			},
			wantText: []string{
				"NAME", "SCOPES",
				"MY_SECRET", "redpanda_connect, redpanda_cluster",
				"ANOTHER_SECRET", "redpanda_connect",
			},
		},
		{
			name:   "text output with no scopes",
			format: "text",
			data: []secretListItem{
				{ID: "EMPTY_SECRET", Scopes: nil},
			},
			wantText: []string{"EMPTY_SECRET"},
		},
		{
			name:   "json output preserves scopes as array",
			format: "json",
			data: []secretListItem{
				{ID: "MY_SECRET", Scopes: []string{"redpanda_connect", "redpanda_cluster"}},
			},
			wantText: []string{`"id"`, `"MY_SECRET"`, `"scopes"`, `"redpanda_connect"`, `"redpanda_cluster"`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			f := config.OutFormatter{Kind: tt.format}
			printSecretList(f, tt.data, &buf)
			output := buf.String()
			for _, want := range tt.wantText {
				require.Contains(t, output, want)
			}
		})
	}
}

func TestPrintSecretListJSONStructure(t *testing.T) {
	data := []secretListItem{
		{ID: "SECRET_ONE", Scopes: []string{"redpanda_connect"}},
		{ID: "SECRET_TWO", Scopes: []string{"redpanda_connect", "redpanda_cluster"}},
	}

	var buf bytes.Buffer
	f := config.OutFormatter{Kind: "json"}
	printSecretList(f, data, &buf)

	var result []map[string]any
	err := json.Unmarshal(buf.Bytes(), &result)
	require.NoError(t, err)
	require.Len(t, result, 2)

	require.Equal(t, "SECRET_ONE", result[0]["id"])
	scopes, ok := result[0]["scopes"].([]any)
	require.True(t, ok)
	require.Len(t, scopes, 1)
	require.Equal(t, "redpanda_connect", scopes[0])

	require.Equal(t, "SECRET_TWO", result[1]["id"])
	scopes2, ok := result[1]["scopes"].([]any)
	require.True(t, ok)
	require.Len(t, scopes2, 2)

	// text output scopes are comma-joined (regression test)
	var textBuf bytes.Buffer
	tf := config.OutFormatter{Kind: "text"}
	printSecretList(tf, data, &textBuf)
	textOut := textBuf.String()
	require.True(t, strings.Contains(textOut, "redpanda_connect, redpanda_cluster"),
		"text output must comma-join scopes, got: %s", textOut)
}
