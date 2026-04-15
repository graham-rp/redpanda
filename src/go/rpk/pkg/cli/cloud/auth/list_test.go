// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package auth

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestPrintCloudAuthList(t *testing.T) {
	data := []cloudAuthRow{
		{Name: "acme-sso", Kind: "sso", Organization: "acme", OrganizationID: "org-123", Current: true},
		{Name: "acme-client", Kind: "client", Organization: "acme", OrganizationID: "org-123"},
	}

	t.Run("text marks current with asterisk", func(t *testing.T) {
		f := config.OutFormatter{Kind: "text"}
		var buf bytes.Buffer
		printCloudAuthList(f, data, &buf)
		output := buf.String()
		require.Contains(t, output, "acme-sso*")
		require.Contains(t, output, "acme-client")
		// Non-current should not have asterisk
		require.NotContains(t, output, "acme-client*")
	})

	t.Run("json uses current bool field not asterisk", func(t *testing.T) {
		f := config.OutFormatter{Kind: "json"}
		var buf bytes.Buffer
		printCloudAuthList(f, data, &buf)

		var rows []map[string]any
		require.NoError(t, json.Unmarshal(buf.Bytes(), &rows))
		require.Len(t, rows, 2)

		// Current row has current:true
		require.Equal(t, "acme-sso", rows[0]["name"])
		require.Equal(t, true, rows[0]["current"])

		// Non-current row omits current field (omitempty)
		require.Equal(t, "acme-client", rows[1]["name"])
		_, hasCurrent := rows[1]["current"]
		require.False(t, hasCurrent, "non-current auth should omit current field")
	})

	t.Run("json name has no asterisk suffix", func(t *testing.T) {
		f := config.OutFormatter{Kind: "json"}
		var buf bytes.Buffer
		printCloudAuthList(f, data, &buf)
		output := buf.String()
		require.NotContains(t, output, "acme-sso*", "JSON output must not contain asterisk in name")
	})

	t.Run("yaml uses current bool field not asterisk", func(t *testing.T) {
		f := config.OutFormatter{Kind: "yaml"}
		var buf bytes.Buffer
		printCloudAuthList(f, data, &buf)
		output := buf.String()
		require.Contains(t, output, "current: true")
		require.NotContains(t, output, "acme-sso*")
	})

	t.Run("text table headers", func(t *testing.T) {
		f := config.OutFormatter{Kind: "text"}
		var buf bytes.Buffer
		printCloudAuthList(f, []cloudAuthRow{}, &buf)
		output := strings.ToLower(buf.String())
		require.Contains(t, output, "name")
		require.Contains(t, output, "kind")
		require.Contains(t, output, "organization")
	})
}
