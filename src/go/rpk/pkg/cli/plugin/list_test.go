// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package plugin

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestPrintPluginList(t *testing.T) {
	data := []pluginRow{
		{Name: "cloud", Description: "Manage Redpanda Cloud", Installed: true},
		{Name: "byoc", Description: "Bring your own cloud", Installed: false},
		{Name: "mm3", Description: "MirrorMaker3", Installed: true, Message: "local binary sha256 differs from manifest sha256"},
	}

	t.Run("text output", func(t *testing.T) {
		var buf bytes.Buffer
		f := config.OutFormatter{Kind: "text"}
		printPluginList(f, data, &buf)

		out := buf.String()
		// installed plugins are prefixed with * in text output
		require.Contains(t, out, "*cloud")
		require.Contains(t, out, "Manage Redpanda Cloud")
		require.Contains(t, out, "byoc")
		require.Contains(t, out, "*mm3")
		require.Contains(t, out, "local binary sha256 differs from manifest sha256")
	})

	t.Run("json output", func(t *testing.T) {
		var buf bytes.Buffer
		f := config.OutFormatter{Kind: "json"}
		printPluginList(f, data, &buf)

		out := buf.String()
		var rows []pluginRow
		err := json.Unmarshal([]byte(strings.TrimSpace(out)), &rows)
		require.NoError(t, err)
		require.Len(t, rows, 3)
		require.Equal(t, "cloud", rows[0].Name)
		require.True(t, rows[0].Installed)
		require.Equal(t, "byoc", rows[1].Name)
		require.False(t, rows[1].Installed)
		require.Equal(t, "mm3", rows[2].Name)
		require.Equal(t, "local binary sha256 differs from manifest sha256", rows[2].Message)
	})
}

func TestPrintLocalPluginList(t *testing.T) {
	data := []localPluginRow{
		{Name: "cloud", Path: "/home/user/.local/bin/.rpk-cloud"},
		{Name: "byoc", Path: "/home/user/.local/bin/.rpk-byoc", Shadows: []string{"/usr/local/bin/.rpk-byoc", "/opt/bin/.rpk-byoc"}},
	}

	t.Run("text output", func(t *testing.T) {
		var buf bytes.Buffer
		f := config.OutFormatter{Kind: "text"}
		printLocalPluginList(f, data, &buf)

		out := buf.String()
		require.Contains(t, out, "cloud")
		require.Contains(t, out, "/home/user/.local/bin/.rpk-cloud")
		require.Contains(t, out, "byoc")
		require.Contains(t, out, "/usr/local/bin/.rpk-byoc")
		require.Contains(t, out, "/opt/bin/.rpk-byoc")
	})

	t.Run("json output", func(t *testing.T) {
		var buf bytes.Buffer
		f := config.OutFormatter{Kind: "json"}
		printLocalPluginList(f, data, &buf)

		out := buf.String()
		var rows []localPluginRow
		err := json.Unmarshal([]byte(strings.TrimSpace(out)), &rows)
		require.NoError(t, err)
		require.Len(t, rows, 2)
		require.Equal(t, "cloud", rows[0].Name)
		require.Equal(t, "/home/user/.local/bin/.rpk-cloud", rows[0].Path)
		require.Nil(t, rows[0].Shadows)
		require.Equal(t, "byoc", rows[1].Name)
		require.Equal(t, []string{"/usr/local/bin/.rpk-byoc", "/opt/bin/.rpk-byoc"}, rows[1].Shadows)
	})
}
