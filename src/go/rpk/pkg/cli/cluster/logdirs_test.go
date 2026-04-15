// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cluster

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestPrintLogDirs(t *testing.T) {
	rows := []logDirRow{
		{Broker: 1, Dir: "/var/lib/redpanda/data", Topic: "foo", Partition: 0, Size: 1024},
		{Broker: 1, Dir: "/var/lib/redpanda/data", Topic: "foo", Partition: 1, Size: 2048},
		{Broker: 2, Dir: "/var/lib/redpanda/data", Topic: "bar", Partition: 0, Size: 512, Error: "some error"},
	}

	t.Run("text output has all columns", func(t *testing.T) {
		var buf bytes.Buffer
		f := config.OutFormatter{Kind: "text"}
		printLogDirs(f, rows, "partition", false, false, &buf)
		out := buf.String()
		require.Contains(t, out, "BROKER")
		require.Contains(t, out, "DIR")
		require.Contains(t, out, "TOPIC")
		require.Contains(t, out, "PARTITION")
		require.Contains(t, out, "SIZE")
		require.Contains(t, out, "ERROR")
		require.Contains(t, out, "foo")
		require.Contains(t, out, "bar")
	})

	t.Run("text output broker aggregation omits topic and partition columns", func(t *testing.T) {
		var buf bytes.Buffer
		f := config.OutFormatter{Kind: "text"}
		printLogDirs(f, rows, "broker", false, false, &buf)
		out := buf.String()
		require.Contains(t, out, "BROKER")
		require.NotContains(t, out, "TOPIC")
		require.NotContains(t, out, "PARTITION")
	})

	t.Run("json output always emits full flat struct", func(t *testing.T) {
		var buf bytes.Buffer
		f := config.OutFormatter{Kind: "json"}
		printLogDirs(f, rows, "broker", false, false, &buf)
		var result []logDirRow
		err := json.Unmarshal(buf.Bytes(), &result)
		require.NoError(t, err)
		require.Len(t, result, 3)
		require.Equal(t, "foo", result[0].Topic)
		require.Equal(t, int32(0), result[0].Partition)
	})

	t.Run("json omits error field when empty", func(t *testing.T) {
		var buf bytes.Buffer
		f := config.OutFormatter{Kind: "json"}
		printLogDirs(f, []logDirRow{{Broker: 1, Dir: "/data", Topic: "t", Partition: 0, Size: 100}}, "partition", false, false, &buf)
		require.NotContains(t, buf.String(), `"error"`)
	})

	t.Run("json includes error field when set", func(t *testing.T) {
		var buf bytes.Buffer
		f := config.OutFormatter{Kind: "json"}
		printLogDirs(f, []logDirRow{{Broker: 1, Dir: "/data", Error: "kaboom"}}, "partition", false, false, &buf)
		require.Contains(t, buf.String(), `"error"`)
		require.Contains(t, buf.String(), "kaboom")
	})

	t.Run("text human-readable size", func(t *testing.T) {
		var buf bytes.Buffer
		f := config.OutFormatter{Kind: "text"}
		printLogDirs(f, []logDirRow{{Broker: 1, Dir: "/data", Topic: "t", Partition: 0, Size: 1048576}}, "partition", true, false, &buf)
		out := buf.String()
		// 1 MiB should render as human-readable
		require.Contains(t, out, "MB")
	})

	t.Run("yaml output", func(t *testing.T) {
		var buf bytes.Buffer
		f := config.OutFormatter{Kind: "yaml"}
		printLogDirs(f, rows, "partition", false, false, &buf)
		out := buf.String()
		require.Contains(t, out, "broker:")
		require.Contains(t, out, "topic:")
	})

	t.Run("text dir aggregation shows broker and dir", func(t *testing.T) {
		var buf bytes.Buffer
		f := config.OutFormatter{Kind: "text"}
		printLogDirs(f, rows, "dir", false, false, &buf)
		out := buf.String()
		lines := strings.Split(strings.TrimSpace(out), "\n")
		header := strings.Fields(lines[0])
		require.Equal(t, []string{"BROKER", "DIR", "SIZE", "ERROR"}, header)
	})

	t.Run("text topic aggregation shows broker, dir, and topic", func(t *testing.T) {
		var buf bytes.Buffer
		f := config.OutFormatter{Kind: "text"}
		printLogDirs(f, rows, "topic", false, false, &buf)
		lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
		header := strings.Fields(lines[0])
		require.Equal(t, []string{"BROKER", "DIR", "TOPIC", "SIZE", "ERROR"}, header)
	})

	t.Run("sort by size orders descending after aggregation", func(t *testing.T) {
		var buf bytes.Buffer
		f := config.OutFormatter{Kind: "text"}
		printLogDirs(f, rows, "partition", false, true, &buf)
		out := buf.String()
		// Row with size 2048 should appear before row with size 512
		idx2048 := strings.Index(out, "2048")
		idx512 := strings.Index(out, "512")
		require.Less(t, idx2048, idx512, "larger size should appear first")
	})
}
