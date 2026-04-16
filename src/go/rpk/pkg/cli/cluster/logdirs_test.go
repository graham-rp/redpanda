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
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/stretchr/testify/require"
)

func TestPrintLogDirs(t *testing.T) {
	// Fresh rows per subtest: collapse() reuses the input slice's backing
	// array, so sharing a single slice across subtests leaks mutations.
	freshRows := func() []logDirRow {
		return []logDirRow{
			{Broker: 1, Dir: "/var/lib/redpanda/data", Topic: "foo", Partition: 0, Size: 1024},
			{Broker: 1, Dir: "/var/lib/redpanda/data", Topic: "foo", Partition: 1, Size: 2048},
			{Broker: 2, Dir: "/var/lib/redpanda/data", Topic: "bar", Partition: 0, Size: 512, Error: "some error"},
		}
	}
	f := config.OutFormatter{Kind: "text"}

	aggCases := []struct {
		agg  string
		want [][]string
	}{
		{
			agg: "partition",
			want: [][]string{
				{"BROKER", "DIR", "TOPIC", "PARTITION", "SIZE", "ERROR"},
				{"1", "/var/lib/redpanda/data", "foo", "0", "1024"},
				{"1", "/var/lib/redpanda/data", "foo", "1", "2048"},
				{"2", "/var/lib/redpanda/data", "bar", "0", "512", "some", "error"},
			},
		},
		{
			agg: "broker",
			want: [][]string{
				{"BROKER", "SIZE", "ERROR"},
				{"1", "3072"},
				{"2", "512", "some", "error"},
			},
		},
		{
			agg: "dir",
			want: [][]string{
				{"BROKER", "DIR", "SIZE", "ERROR"},
				{"1", "/var/lib/redpanda/data", "3072"},
				{"2", "/var/lib/redpanda/data", "512", "some", "error"},
			},
		},
		{
			agg: "topic",
			want: [][]string{
				{"BROKER", "DIR", "TOPIC", "SIZE", "ERROR"},
				{"1", "/var/lib/redpanda/data", "foo", "3072"},
				{"2", "/var/lib/redpanda/data", "bar", "512", "some", "error"},
			},
		},
	}
	for _, tc := range aggCases {
		t.Run("agg="+tc.agg, func(t *testing.T) {
			var buf bytes.Buffer
			printLogDirs(f, freshRows(), tc.agg, false, false, &buf)
			require.Equal(t, tc.want, out.TableRows(buf.String()))
		})
	}

	t.Run("sort-by-size descending", func(t *testing.T) {
		var buf bytes.Buffer
		printLogDirs(f, freshRows(), "partition", false, true, &buf)
		require.Equal(t, [][]string{
			{"BROKER", "DIR", "TOPIC", "PARTITION", "SIZE", "ERROR"},
			{"1", "/var/lib/redpanda/data", "foo", "1", "2048"},
			{"1", "/var/lib/redpanda/data", "foo", "0", "1024"},
			{"2", "/var/lib/redpanda/data", "bar", "0", "512", "some", "error"},
		}, out.TableRows(buf.String()))
	})

	t.Run("human-readable size", func(t *testing.T) {
		var buf bytes.Buffer
		printLogDirs(f, []logDirRow{{Broker: 1, Dir: "/data", Topic: "t", Partition: 0, Size: 1048576}}, "partition", true, false, &buf)
		require.Equal(t, [][]string{
			{"BROKER", "DIR", "TOPIC", "PARTITION", "SIZE", "ERROR"},
			{"1", "/data", "t", "0", "1.049MB"},
		}, out.TableRows(buf.String()))
	})
}
