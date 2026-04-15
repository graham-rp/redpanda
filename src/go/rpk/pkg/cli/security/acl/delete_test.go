// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package acl

import (
	"bytes"
	"strings"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestPrintDeleteOutput(t *testing.T) {
	output := aclDeleteOutput{
		Filters: []aclWithMessage{
			{
				Principal:           "User:alice",
				Host:                "*",
				ResourceType:        "Topic",
				ResourceName:        "foo",
				ResourcePatternType: "Literal",
				Operation:           "Read",
				Permission:          "Allow",
				Message:             "",
			},
		},
		Deletions: []aclWithMessage{
			{
				Principal:           "User:alice",
				Host:                "*",
				ResourceType:        "Topic",
				ResourceName:        "foo",
				ResourcePatternType: "Literal",
				Operation:           "Read",
				Permission:          "Allow",
				Message:             "",
			},
		},
	}

	t.Run("text output contains header and data", func(t *testing.T) {
		var buf bytes.Buffer
		f := config.OutFormatter{Kind: "text"}
		printDeleteOutput(f, output, true, &buf)
		got := buf.String()
		require.Contains(t, got, "PRINCIPAL")
		require.Contains(t, got, "User:alice")
	})

	t.Run("text output writes filters and deletions tables to writer", func(t *testing.T) {
		var buf bytes.Buffer
		f := config.OutFormatter{Kind: "text"}
		printDeleteOutput(f, output, true, &buf)
		got := buf.String()
		// Filters table and deletions table both appear in the writer output.
		// Section headers go to stdout (out.Section), so we just check for table rows.
		require.Equal(t, 2, strings.Count(got, "User:alice"), "expected data in both filters and deletions tables")
	})

	t.Run("text output no deletions header when false and no filters", func(t *testing.T) {
		noFilters := aclDeleteOutput{
			Deletions: output.Deletions,
		}
		var buf bytes.Buffer
		f := config.OutFormatter{Kind: "text"}
		printDeleteOutput(f, noFilters, false, &buf)
		got := buf.String()
		require.NotContains(t, got, "DELETIONS")
		require.Contains(t, got, "User:alice")
	})

	t.Run("json output contains filters and deletions keys", func(t *testing.T) {
		var buf bytes.Buffer
		f := config.OutFormatter{Kind: "json"}
		printDeleteOutput(f, output, false, &buf)
		got := buf.String()
		require.Contains(t, got, `"filters"`)
		require.Contains(t, got, `"deletions"`)
		require.Contains(t, got, "User:alice")
	})

	t.Run("filters omitted from json when empty", func(t *testing.T) {
		noFilters := aclDeleteOutput{
			Deletions: output.Deletions,
		}
		var buf bytes.Buffer
		f := config.OutFormatter{Kind: "json"}
		printDeleteOutput(f, noFilters, false, &buf)
		got := buf.String()
		require.False(t, strings.Contains(got, `"filters"`), "filters should be omitted when nil/empty")
	})
}
