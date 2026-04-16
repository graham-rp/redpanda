// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package partitions

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/stretchr/testify/require"
)

func Test_buildMovementCancelResult(t *testing.T) {
	tests := []struct {
		name      string
		movements []rpadmin.PartitionsMovementResult
		want      []movementCancelResult
	}{
		{
			name:      "empty",
			movements: []rpadmin.PartitionsMovementResult{},
			want:      []movementCancelResult{},
		},
		{
			name: "single result",
			movements: []rpadmin.PartitionsMovementResult{
				{Namespace: "kafka", Topic: "foo", Partition: 0, Result: "success"},
			},
			want: []movementCancelResult{
				{Namespace: "kafka", Topic: "foo", Partition: 0, Result: "success"},
			},
		},
		{
			name: "multiple results",
			movements: []rpadmin.PartitionsMovementResult{
				{Namespace: "kafka", Topic: "foo", Partition: 0, Result: "success"},
				{Namespace: "kafka", Topic: "bar", Partition: 1, Result: "failed"},
				{Namespace: "redpanda_internal", Topic: "tx", Partition: 2, Result: "success"},
			},
			want: []movementCancelResult{
				{Namespace: "kafka", Topic: "foo", Partition: 0, Result: "success"},
				{Namespace: "kafka", Topic: "bar", Partition: 1, Result: "failed"},
				{Namespace: "redpanda_internal", Topic: "tx", Partition: 2, Result: "success"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildMovementCancelResult(tt.movements)
			require.Equal(t, tt.want, got)
		})
	}
}

func Test_printMovementsResult(t *testing.T) {
	movements := []rpadmin.PartitionsMovementResult{
		{Namespace: "kafka", Topic: "foo", Partition: 0, Result: "success"},
		{Namespace: "kafka", Topic: "bar", Partition: 1, Result: "failed"},
	}

	t.Run("text output", func(t *testing.T) {
		f := config.OutFormatter{Kind: "text"}
		var buf bytes.Buffer
		err := printMovementsResult(f, buildMovementCancelResult(movements), &buf)
		require.NoError(t, err)
		out := buf.String()
		require.Contains(t, out, "NAMESPACE")
		require.Contains(t, out, "TOPIC")
		require.Contains(t, out, "PARTITION")
		require.Contains(t, out, "RESULT")
		require.Contains(t, out, "kafka")
		require.Contains(t, out, "foo")
		require.Contains(t, out, "bar")
		require.Contains(t, out, "success")
		require.Contains(t, out, "failed")
	})

	t.Run("json output", func(t *testing.T) {
		f := config.OutFormatter{Kind: "json"}
		var buf bytes.Buffer
		err := printMovementsResult(f, buildMovementCancelResult(movements), &buf)
		require.NoError(t, err)
		var got []movementCancelResult
		require.NoError(t, json.Unmarshal(buf.Bytes(), &got))
		require.Len(t, got, 2)
		require.Equal(t, "kafka", got[0].Namespace)
		require.Equal(t, "foo", got[0].Topic)
		require.Equal(t, 0, got[0].Partition)
		require.Equal(t, "success", got[0].Result)
	})

	t.Run("json empty results", func(t *testing.T) {
		f := config.OutFormatter{Kind: "json"}
		var buf bytes.Buffer
		err := printMovementsResult(f, []movementCancelResult{}, &buf)
		require.NoError(t, err)
		var got []movementCancelResult
		require.NoError(t, json.Unmarshal(buf.Bytes(), &got))
		require.Empty(t, got)
	})

	t.Run("json field names", func(t *testing.T) {
		f := config.OutFormatter{Kind: "json"}
		single := []rpadmin.PartitionsMovementResult{
			{Namespace: "kafka", Topic: "mytopic", Partition: 3, Result: "success"},
		}
		var buf bytes.Buffer
		err := printMovementsResult(f, buildMovementCancelResult(single), &buf)
		require.NoError(t, err)
		raw := buf.String()
		require.True(t, strings.Contains(raw, `"namespace"`), "expected json key 'namespace'")
		require.True(t, strings.Contains(raw, `"topic"`), "expected json key 'topic'")
		require.True(t, strings.Contains(raw, `"partition"`), "expected json key 'partition'")
		require.True(t, strings.Contains(raw, `"result"`), "expected json key 'result'")
	})
}
