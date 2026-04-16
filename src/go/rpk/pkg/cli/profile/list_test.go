// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package profile

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestPrintProfileList(t *testing.T) {
	profiles := []profileListItem{
		{Name: "dev", Description: "development cluster", Current: true},
		{Name: "prod", Description: ""},
	}

	jsonBytes, err := json.Marshal(profiles)
	require.NoError(t, err)
	yamlBytes, err := yaml.Marshal(profiles)
	require.NoError(t, err)

	cases := []struct {
		kind   string
		output string
	}{
		{
			kind: "text",
			output: "NAME  DESCRIPTION\n" +
				"dev*  development cluster\n" +
				"prod  \n",
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
			printProfileList(f, profiles, b)
			require.Equal(t, c.output, b.String())
		})
	}
}
