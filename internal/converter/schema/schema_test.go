// Copyright 2024 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schema

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/converter/merge"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

func TestMergeSchema(t *testing.T) {
	testcases := []struct {
		originSchema map[string]*ast.JsonStreamField
		newSchema    map[string]*ast.JsonStreamField
		resultSchema map[string]*ast.JsonStreamField
		err          error
	}{
		{
			originSchema: map[string]*ast.JsonStreamField{
				"a": nil,
			},
			newSchema: map[string]*ast.JsonStreamField{
				"b": nil,
			},
			resultSchema: map[string]*ast.JsonStreamField{
				"a": nil,
				"b": nil,
			},
		},
		{
			originSchema: map[string]*ast.JsonStreamField{
				"a": nil,
			},
			newSchema: map[string]*ast.JsonStreamField{
				"a": nil,
			},
			resultSchema: map[string]*ast.JsonStreamField{
				"a": nil,
			},
		},
		{
			originSchema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "array",
					Items: &ast.JsonStreamField{
						Type: "struct",
						Properties: map[string]*ast.JsonStreamField{
							"b": {
								Type: "bigint",
							},
						},
					},
				},
			},
			newSchema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "array",
					Items: &ast.JsonStreamField{
						Type: "struct",
						Properties: map[string]*ast.JsonStreamField{
							"b": {
								Type: "string",
							},
						},
					},
				},
			},
			err: errors.New("column field type b between current[bigint] and new[string] are not equal"),
		},
		{
			originSchema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "array",
					Items: &ast.JsonStreamField{
						Type: "bigint",
					},
				},
			},
			newSchema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "array",
					Items: &ast.JsonStreamField{
						Type: "string",
					},
				},
			},
			err: errors.New("array column field type a between current[bigint] and new[string] are not equal"),
		},
		{
			originSchema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "struct",
					Properties: map[string]*ast.JsonStreamField{
						"b": {
							Type: "bigint",
						},
					},
				},
			},
			newSchema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "struct",
					Properties: map[string]*ast.JsonStreamField{
						"b": {
							Type: "string",
						},
					},
				},
			},
			err: errors.New("column field type b between current[bigint] and new[string] are not equal"),
		},
		{
			originSchema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "bigint",
				},
			},
			newSchema: map[string]*ast.JsonStreamField{
				"b": {
					Type: "string",
				},
			},
			resultSchema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "bigint",
				},
				"b": {
					Type: "string",
				},
			},
		},
		{
			originSchema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "struct",
					Properties: map[string]*ast.JsonStreamField{
						"b": {
							Type: "bigint",
						},
					},
				},
			},
			newSchema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "struct",
					Properties: map[string]*ast.JsonStreamField{
						"c": {
							Type: "string",
						},
					},
				},
			},
			resultSchema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "struct",
					Properties: map[string]*ast.JsonStreamField{
						"b": {
							Type: "bigint",
						},
						"c": {
							Type: "string",
						},
					},
				},
			},
		},
	}
	for _, tc := range testcases {
		sLayer := NewSchemaLayer("r1", "s1", tc.originSchema, false)
		err := sLayer.MergeSchema("r2", "s1", tc.newSchema, false)
		if tc.err == nil {
			require.NoError(t, err)
		} else {
			require.Equal(t, tc.err, err)
		}
	}
}

func TestMergeWildcardSchema(t *testing.T) {
	originSchema := map[string]*ast.JsonStreamField{
		"a": {
			Type: "bigint",
		},
	}
	f := NewSchemaLayer("1", "", originSchema, false)
	require.NoError(t, f.MergeSchema("2", "", nil, true))
	newSchema := map[string]*ast.JsonStreamField{
		"b": {
			Type: "bigint",
		},
	}
	require.NoError(t, f.MergeSchema("3", "", newSchema, false))
	s := f.GetSchema()
	require.Nil(t, s)
}

func TestAttachDetachSchema(t *testing.T) {
	f := NewSchemaLayer("rule1", "demo", nil, true)
	err := f.MergeSchema("rule2", "demo", map[string]*ast.JsonStreamField{
		"a": nil,
	}, false)
	require.NoError(t, err)
	r := merge.GetRuleSchema("rule1")
	er := merge.RuleSchemaResponse{
		Schema: map[string]map[string]*ast.JsonStreamField{
			"demo": nil,
		},
		Wildcard: map[string]bool{
			"demo": true,
		},
	}
	require.Equal(t, r, er)
	r = merge.GetRuleSchema("rule2")
	require.Equal(t, r, er)
	// detach rule
	require.NoError(t, f.DetachSchema("rule1"))
	r = merge.GetRuleSchema("rule2")
	er = merge.RuleSchemaResponse{
		Schema: map[string]map[string]*ast.JsonStreamField{
			"demo": {
				"a": nil,
			},
		},
		Wildcard: map[string]bool{
			"demo": false,
		},
	}
	require.Equal(t, r, er)
}
