// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
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
	ctx1 := mockContext.NewMockContext("r1", "t1")
	ctx2 := mockContext.NewMockContext("r2", "t2")
	for i, tc := range testcases {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			sLayer := newSharedLayer()
			sLayer.RegSchema("r1", "s1", tc.originSchema, false)
			sLayer.RegSchema("r2", "s1", tc.newSchema, false)
			err := sLayer.Attach(ctx1)
			require.NoError(t, err)
			err = sLayer.Attach(ctx2)
			if tc.err == nil {
				require.NoError(t, err)
			} else {
				require.Equal(t, tc.err, err)
			}
		})
	}
}

func TestMergeWildcardSchema(t *testing.T) {
	originSchema := map[string]*ast.JsonStreamField{
		"a": {
			Type: "bigint",
		},
	}
	ctx1 := mockContext.NewMockContext("1", "t1")
	ctx2 := mockContext.NewMockContext("2", "t2")
	f := newSharedLayer()
	f.RegSchema("1", "", originSchema, false)
	err := f.Attach(ctx1)
	require.NoError(t, err)
	f.RegSchema("2", "", nil, true)
	require.NoError(t, f.Attach(ctx2))
	newSchema := map[string]*ast.JsonStreamField{
		"b": {
			Type: "bigint",
		},
	}
	f.RegSchema("3", "", newSchema, false)
	ctx3 := mockContext.NewMockContext("3", "t3")
	require.NoError(t, f.Attach(ctx3))
	s := f.GetSchema()
	require.Nil(t, s)
}

func TestAttachDetachSchema(t *testing.T) {
	f := newSharedLayer()
	f.RegSchema("rule1", "demo", nil, true)
	ctx1 := mockContext.NewMockContext("rule1", "t1")
	err := f.Attach(ctx1)
	require.NoError(t, err)
	f.RegSchema("rule2", "demo", map[string]*ast.JsonStreamField{
		"a": nil,
	}, false)
	ctx2 := mockContext.NewMockContext("rule2", "t1")
	err = f.Attach(ctx2)
	require.NoError(t, err)
	r := GetRuleSchema("rule1")
	er := RuleSchemaResponse{
		Schema: map[string]map[string]*ast.JsonStreamField{
			"demo": nil,
		},
		Wildcard: map[string]bool{
			"demo": true,
		},
	}
	require.Equal(t, r, er)
	r = GetRuleSchema("rule2")
	require.Equal(t, r, er)
	// detach rule
	require.NoError(t, f.Detach(ctx1))
	r = GetRuleSchema("rule2")
	er = RuleSchemaResponse{
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
