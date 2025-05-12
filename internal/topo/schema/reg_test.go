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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

func TestSchemaStore(t *testing.T) {
	a := map[string]*ast.JsonStreamField{
		"a": {
			Type: "bigint",
		},
	}
	AddRuleSchema("rule1", "source1", a, false)
	require.Equal(t, RuleSchemaResponse{
		Schema: map[string]map[string]*ast.JsonStreamField{
			"source1": a,
		},
		Wildcard: map[string]bool{
			"source1": false,
		},
	}, GetRuleSchema("rule1"))
	RemoveRuleSchema("rule1")
	require.Equal(t, RuleSchemaResponse{
		Schema:   nil,
		Wildcard: nil,
	}, GetRuleSchema("rule1"))
}
