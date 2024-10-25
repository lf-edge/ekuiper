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

package node_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node"
	"github.com/lf-edge/ekuiper/v2/internal/topo/planner"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func init() {
	testx.InitEnv("node_test")
}

func TestIncAggWindow(t *testing.T) {
	o := &def.RuleOption{
		BufferLength: 10,
	}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	sql := "select count(*) from stream group by countwindow(2)"
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	require.NoError(t, err)
	p, err := planner.CreateLogicalPlan(stmt, &def.RuleOption{
		Qos: 0,
	}, kv)
	require.NoError(t, err)
	require.NotNil(t, p)
	incPlan := extractIncWindowPlan(p)
	require.NotNil(t, incPlan)
	op, err := node.NewWindowIncAggOp("1", &node.WindowConfig{
		Type:        incPlan.WType,
		CountLength: incPlan.Length,
	}, incPlan.Dimensions, incPlan.IncAggFuncs, o)
	require.NoError(t, err)
	require.NotNil(t, op)
	input, _ := op.GetInput()
	output := make(chan any, 10)
	op.AddOutput(output, "output")
	errCh := make(chan error, 10)
	op.Exec(mockContext.NewMockContext("1", "2"), errCh)
	time.Sleep(100 * time.Millisecond)
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}}
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}}
	got := <-output
	wt, ok := got.(*xsql.WindowTuples)
	require.True(t, ok)
	require.NotNil(t, wt)
	d := wt.ToMaps()
	require.Equal(t, []map[string]any{
		{
			"a":             int64(1),
			"inc_agg_col_1": int64(2),
		},
	}, d)
	op.Close()
}

func extractIncWindowPlan(cur planner.LogicalPlan) *planner.IncWindowPlan {
	switch plan := cur.(type) {
	case *planner.IncWindowPlan:
		return plan
	default:
		for _, child := range plan.Children() {
			got := extractIncWindowPlan(child)
			if got != nil {
				return got
			}
		}
	}
	return nil
}

func prepareStream() error {
	kv, err := store.GetKV("stream")
	if err != nil {
		return err
	}
	streamSqls := map[string]string{
		"sharedStream": `CREATE STREAM sharedStream (
					a BIGINT,
					b BIGINT,
				) WITH (DATASOURCE="src1", SHARED="true");`,
		"stream": `CREATE STREAM stream (
					a BIGINT,
					b BIGINT,
				) WITH (DATASOURCE="src1");`,
	}

	types := map[string]ast.StreamType{
		"sharedStream": ast.TypeStream,
		"stream":       ast.TypeStream,
	}
	for name, sql := range streamSqls {
		s, err := json.Marshal(&xsql.StreamInfo{
			StreamType: types[name],
			Statement:  sql,
		})
		if err != nil {
			return err
		}
		err = kv.Set(name, string(s))
		if err != nil {
			return err
		}
	}
	return nil
}
