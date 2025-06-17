// Copyright 2025 EMQ Technologies Co., Ltd.
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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
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

func TestWindowV2SlidingWindowCondition(t *testing.T) {
	conf.IsTesting = true
	now := time.Now()
	o := &def.RuleOption{
		BufferLength: 10,
	}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	sql := "select count(*) from stream group by slidingWindow(ss,4) over (when a = 2)"
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	require.NoError(t, err)
	p, err := planner.CreateLogicalPlan(stmt, o, kv)
	require.NoError(t, err)
	require.NotNil(t, p)
	windowPlan := extractWindowPlan(p)
	require.NotNil(t, windowPlan)
	op, err := node.NewWindowV2Op("window", node.WindowConfig{
		TriggerCondition: windowPlan.GetTriggerCondition(),
		Type:             windowPlan.WindowType(),
		Length:           4 * time.Second,
		RawInterval:      4,
		TimeUnit:         ast.SS,
	}, o)
	require.NoError(t, err)
	require.NotNil(t, op)
	input, _ := op.GetInput()
	output := make(chan any, 10)
	op.AddOutput(output, "output")
	errCh := make(chan error, 10)
	ctx, cancel := mockContext.NewMockContext("1", "2").WithCancel()
	op.Exec(ctx, errCh)
	waitExecute()
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}, Timestamp: now}
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(2)}, Timestamp: now.Add(time.Second)}
	waitExecute()
	got := <-output
	wt, ok := got.(*xsql.WindowTuples)
	require.True(t, ok)
	require.NotNil(t, wt)
	d := wt.ToMaps()
	require.Equal(t, []map[string]any{
		{
			"a": int64(1),
		},
		{
			"a": int64(2),
		},
	}, d)
	cancel()
	waitExecute()
	op.Close()
}

func TestWindowV2SlidingWindow(t *testing.T) {
	conf.IsTesting = true
	now := time.Now()
	o := &def.RuleOption{
		BufferLength: 10,
	}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	sql := "select count(*) from stream group by slidingWindow(ss,4)"
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	require.NoError(t, err)
	p, err := planner.CreateLogicalPlan(stmt, o, kv)
	require.NoError(t, err)
	require.NotNil(t, p)
	windowPlan := extractWindowPlan(p)
	require.NotNil(t, windowPlan)
	op, err := node.NewWindowV2Op("window", node.WindowConfig{
		Type:        windowPlan.WindowType(),
		Length:      4 * time.Second,
		RawInterval: 4,
		TimeUnit:    ast.SS,
	}, o)
	require.NoError(t, err)
	require.NotNil(t, op)
	input, _ := op.GetInput()
	output := make(chan any, 10)
	op.AddOutput(output, "output")
	errCh := make(chan error, 10)
	ctx, cancel := mockContext.NewMockContext("1", "2").WithCancel()
	op.Exec(ctx, errCh)
	waitExecute()
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}, Timestamp: now}
	waitExecute()
	got := <-output
	wt, ok := got.(*xsql.WindowTuples)
	require.True(t, ok)
	require.NotNil(t, wt)
	d := wt.ToMaps()
	require.Equal(t, []map[string]any{
		{
			"a": int64(1),
		},
	}, d)
	cancel()
	waitExecute()
	op.Close()
}

func extractWindowPlan(cur planner.LogicalPlan) *planner.WindowPlan {
	switch plan := cur.(type) {
	case *planner.WindowPlan:
		return plan
	default:
		for _, child := range plan.Children() {
			got := extractWindowPlan(child)
			if got != nil {
				return got
			}
		}
	}
	return nil
}
