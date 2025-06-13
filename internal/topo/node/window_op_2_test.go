package node_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node"
	"github.com/lf-edge/ekuiper/v2/internal/topo/planner"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func TestSlidingWindow(t *testing.T) {
	t.Skip("only test this locally")
	conf.IsTesting = true
	node.EnableAlignWindow = false
	now := time.Now()
	timex.SetNow(now)
	o := &def.RuleOption{
		BufferLength: 10,
		PlanOptimizeStrategy: &def.PlanOptimizeStrategy{
			WindowOption: &def.WindowOption{
				EnableSendSlidingWindowTwice: true,
			},
		},
		Qos: 0,
	}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	sql := "select count(*) from stream group by slidingwindow(ss,2,2) over (when a = 1)"
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	require.NoError(t, err)
	p, err := planner.CreateLogicalPlan(stmt, o, kv)
	require.NoError(t, err)
	require.NotNil(t, p)
	windowPlan := extractWindowPlan(p)
	require.NotNil(t, windowPlan)
	op, err := node.NewWindowOp("1", *windowPlan.GenWindowConfig(), o)
	require.NoError(t, err)
	require.NotNil(t, op)
	input, _ := op.GetInput()
	output := make(chan any, 10)
	op.AddOutput(output, "output")
	errCh := make(chan error, 10)
	ctx, cancel := mockContext.NewMockContext("1", "2").WithCancel()
	op.Exec(ctx, errCh)
	waitExecute()
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}, Timestamp: now.Add(1 * time.Millisecond)}
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
	timex.Add(500 * time.Millisecond)
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(2)}, Timestamp: now.Add(500 * time.Millisecond)}
	waitExecute()
	timex.Add(1700 * time.Millisecond)
	got = <-output
	wt, ok = got.(*xsql.WindowTuples)
	require.True(t, ok)
	require.NotNil(t, wt)
	d = wt.ToMaps()
	require.Equal(t, []map[string]any{
		{
			"a": int64(2),
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
