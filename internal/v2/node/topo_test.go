package node

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/v2/catalog"
	"github.com/lf-edge/ekuiper/v2/internal/v2/planner"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
)

func TestTopo(t *testing.T) {
	ctx := context.Background()
	actions := make([]map[string]interface{}, 0)
	actions = append(actions, map[string]interface{}{"log": map[string]any{}})
	stmt, err := xsql.NewParser(strings.NewReader("select a from demo")).Parse()
	require.NoError(t, err)
	require.NotNil(t, stmt)
	lpbuilder := &planner.LogicalPlanBuilder{}
	lp, err := lpbuilder.CreateLogicalPlan(ctx, stmt, prepareCatalog(), actions)
	require.NoError(t, err)
	require.NotNil(t, lp)
	ppbuilder := &planner.PhysicalPlanBuilder{}
	pp, err := ppbuilder.BuildPhysicalPlan(ctx, lp)
	require.NoError(t, err)
	require.NotNil(t, pp)
	topo, err := CreateTopo(ctx, pp)
	require.NoError(t, err)
	err = topo.Start(ctx)
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	err = topo.Stop(ctx)
	require.NoError(t, err)
}

func prepareCatalog() *catalog.Catalog {
	c := catalog.NewCatalog()
	c.AddStream("demo", &catalog.Stream{StreamName: "demo"})
	return c
}
