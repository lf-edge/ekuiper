package planner

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/v2/catalog"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
)

func TestParser(t *testing.T) {
	stmt, err := xsql.NewParser(strings.NewReader("select sum(a) as c, c + 1 as d from demo where c > 1")).Parse()
	require.NoError(t, err)
	require.NotNil(t, stmt)
}

func TestLogicalPlan(t *testing.T) {
	ctx := context.Background()
	stmt, err := xsql.NewParser(strings.NewReader("select a, *, count(*) from demo")).Parse()
	require.NoError(t, err)
	require.NotNil(t, stmt)
	lpbuilder := &LogicalPlanBuilder{}
	lp, err := lpbuilder.CreateLogicalPlan(ctx, stmt, prepareCatalog(), prepareAction())
	require.NoError(t, err)
	require.NotNil(t, lp)
	fmt.Println(ExplainLogicalPlan(lp))
	ppbuilder := &PhysicalPlanBuilder{}
	pp, err := ppbuilder.BuildPhysicalPlan(ctx, lp)
	require.NoError(t, err)
	require.NotNil(t, pp)
	fmt.Println()
	fmt.Println(ExplainPhysicalPlan(pp))
}

func prepareCatalog() *catalog.Catalog {
	c := catalog.NewCatalog()
	c.AddStream("demo", &catalog.Stream{StreamName: "demo"})
	return c
}
func prepareAction() []map[string]interface{} {
	actions := make([]map[string]interface{}, 0)
	actions = append(actions, map[string]interface{}{"log": map[string]any{}})
	return actions
}
