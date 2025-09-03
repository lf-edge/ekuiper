package topology

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/lf-edge/ekuiper/v2/internal/v2/catalog"
	"github.com/lf-edge/ekuiper/v2/internal/v2/node"
	"github.com/lf-edge/ekuiper/v2/internal/v2/planner"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
)

var (
	GlobalTopoManager *TopoManager
)

func init() {
	GlobalTopoManager = &TopoManager{
		topos: make(map[string]*node.Topo),
	}
}

type TopoManager struct {
	sync.Mutex
	topos map[string]*node.Topo
}

func (m *TopoManager) CreateRule(ctx context.Context, name, sql string) error {
	m.Lock()
	defer m.Unlock()
	if _, ok := m.topos[name]; ok {
		return fmt.Errorf("rule %s already exists", name)
	}
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	if err != nil {
		return err
	}
	c := catalog.NewCatalog()
	c.AddStream("demo", &catalog.Stream{StreamName: "demo"})
	actions := make([]map[string]interface{}, 0)
	actions = append(actions, map[string]interface{}{"log": map[string]any{}})
	lpbuilder := &planner.LogicalPlanBuilder{}
	lp, err := lpbuilder.CreateLogicalPlan(ctx, stmt, c, actions)
	if err != nil {
		return err
	}
	ppbuilder := &planner.PhysicalPlanBuilder{}
	pp, err := ppbuilder.BuildPhysicalPlan(ctx, lp)
	if err != nil {
		return err
	}
	topo, err := node.CreateTopo(ctx, pp)
	if err != nil {
		return err
	}
	m.topos[name] = topo
	return topo.Start()
}

func (m *TopoManager) DeleteRule(name string) error {
	m.Lock()
	defer m.Unlock()
	topo, ok := m.topos[name]
	if !ok {
		return fmt.Errorf("rule %s not found", name)
	}
	topo.Stop()
	topo.Release()
	delete(m.topos, name)
	return nil
}
