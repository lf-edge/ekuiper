package planner

import (
	"context"
	"fmt"

	"github.com/lf-edge/ekuiper/v2/internal/v2/catalog"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

type LogicalPlanBuilder struct {
	planIndex int
	root      LogicalPlan
	end       LogicalPlan
}

func (b *LogicalPlanBuilder) init() {
	b.planIndex = 0
	b.root = NewLogicalPlanRoot(b.planIndex)
	b.planIndex++
}

func (b *LogicalPlanBuilder) finish(children []LogicalPlan) LogicalPlan {
	b.end = NewLogicalPlanEnd(b.planIndex)
	b.end.SetChildren(children)
	b.planIndex++
	return b.end
}

func (b *LogicalPlanBuilder) CreateLogicalPlan(ctx context.Context, stmt *ast.SelectStatement, c *catalog.Catalog, actions []map[string]interface{}) (LogicalPlan, error) {
	b.init()
	var err error
	var children []LogicalPlan
	children, err = b.extractDatasource(ctx, stmt, c)
	if err != nil {
		return nil, err
	}
	children, err = b.extractWindow(ctx, stmt, children)
	if err != nil {
		return nil, err
	}
	children, err = b.extractProjectPlan(ctx, stmt, children)
	if err != nil {
		return nil, err
	}
	children, err = b.extractActions(ctx, actions, children)
	if err != nil {
		return nil, err
	}
	return b.finish(children), nil
}

func (b *LogicalPlanBuilder) extractWindow(ctx context.Context, stmt *ast.SelectStatement, children []LogicalPlan) ([]LogicalPlan, error) {
	w := stmt.Dimensions.GetWindow()
	if w == nil {
		return children, nil
	}
	wp := NewWindowPlan(b.planIndex, int(w.Length.Val))
	wp.SetChildren(children)
	b.planIndex++
	return []LogicalPlan{wp}, nil
}

func (b *LogicalPlanBuilder) extractDatasource(ctx context.Context, stmt *ast.SelectStatement, c *catalog.Catalog) ([]LogicalPlan, error) {
	streams := xsql.GetStreams(stmt)
	dsPlans := make([]LogicalPlan, 0)
	for _, s := range streams {
		stream, ok := c.GetStream(s)
		if !ok {
			return nil, fmt.Errorf("stream %s not found", s)
		}
		dsPlans = append(dsPlans, NewDataSourcePlan(stream, b.planIndex))
		b.planIndex++
	}
	for _, p := range dsPlans {
		p.AddChild(b.root)
	}
	return dsPlans, nil
}

func (b *LogicalPlanBuilder) extractProjectPlan(ctx context.Context, stmt *ast.SelectStatement, children []LogicalPlan) ([]LogicalPlan, error) {
	proj := NewProjectPlan(b.planIndex, stmt.Fields)
	proj.SetChildren(children)
	b.planIndex++
	return []LogicalPlan{proj}, nil
}

func (b *LogicalPlanBuilder) extractActions(ctx context.Context, actions []map[string]interface{}, children []LogicalPlan) ([]LogicalPlan, error) {
	sinkPlans := make([]LogicalPlan, 0)
	for _, action := range actions {
		for k, v := range action {
			props, ok := v.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("action %s not have props", k)
			}
			sink := NewDataSinkPlan(b.planIndex, k, props)
			sink.SetChildren(children)
			b.planIndex++
			sinkPlans = append(sinkPlans, sink)
		}
	}
	return sinkPlans, nil
}

type PhysicalPlanBuilder struct {
	planIndex int
	plans     map[int]PhysicalPlan
	root      PhysicalPlan
	end       PhysicalPlan
}

func (pb *PhysicalPlanBuilder) BuildPhysicalPlan(ctx context.Context, planEnd LogicalPlan) (PhysicalPlan, error) {
	pb.planIndex = 0
	pb.plans = make(map[int]PhysicalPlan)
	return pb.buildPhysicalPlan(ctx, planEnd)
}

func (pb *PhysicalPlanBuilder) buildPhysicalPlan(ctx context.Context, plan LogicalPlan) (PhysicalPlan, error) {
	if pp, ok := pb.plans[plan.GetIndex()]; ok {
		return pp, nil
	}
	children := make([]PhysicalPlan, 0)
	for _, child := range plan.GetChildren() {
		childPlan, err := pb.buildPhysicalPlan(ctx, child)
		if err != nil {
			return nil, err
		}
		children = append(children, childPlan)
	}
	var headPlan PhysicalPlan
	var tailPlan PhysicalPlan
	switch p := plan.(type) {
	case *DataSourcePlan:
		pd := NewPhysicalDataSource(p, pb.planIndex)
		pb.planIndex++
		headPlan = pd
		tailPlan = pd
	case *DataSinkPlan:
		ps := NewPhysicalDataSink(p, pb.planIndex)
		pb.planIndex++
		headPlan = ps
		tailPlan = ps
	case *WindowPlan:
		pw := NewPhysicalCountWindow(p, pb.planIndex)
		pb.planIndex++
		headPlan = pw
		tailPlan = pw
	case *ProjectPlan:
		pp := NewPhysicalProject(p, pb.planIndex)
		pb.planIndex++
		headPlan = pp
		tailPlan = pp
	case *LogicalPlanRoot:
		pr := NewPhysicalStakeRoot(p, pb.planIndex)
		pb.root = pr
		pb.planIndex++
		headPlan = pr
		tailPlan = pr
	case *LogicalPlanEnd:
		pe := NewPhysicalStakeEnd(p, pb.planIndex)
		pb.end = pe
		pb.planIndex++
		headPlan = pe
		tailPlan = pe
	}
	tailPlan.SetChildren(children)
	pb.plans[headPlan.GetIndex()] = headPlan
	return headPlan, nil
}
