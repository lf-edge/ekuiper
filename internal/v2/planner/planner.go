package planner

import (
	"context"
	"fmt"

	"github.com/lf-edge/ekuiper/v2/internal/v2/catalog"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

type PlanBuilder struct {
	planIndex int64
	root      LogicalPlan
	end       LogicalPlan
}

func (b *PlanBuilder) init() {
	b.planIndex = 0
	b.root = &LogicalPlanRoot{BaseLogicalPlan: NewBaseLogicalPlan(b.planIndex)}
	b.planIndex++
}

func (b *PlanBuilder) finish(children []LogicalPlan) LogicalPlan {
	b.end = &LogicalPlanEnd{BaseLogicalPlan: NewBaseLogicalPlan(b.planIndex)}
	b.end.SetChildren(children)
	b.planIndex++
	return b.root
}

func (b *PlanBuilder) CreateLogicalPlan(ctx context.Context, stmt *ast.SelectStatement, c *catalog.Catalog) (LogicalPlan, error) {
	b.init()
	var err error
	var children []LogicalPlan
	children, err = b.extractDatasource(ctx, stmt, c)
	if err != nil {
		return nil, err
	}
	children, err = b.extractProjectPlan(ctx, stmt, children)
	if err != nil {
		return nil, err
	}
	return b.finish(children), nil
}

func (b *PlanBuilder) extractDatasource(ctx context.Context, stmt *ast.SelectStatement, c *catalog.Catalog) ([]LogicalPlan, error) {
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

func (b *PlanBuilder) extractProjectPlan(ctx context.Context, stmt *ast.SelectStatement, children []LogicalPlan) ([]LogicalPlan, error) {
	proj := ProjectPlan{BaseLogicalPlan: NewBaseLogicalPlan(b.planIndex), Fields: stmt.Fields}
	proj.SetChildren(children)
	b.planIndex++
	return []LogicalPlan{proj}, nil
}
