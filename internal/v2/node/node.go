package node

import (
	"context"

	"github.com/lf-edge/ekuiper/v2/internal/v2/planner"
)

type Topo struct {
	operators map[int]NodeOperator
}

func CreateTopo(ctx context.Context, lp planner.PhysicalPlan) (*Topo, error) {

}

func (t *Topo) buildNodes(ctx context.Context, lp planner.PhysicalPlan, parentKey string, parentCh chan *NodeMessage) error {
	switch lp.(type) {
	case *planner.PhysicalDataSource:
	case *planner.PhysicalProject:
	case *planner.PhysicalStake:
	}
	return nil
}
