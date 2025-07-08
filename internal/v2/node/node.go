package node

import (
	"context"
	"fmt"

	"github.com/lf-edge/ekuiper/v2/internal/v2/planner"
)

type Topo struct {
	ctx       context.Context
	operators map[int]NodeOperator

	exitCh    chan struct{}
	controlCh chan *NodeMessage
	recvCh    chan *NodeMessage
}

func NewTopo() *Topo {
	t := &Topo{
		ctx:       context.Background(),
		operators: make(map[int]NodeOperator),
		exitCh:    make(chan struct{}),
		controlCh: make(chan *NodeMessage, 8),
		recvCh:    make(chan *NodeMessage, 8),
	}
	return t
}

func CreateTopo(ctx context.Context, lp planner.PhysicalPlan) (*Topo, error) {
	t := NewTopo()
	t.buildNodes(ctx, lp, "topo", t.recvCh)
	return t, nil
}

func (t *Topo) Start(ctx context.Context) error {
	t.controlCh <- &NodeMessage{StartRuleSignal: true}
	select {
	case <-ctx.Done():
		return nil
	case msg := <-t.recvCh:
		if msg.StartRuleSignal {
			fmt.Println("received start rule signal")
			return nil
		} else {
			return fmt.Errorf("invalid start rule signal")
		}
	}
}

func (t *Topo) buildNodes(ctx context.Context, lp planner.PhysicalPlan, parentKey string, parentCh chan *NodeMessage) error {
	var node NodeOperator
	switch p := lp.(type) {
	case *planner.PhysicalDataSource:
		op := NewSourceNode(p)
		op.AddOutput(parentKey, parentCh)
		t.operators[lp.GetIndex()] = op
		node = op
		op.run(ctx)
	case *planner.PhysicalProject:
		op := NewProjectNode(p)
		op.AddOutput(parentKey, parentCh)
		t.operators[lp.GetIndex()] = op
		node = op
		op.run(ctx)
	case *planner.PhysicalStake:
		op := NewStakeNode(p)
		op.AddOutput(parentKey, parentCh)
		t.operators[lp.GetIndex()] = op
		node = op
		if p.IsRoot {
			op.IsRoot = true
			op.Input = t.controlCh
		} else if p.IsEnd {
			op.IsEnd = true
		}
		op.run(t.ctx)
	}
	for _, child := range lp.GetChildren() {
		t.buildNodes(ctx, child, node.GetName(), node.GetInput())
	}
	return nil
}
