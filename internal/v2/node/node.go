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

func (t *Topo) Start(ctx context.Context) error {
	t.controlCh <- NewSignalMsg(StartRuleSignal)
	select {
	case <-ctx.Done():
		return nil
	case msg := <-t.recvCh:
		if msg.IsSameControlSignal(StartRuleSignal) {
			fmt.Println("received start rule signal")
			return nil
		} else {
			return fmt.Errorf("invalid start rule signal")
		}
	}
}

func (t *Topo) Stop(ctx context.Context) error {
	t.controlCh <- NewSignalMsg(StopRuleSignal)
	select {
	case <-ctx.Done():
		return nil
	case msg := <-t.recvCh:
		if msg.IsSameControlSignal(StopRuleSignal) {
			fmt.Println("received stop rule signal")
			return nil
		} else {
			return fmt.Errorf("invalid start rule signal")
		}
	}
}

func (t *Topo) QuickStop() {

}

func CreateTopo(ctx context.Context, physicalPlanEnd planner.PhysicalPlan) (*Topo, error) {
	t := NewTopo()
	t.buildNodes(ctx, physicalPlanEnd, "topo", t.recvCh)
	return t, nil
}

func (t *Topo) buildNodes(ctx context.Context, lp planner.PhysicalPlan, outputKey string, outputChannel chan *NodeMessage) error {
	if _, ok := t.operators[lp.GetIndex()]; ok {
		return nil
	}
	var node NodeOperator
	switch p := lp.(type) {
	case *planner.PhysicalDataSource:
		op := NewSourceNode(p)
		node = op
	case *planner.PhysicalProject:
		op := NewProjectNode(p)
		node = op
	case *planner.PhysicalStake:
		op := NewStakeNode(p)
		node = op
		if p.IsRoot {
			op.IsRoot = true
			op.Input = t.controlCh
		} else if p.IsEnd {
			op.IsEnd = true
		}
	}
	node.AddOutput(outputKey, outputChannel)
	t.operators[lp.GetIndex()] = node
	for _, child := range lp.GetChildren() {
		t.buildNodes(ctx, child, node.GetName(), node.GetInput())
	}
	node.Run(t.ctx)
	return nil
}
