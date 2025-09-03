package node

import (
	"context"
	"fmt"

	"github.com/lf-edge/ekuiper/v2/internal/v2/planner"
)

type Topo struct {
	ctx       context.Context
	cancel    context.CancelFunc
	operators map[int]NodeOperator

	exitCh    chan struct{}
	controlCh chan *NodeMessage
	recvCh    chan *NodeMessage
}

func NewTopo(parCtx context.Context) *Topo {
	ctx, cancel := context.WithCancel(parCtx)
	t := &Topo{
		ctx:       ctx,
		cancel:    cancel,
		operators: make(map[int]NodeOperator),
		exitCh:    make(chan struct{}),
		controlCh: make(chan *NodeMessage, 8),
		recvCh:    make(chan *NodeMessage, 8),
	}
	return t
}

func (t *Topo) Start() error {
	fmt.Println("topo send start signal")
	t.controlCh <- NewSignalMsg(StartRuleSignal)
	select {
	case <-t.ctx.Done():
		return nil
	case msg := <-t.recvCh:
		if msg.IsControlSignal(StartRuleSignal) {
			fmt.Println("topo recv start signal")
			return nil
		} else {
			return fmt.Errorf("invalid start rule signal")
		}
	}
}

func (t *Topo) Stop() error {
	fmt.Println("topo send start signal")
	t.controlCh <- NewSignalMsg(StopRuleSignal)
	for {
		select {
		case <-t.ctx.Done():
			return nil
		case msg := <-t.recvCh:
			if msg.IsControlSignal(StopRuleSignal) {
				fmt.Println("topo recv stop signal")
				return nil
			}
		}
	}
}

func (t *Topo) Release() {
	t.cancel()
}

func CreateTopo(ctx context.Context, physicalPlanEnd planner.PhysicalPlan) (*Topo, error) {
	t := NewTopo(ctx)
	if err := t.buildNodes(ctx, physicalPlanEnd, "topo", t.recvCh); err != nil {
		return nil, err
	}
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
	case *planner.PhysicalDataSink:
		op := NewSinkNode(p)
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
	node.Run(t.ctx)
	fmt.Println(fmt.Sprintf("node %v running", node.GetName()))
	for _, child := range lp.GetChildren() {
		t.buildNodes(ctx, child, node.GetName(), node.GetInput())
	}
	return nil
}
