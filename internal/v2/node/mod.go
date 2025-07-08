package node

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lf-edge/ekuiper/v2/internal/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/v2/catalog"
	"github.com/lf-edge/ekuiper/v2/internal/v2/planner"
)

type NodeOperator interface {
	AddOutput(key string, output chan *NodeMessage)
	GetInput() chan *NodeMessage
	GetName() string
	BroadCast(msg *NodeMessage)
}

type NodeStatus struct {
	RecvStartSignalCount int
}

type BaseNode struct {
	Index  int
	Name   string
	Input  chan *NodeMessage
	Output map[string]chan *NodeMessage

	CountOfChannelSendToMe int
	Status                 NodeStatus
}

func (b *BaseNode) GetName() string {
	return fmt.Sprintf("%s_%v", b.Name, b.Index)
}

func NewBaseNode(Index int, Name string, CountOfChannelSendToMe int) *BaseNode {
	b := &BaseNode{Index: Index, Name: Name}
	b.Input = make(chan *NodeMessage, 1024)
	b.Output = make(map[string]chan *NodeMessage)
	return b
}

func (b *BaseNode) AddOutput(key string, output chan *NodeMessage) {
	if output == nil {
		return
	}
	b.Output[key] = output
}

func (b *BaseNode) GetInput() chan *NodeMessage {
	return b.Input
}

func (b *BaseNode) HandleNodeMsg(msg *NodeMessage) (bool, bool) {
	if msg.StartRuleSignal {
		return true, b.HandleStartSignal()
	}
	return false, false
}

func (b *BaseNode) HandleStartSignal() bool {
	b.Status.RecvStartSignalCount++
	return b.Status.RecvStartSignalCount >= b.CountOfChannelSendToMe
}

func (b *BaseNode) BroadCast(msg *NodeMessage) {
	for _, output := range b.Output {
		output <- msg
	}
}

type NodeMessage struct {
	StartRuleSignal bool
	Err             error
	Tuples          []*api.Tuple
}

func (nm *NodeMessage) TupleString() string {
	m := make([]map[string]any, 0)
	for _, tuple := range nm.Tuples {
		m = append(m, tuple.ToMap())
	}
	v, _ := json.Marshal(m)
	return string(v)
}

type SourceNode struct {
	Stream  *catalog.Stream
	Started bool
	*BaseNode
}

func NewSourceNode(pp *planner.PhysicalDataSource) *SourceNode {
	sn := &SourceNode{BaseNode: NewBaseNode(pp.GetIndex(), "source", len(pp.GetChildren()))}
	sn.Stream = pp.Stream
	return sn
}

func (sn *SourceNode) run(ctx context.Context) {
	var ticker *time.Ticker
	defer func() {
		if ticker != nil {
			ticker.Stop()
		}
	}()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-sn.Input:
				processd, send := sn.HandleNodeMsg(msg)
				if processd {
					if send {
						sn.BroadCast(msg)
						if msg.StartRuleSignal && !sn.Started {
							sn.Started = true
							break
						}
					}
					continue
				}
				sn.BroadCast(msg)
			}
			if sn.Started {
				break
			}
		}
		ticker = time.NewTicker(time.Second)
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-sn.Input:
				processd, send := sn.HandleNodeMsg(msg)
				if processd {
					if send {
						sn.BroadCast(msg)
					}
					continue
				}
				sn.BroadCast(msg)
			case <-ticker.C:
				msg := &NodeMessage{Tuples: make([]*api.Tuple, 0)}
				t, err := api.NewTuple(sn.Stream.StreamName, map[string]any{"key": "value"})
				if err != nil {
					msg.Err = err
				} else {
					msg.Tuples = append(msg.Tuples, t)
				}
				sn.BroadCast(msg)
			}
		}
	}()
}

type StakeNode struct {
	IsEnd  bool
	IsRoot bool
	*BaseNode
}

func NewStakeNode(pp *planner.PhysicalStake) *StakeNode {
	sn := &StakeNode{BaseNode: NewBaseNode(pp.GetIndex(), "stake", len(pp.GetChildren()))}
	return sn
}

func (sn *StakeNode) run(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-sn.Input:
				processd, send := sn.HandleNodeMsg(msg)
				if processd {
					if send {
						sn.BroadCast(msg)
					}
					continue
				}
				sn.BroadCast(msg)
			}
		}
	}()
}

type ProjectNode struct {
	*BaseNode
}

func NewProjectNode(pp *planner.PhysicalProject) *ProjectNode {
	return &ProjectNode{NewBaseNode(pp.GetIndex(), "project", len(pp.GetChildren()))}
}

func (pn *ProjectNode) run(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-pn.Input:
				processd, send := pn.HandleNodeMsg(msg)
				if processd {
					if send {
						pn.BroadCast(msg)
					}
					continue
				}
				if len(msg.Tuples) > 0 {
					fmt.Println(msg.TupleString())
				}
				//pn.BroadCast(msg)
			}
		}
	}()
}
