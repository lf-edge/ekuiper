package node

import (
	"context"
	"fmt"
)

type NodeOperator interface {
	AddOutput(key string, output chan *NodeMessage)
	GetInput() chan *NodeMessage
	GetName() string
	BroadCast(msg *NodeMessage)
	Run(ctx context.Context)
	Close(ctx context.Context)
}

type NodeStatus struct {
	RecvStartSignalCount int
	RecvStopSignalCount  int
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
	b.CountOfChannelSendToMe = CountOfChannelSendToMe
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

func (b *BaseNode) HandleNodeMsg(ctx context.Context, msg *NodeMessage) (processed bool, send bool) {
	if msg.Control != nil {
		switch msg.Control.ControlSignal {
		case StartRuleSignal:
			return true, b.HandleStartSignal()
		case StopRuleSignal:
			return true, b.HandleStopSignal(ctx)
		}
	}
	return false, false
}

func (b *BaseNode) Close(ctx context.Context) {
	fmt.Println(fmt.Sprintf("%v_%v recv and send stop signal", b.Name, b.Index))
	return
}

func (b *BaseNode) HandleStartSignal() bool {
	b.Status.RecvStartSignalCount++
	send := b.Status.RecvStartSignalCount >= b.CountOfChannelSendToMe
	if send {
		fmt.Println(fmt.Sprintf("%v_%v recv and send start signal", b.Name, b.Index))
	}
	return send
}

func (b *BaseNode) HandleStopSignal(ctx context.Context) bool {
	b.Status.RecvStopSignalCount++
	send := b.Status.RecvStopSignalCount >= b.CountOfChannelSendToMe
	if send {
		b.Close(ctx)
	}
	return send
}

func (b *BaseNode) BroadCast(msg *NodeMessage) {
	for _, output := range b.Output {
		output <- msg
	}
}
