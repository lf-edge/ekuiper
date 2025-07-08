package node

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/lf-edge/ekuiper/v2/internal/v2/api"
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

func (b *BaseNode) HandleNodeMsg(msg *NodeMessage) (processed bool, send bool) {
	if msg.StartRuleSignal {
		return true, b.HandleStartSignal()
	}
	return false, false
}

func (b *BaseNode) Close(ctx context.Context) {
	return
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
