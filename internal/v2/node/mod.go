package node

type NodeOperator interface {
	AddOutput(key string, output chan *NodeMessage)
	GetInput() chan *NodeMessage
}

type BaseNode struct {
	Index  int
	Name   string
	Input  chan *NodeMessage
	Output map[string]chan *NodeMessage
}

func NewBaseNode(Index int, Name string) *BaseNode {
	b := &BaseNode{Index: Index, Name: Name}
	b.Input = make(chan *NodeMessage, 1024)
	b.Output = make(map[string]chan *NodeMessage)
	return b
}

func (b *BaseNode) AddOutput(key string, output chan *NodeMessage) {
	b.Output[key] = output
}

func (b *BaseNode) GetInput() chan *NodeMessage {
	return b.Input
}

type NodeMessage struct {
}

type SourceNode struct {
	*BaseNode
}

type StakeNode struct {
	*BaseNode
}

type ProjectNode struct {
	*BaseNode
}
