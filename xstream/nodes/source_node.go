package nodes

import (
	"engine/xstream/api"
	"fmt"
)

type SourceNode struct {
	source api.Source
	outs   map[string]chan<- interface{}
	name   string
	ctx    api.StreamContext
}

func NewSourceNode(name string, source api.Source) *SourceNode{
	return &SourceNode{
		source: source,
		outs: make(map[string]chan<- interface{}),
		name: name,
		ctx: nil,
	}
}

func (m *SourceNode) Open(ctx api.StreamContext) error {
	m.ctx = ctx
	logger := ctx.GetLogger()
	logger.Debugf("open source node %s", m.name)
	return m.source.Open(ctx, func(data interface{}){
		m.Broadcast(data)
		logger.Debugf("%s consume data %v complete", m.name, data)
	})
}

func (m *SourceNode) Broadcast(data interface{}) (err error){
	return Broadcast(m.outs, data)
}

func (m *SourceNode) GetName() string{
	return m.name
}

func (m *SourceNode) AddOutput(output chan<- interface{}, name string) (err error) {
	if _, ok := m.outs[name]; !ok{
		m.outs[name] = output
	}else{
		return fmt.Errorf("fail to add output %s, stream node %s already has an output of the same name", name, m.name)
	}
	return nil
}
