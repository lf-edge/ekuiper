package nodes

import (
	"github.com/emqx/kuiper/xstream/api"
)

type SinkNode struct {
	sink   api.Sink
	input  chan interface{}
	name   string
	ctx    api.StreamContext
}

func NewSinkNode(name string, sink api.Sink) *SinkNode{
	return &SinkNode{
		sink: sink,
		input: make(chan interface{}, 1024),
		name: name,
		ctx: nil,
	}
}

func (m *SinkNode) Open(ctx api.StreamContext, result chan<- error) {
	m.ctx = ctx
	logger := ctx.GetLogger()
	logger.Debugf("open sink node %s", m.name)
	go func() {
		if err := m.sink.Open(ctx); err != nil{
			go func() {
				select{
				case result <- err:
				case <-ctx.Done():
				}
			}()
			return
		}
		for {
			select {
			case item := <-m.input:
				if err := m.sink.Collect(ctx, item); err != nil{
					//TODO deal with publish error
					logger.Errorf("sink node %s publish %v error: %v", ctx.GetOpId(), item, err)
				}
			case <-ctx.Done():
				logger.Infof("sink node %s done", m.name)
				if err := m.sink.Close(ctx); err != nil{
					logger.Warnf("close sink node %s fails: %v", m.name, err)
				}
				return
			}
		}
	}()
}

func (m *SinkNode) GetName() string{
	return m.name
}

func (m *SinkNode) GetInput() (chan<- interface{}, string)  {
	return m.input, m.name
}
