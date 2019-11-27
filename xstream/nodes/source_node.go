package nodes

import (
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
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

func (m *SourceNode) Open(ctx api.StreamContext, errCh chan<- error) {
	m.ctx = ctx
	logger := ctx.GetLogger()
	logger.Debugf("open source node %s", m.name)
	go func(){
		if err := m.source.Open(ctx, func(message map[string]interface{}, meta map[string]interface{}){
			tuple := &xsql.Tuple{Emitter: m.name, Message:message, Timestamp: common.GetNowInMilli(), Metadata:meta}
			m.Broadcast(tuple)
			logger.Debugf("%s consume data %v complete", m.name, tuple)
		}); err != nil{
			select {
			case errCh <- err:
			case <-ctx.Done():
				if err := m.source.Close(ctx); err != nil{
					go func() { errCh <- err }()
				}
			}
		}
		for {
			select {
			case <-ctx.Done():
				logger.Infof("source %s done", m.name)
				if err := m.source.Close(ctx); err != nil{
					go func() { errCh <- err }()
				}
				return
			}
		}
	}()
}

func (m *SourceNode) Broadcast(data interface{}) int{
	return Broadcast(m.outs, data, m.ctx)
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
