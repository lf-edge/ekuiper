package nodes

import (
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/common/plugin_manager"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/sinks"
	"sync"
)

type SinkNode struct {
	input  chan interface{}
	name   string
	ctx    api.StreamContext
	concurrency int

	sinkType string
	options map[string]interface{}
	mutex   sync.RWMutex
	sinks []api.Sink
}

func NewSinkNode(name string, sinkType string, props map[string]interface{}) *SinkNode{
	return &SinkNode{
		input: make(chan interface{}, 1024),
		name: name,
		sinkType: sinkType,
		options: props,
		concurrency: 1,
		ctx: nil,
	}
}

//Only for mock source, do not use it in production
func NewSinkNodeWithSink(name string, sink api.Sink) *SinkNode{
	return &SinkNode{
		input: make(chan interface{}, 1024),
		name: name,
		sinks: []api.Sink{sink},
		options: nil,
		concurrency: 1,
		ctx: nil,
	}
}

func (m *SinkNode) Open(ctx api.StreamContext, result chan<- error) {
	m.ctx = ctx
	logger := ctx.GetLogger()
	logger.Debugf("open sink node %s", m.name)
	go func() {
		if c, ok := m.options["concurrency"]; ok {
			if t, err := common.ToInt(c); err != nil {
				logger.Warnf("invalid type for concurrency property, should be int but found %t", c)
			} else {
				m.concurrency = t
			}
		}
		runAsync := false
		if c, ok := m.options["runAsync"]; ok {
			if t, ok := c.(bool); !ok {
				logger.Warnf("invalid type for runAsync property, should be bool but found %t", c)
			} else {
				runAsync = t
			}
		}
		createSink := len(m.sinks) == 0
		logger.Infof("open sink node %d instances", m.concurrency)
		for i := 0; i < m.concurrency; i++ { // workers
			go func(instance int){
				var sink api.Sink
				if createSink{
					sink, err := getSink(m.sinkType, m.options)
					if err != nil{
						m.drainError(result, err, ctx, logger)
						return
					}
					m.mutex.Lock()
					m.sinks = append(m.sinks, sink)
					m.mutex.Unlock()
					if err := sink.Open(ctx); err != nil {
						m.drainError(result, err, ctx, logger)
						return
					}
				}else{
					sink = m.sinks[instance]
				}

				for {
					select {
					case item := <-m.input:
						if runAsync{
							go doCollect(sink, ctx, item, logger)
						} else {
							doCollect(sink, ctx, item, logger)
						}

					case <-ctx.Done():
						logger.Infof("sink node %s instance %d done", m.name, instance)
						if err := sink.Close(ctx); err != nil {
							logger.Warnf("close sink node %s instance %d fails: %v", m.name, instance, err)
						}
						return
					}
				}
			}(i)
		}
	}()
}

func doCollect(sink api.Sink, ctx api.StreamContext, item interface{}, logger api.Logger) {
	if err := sink.Collect(ctx, item); err != nil {
		//TODO deal with publish error
		logger.Errorf("sink node %s publish %v error: %v", ctx.GetOpId(), item, err)
	}
}

func getSink(name string, action map[string]interface{}) (api.Sink, error) {
	var s api.Sink
	switch name {
	case "log":
		s = sinks.NewLogSink()
	case "mqtt":
		s = &sinks.MQTTSink{}
	case "rest":
		s = &sinks.RestSink{}
	default:
		nf, err := plugin_manager.GetPlugin(name, "sinks")
		if err != nil {
			return nil, err
		}
		var ok bool
		s, ok = nf.(api.Sink)
		if !ok {
			return nil, fmt.Errorf("exported symbol %s is not type of api.Sink", name)
		}
	}

	err := s.Configure(action)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (m *SinkNode) GetName() string{
	return m.name
}

func (m *SinkNode) GetInput() (chan<- interface{}, string)  {
	return m.input, m.name
}

func (m *SinkNode) drainError(errCh chan<- error, err error, ctx api.StreamContext, logger api.Logger) {
	go func(){
		select {
		case errCh <- err:
		case <-ctx.Done():
			m.close(ctx, logger)
		}
	}()
}

func (m *SinkNode) close(ctx api.StreamContext, logger api.Logger) {
	for _, s := range m.sinks {
		if err := s.Close(ctx); err != nil {
			logger.Warnf("close sink fails: %v", err)
		}
	}
}