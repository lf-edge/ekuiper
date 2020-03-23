package nodes

import (
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/common/plugin_manager"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/sinks"
	"sync"
	"time"
)

type SinkNode struct {
	//static
	input    chan interface{}
	name     string
	sinkType string
	mutex    sync.RWMutex
	//configs (also static for sinks)
	concurrency int
	options     map[string]interface{}
	isMock      bool
	//states varies after restart
	ctx          api.StreamContext
	statManagers []StatManager
	sinks        []api.Sink
}

func NewSinkNode(name string, sinkType string, props map[string]interface{}) *SinkNode {
	bufferLength := 1024
	if c, ok := props["bufferLength"]; ok {
		if t, err := common.ToInt(c); err != nil || t <= 0 {
			//invalid property bufferLength
		} else {
			bufferLength = t
		}
	}
	return &SinkNode{
		input:       make(chan interface{}, bufferLength),
		name:        name,
		sinkType:    sinkType,
		options:     props,
		concurrency: 1,
		ctx:         nil,
	}
}

//Only for mock source, do not use it in production
func NewSinkNodeWithSink(name string, sink api.Sink) *SinkNode {
	return &SinkNode{
		input:       make(chan interface{}, 1024),
		name:        name,
		sinks:       []api.Sink{sink},
		options:     nil,
		concurrency: 1,
		ctx:         nil,
		isMock:      true,
	}
}

func (m *SinkNode) Open(ctx api.StreamContext, result chan<- error) {
	m.ctx = ctx
	logger := ctx.GetLogger()
	logger.Debugf("open sink node %s", m.name)
	go func() {
		if c, ok := m.options["concurrency"]; ok {
			if t, err := common.ToInt(c); err != nil || t <= 0 {
				logger.Warnf("invalid type for concurrency property, should be positive integer but found %t", c)
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
		retryInterval := 1000
		if c, ok := m.options["retryInterval"]; ok {
			if t, err := common.ToInt(c); err != nil || t < 0 {
				logger.Warnf("invalid type for retryInterval property, should be positive integer but found %t", c)
			} else {
				retryInterval = t
			}
		}
		cacheLength := 1024
		if c, ok := m.options["cacheLength"]; ok {
			if t, err := common.ToInt(c); err != nil || t < 0 {
				logger.Warnf("invalid type for cacheLength property, should be positive integer but found %t", c)
			} else {
				cacheLength = t
			}
		}
		cacheSaveInterval := 1000
		if c, ok := m.options["cacheSaveInterval"]; ok {
			if t, err := common.ToInt(c); err != nil || t < 0 {
				logger.Warnf("invalid type for cacheSaveInterval property, should be positive integer but found %t", c)
			} else {
				cacheSaveInterval = t
			}
		}
		m.reset()
		logger.Infof("open sink node %d instances", m.concurrency)
		for i := 0; i < m.concurrency; i++ { // workers
			go func(instance int) {
				var sink api.Sink
				var err error
				if !m.isMock {
					sink, err = getSink(m.sinkType, m.options)
					if err != nil {
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
				} else {
					sink = m.sinks[instance]
				}

				stats, err := NewStatManager("sink", ctx)
				if err != nil {
					m.drainError(result, err, ctx, logger)
					return
				}
				m.mutex.Lock()
				m.statManagers = append(m.statManagers, stats)
				m.mutex.Unlock()

				cache := NewCache(m.input, cacheLength, cacheSaveInterval, result, ctx)
				for {
					select {
					case data := <-cache.Out:
						stats.SetBufferLength(int64(cache.Length()))
						if runAsync {
							go doCollect(sink, data, stats, retryInterval, cache.Complete, ctx)
						} else {
							doCollect(sink, data, stats, retryInterval, cache.Complete, ctx)
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

func (m *SinkNode) reset() {
	if !m.isMock {
		m.sinks = nil
	}
	m.statManagers = nil
}

func doCollect(sink api.Sink, item *CacheTuple, stats StatManager, retryInterval int, signalCh chan<- int, ctx api.StreamContext) {
	stats.IncTotalRecordsIn()
	stats.ProcessTimeStart()
	logger := ctx.GetLogger()
	var outdata []byte
	switch val := item.data.(type) {
	case []byte:
		outdata = val
	case error:
		outdata = []byte(fmt.Sprintf(`[{"error":"%s"}]`, val.Error()))
	default:
		outdata = []byte(fmt.Sprintf(`[{"error":"result is not a string but found %#v"}]`, val))
	}
	for {
		if err := sink.Collect(ctx, outdata); err != nil {
			stats.IncTotalExceptions()
			logger.Warnf("sink node %s instance %d publish %s error: %v", ctx.GetOpId(), ctx.GetInstanceId(), outdata, err)
			if retryInterval > 0 {
				time.Sleep(time.Duration(retryInterval) * time.Millisecond)
				logger.Debugf("try again")
			} else {
				break
			}
		} else {
			logger.Debugf("success")
			stats.IncTotalRecordsOut()
			signalCh <- item.index
			break
		}
	}
	stats.ProcessTimeEnd()
}

func doGetSink(name string, action map[string]interface{}) (api.Sink, error) {
	var s api.Sink
	switch name {
	case "log":
		s = sinks.NewLogSink()
	case "logToMemory":
		s = sinks.NewLogSinkToMemory()
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

func (m *SinkNode) GetName() string {
	return m.name
}

func (m *SinkNode) GetInput() (chan<- interface{}, string) {
	return m.input, m.name
}

func (m *SinkNode) GetMetrics() (result [][]interface{}) {
	for _, stats := range m.statManagers {
		result = append(result, stats.GetMetrics())
	}
	return result
}

func (m *SinkNode) drainError(errCh chan<- error, err error, ctx api.StreamContext, logger api.Logger) {
	go func() {
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
