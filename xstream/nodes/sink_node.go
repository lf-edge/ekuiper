package nodes

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/common/templates"
	"github.com/emqx/kuiper/plugins"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/sinks"
	"sync"
	"text/template"
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
func NewSinkNodeWithSink(name string, sink api.Sink, props map[string]interface{}) *SinkNode {
	return &SinkNode{
		input:       make(chan interface{}, 1024),
		name:        name,
		sinks:       []api.Sink{sink},
		options:     props,
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
		omitIfEmpty := false
		if c, ok := m.options["omitIfEmpty"]; ok {
			if t, ok := c.(bool); !ok {
				logger.Warnf("invalid type for omitIfEmpty property, should be a bool value 'true/false'.", c)
			} else {
				omitIfEmpty = t
			}
		}
		sendSingle := false
		if c, ok := m.options["sendSingle"]; ok {
			if t, ok := c.(bool); !ok {
				logger.Warnf("invalid type for sendSingle property, should be a bool value 'true/false'.", c)
			} else {
				sendSingle = t
			}
		}
		var tp *template.Template = nil
		if c, ok := m.options["dataTemplate"]; ok {
			if t, ok := c.(string); !ok {
				logger.Warnf("invalid type for dateTemplate property, should be a string value.", c)
			} else {
				funcMap := template.FuncMap{
					"json": templates.JsonMarshal,
				}
				temp, err := template.New("sink").Funcs(funcMap).Parse(t)
				if err != nil {
					logger.Warnf("property dataTemplate %v is invalid: %v", t, err)
				} else {
					tp = temp
				}
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
							go doCollect(sink, data, stats, retryInterval, omitIfEmpty, sendSingle, tp, cache.Complete, ctx)
						} else {
							doCollect(sink, data, stats, retryInterval, omitIfEmpty, sendSingle, tp, cache.Complete, ctx)
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

func extractInput(v []byte) ([]map[string]interface{}, error) {
	var j []map[string]interface{}
	if err := json.Unmarshal(v, &j); err != nil {
		return nil, fmt.Errorf("fail to decode the input %s as json: %v", v, err)
	}
	return j, nil
}

func doCollect(sink api.Sink, item *CacheTuple, stats StatManager, retryInterval int, omitIfEmpty bool, sendSingle bool, tp *template.Template, signalCh chan<- int, ctx api.StreamContext) {
	stats.IncTotalRecordsIn()
	stats.ProcessTimeStart()
	defer stats.ProcessTimeEnd()
	logger := ctx.GetLogger()
	var outdatas [][]byte
	switch val := item.data.(type) {
	case []byte:
		if omitIfEmpty && string(val) == "[{}]" {
			return
		}
		var (
			err error
			j   []map[string]interface{}
		)
		if sendSingle || tp != nil {
			j, err = extractInput(val)
			if err != nil {
				stats.IncTotalExceptions()
				logger.Warnf("sink node %s instance %d publish %s error: %v", ctx.GetOpId(), ctx.GetInstanceId(), val, err)
				return
			}
			logger.Debugf("receive %d records", len(j))
		}
		if !sendSingle {
			if tp != nil {
				var output bytes.Buffer
				err := tp.Execute(&output, j)
				if err != nil {
					logger.Warnf("sink node %s instance %d publish %s decode template error: %v", ctx.GetOpId(), ctx.GetInstanceId(), val, err)
					stats.IncTotalExceptions()
					return
				}
				outdatas = append(outdatas, output.Bytes())
			} else {
				outdatas = [][]byte{val}
			}
		} else {
			for _, r := range j {
				var output bytes.Buffer
				err := tp.Execute(&output, r)
				if err != nil {
					logger.Warnf("sink node %s instance %d publish %s decode template error: %v", ctx.GetOpId(), ctx.GetInstanceId(), val, err)
					stats.IncTotalExceptions()
					return
				}
				outdatas = append(outdatas, output.Bytes())
			}
		}

	case error:
		outdatas = [][]byte{[]byte(fmt.Sprintf(`[{"error":"%s"}]`, val.Error()))}
	default:
		outdatas = [][]byte{[]byte(fmt.Sprintf(`[{"error":"result is not a string but found %#v"}]`, val))}
	}

	for _, outdata := range outdatas {
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
	}
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
	case "nop":
		s = &sinks.NopSink{}
	default:
		nf, err := plugins.GetPlugin(name, plugins.SINK)
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
