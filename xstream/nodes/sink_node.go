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
	*defaultSinkNode
	//static
	sinkType string
	mutex    sync.RWMutex
	//configs (also static for sinks)
	options map[string]interface{}
	isMock  bool
	//states varies after restart
	sinks []api.Sink
	tch   chan struct{} //channel to trigger cache saved, will be trigger by checkpoint only
}

func NewSinkNode(name string, sinkType string, props map[string]interface{}) *SinkNode {
	bufferLength := 1024
	if c, ok := props["bufferLength"]; ok {
		if t, err := common.ToInt(c, common.STRICT); err != nil || t <= 0 {
			//invalid property bufferLength
		} else {
			bufferLength = t
		}
	}
	return &SinkNode{
		defaultSinkNode: &defaultSinkNode{
			input: make(chan interface{}, bufferLength),
			defaultNode: &defaultNode{
				name:        name,
				concurrency: 1,
				ctx:         nil,
			},
		},
		sinkType: sinkType,
		options:  props,
	}
}

//Only for mock source, do not use it in production
func NewSinkNodeWithSink(name string, sink api.Sink, props map[string]interface{}) *SinkNode {
	return &SinkNode{
		defaultSinkNode: &defaultSinkNode{
			input: make(chan interface{}, 1024),
			defaultNode: &defaultNode{
				name:        name,
				concurrency: 1,
				ctx:         nil,
			},
		},
		sinks:   []api.Sink{sink},
		options: props,
		isMock:  true,
	}
}

func (m *SinkNode) Open(ctx api.StreamContext, result chan<- error) {
	m.ctx = ctx
	logger := ctx.GetLogger()
	logger.Debugf("open sink node %s", m.name)
	if m.qos >= api.AtLeastOnce {
		m.tch = make(chan struct{})
	}
	go func() {
		if c, ok := m.options["concurrency"]; ok {
			if t, err := common.ToInt(c, common.STRICT); err != nil || t <= 0 {
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
			if t, err := common.ToInt(c, common.STRICT); err != nil || t < 0 {
				logger.Warnf("invalid type for retryInterval property, should be positive integer but found %t", c)
			} else {
				retryInterval = t
			}
		}
		retryCount := 3
		if c, ok := m.options["retryCount"]; ok {
			if t, err := common.ToInt(c, common.STRICT); err != nil || t < 0 {
				logger.Warnf("invalid type for retryCount property, should be positive integer but found %t", c)
			} else {
				retryCount = t
			}
		}
		cacheLength := 1024
		if c, ok := m.options["cacheLength"]; ok {
			if t, err := common.ToInt(c, common.STRICT); err != nil || t < 0 {
				logger.Warnf("invalid type for cacheLength property, should be positive integer but found %t", c)
			} else {
				cacheLength = t
			}
		}
		cacheSaveInterval := 1000
		if c, ok := m.options["cacheSaveInterval"]; ok {
			if t, err := common.ToInt(c, common.STRICT); err != nil || t < 0 {
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
					"json":   templates.JsonMarshal,
					"base64": templates.Base64Encode,
					"add":    templates.Add,
				}
				temp, err := template.New("sink").Funcs(funcMap).Parse(t)
				if err != nil {
					msg := fmt.Sprintf("property dataTemplate %v is invalid: %v", t, err)
					logger.Warnf(msg)
					result <- fmt.Errorf(msg)
					return
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
					logger.Debugf("Trying to get sink for rule %s with options %v\n", ctx.GetRuleId(), m.options)
					sink, err = getSink(m.sinkType, m.options)
					if err != nil {
						m.drainError(result, err, ctx, logger)
						return
					}
					logger.Debugf("Successfully get the sink %s", m.sinkType)
					m.mutex.Lock()
					m.sinks = append(m.sinks, sink)
					m.mutex.Unlock()
					logger.Debugf("Now is to open sink for rule %s.\n", ctx.GetRuleId())
					if err := sink.Open(ctx); err != nil {
						m.drainError(result, err, ctx, logger)
						return
					}
					logger.Debugf("Successfully open sink for rule %s.\n", ctx.GetRuleId())
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

				if common.Config.Sink.DisableCache {
					for {
						select {
						case data := <-m.input:
							if newdata, processed := m.preprocess(data); processed {
								break
							} else {
								data = newdata
							}
							stats.SetBufferLength(int64(len(m.input)))
							if runAsync {
								go doCollect(sink, data, stats, omitIfEmpty, sendSingle, tp, ctx)
							} else {
								doCollect(sink, data, stats, omitIfEmpty, sendSingle, tp, ctx)
							}
						case <-ctx.Done():
							logger.Infof("sink node %s instance %d done", m.name, instance)
							if err := sink.Close(ctx); err != nil {
								logger.Warnf("close sink node %s instance %d fails: %v", m.name, instance, err)
							}
							return
						case <-m.tch:
							logger.Debugf("rule %s sink receive checkpoint, do nothing", ctx.GetRuleId())
						}
					}
				} else {
					logger.Infof("Creating sink cache")
					var cache *Cache
					if m.qos >= api.AtLeastOnce {
						cache = NewCheckpointbasedCache(m.input, cacheLength, m.tch, result, ctx)
					} else {
						cache = NewTimebasedCache(m.input, cacheLength, cacheSaveInterval, result, ctx)
					}
					for {
						select {
						case data := <-cache.Out:
							if newdata, processed := m.preprocess(data.data); processed {
								break
							} else {
								data.data = newdata
							}
							stats.SetBufferLength(int64(len(m.input)))
							if runAsync {
								go doCollectCacheTuple(sink, data, stats, retryInterval, retryCount, omitIfEmpty, sendSingle, tp, cache.Complete, ctx)
							} else {
								doCollectCacheTuple(sink, data, stats, retryInterval, retryCount, omitIfEmpty, sendSingle, tp, cache.Complete, ctx)
							}
						case <-ctx.Done():
							logger.Infof("sink node %s instance %d done", m.name, instance)
							if err := sink.Close(ctx); err != nil {
								logger.Warnf("close sink node %s instance %d fails: %v", m.name, instance, err)
							}
							return
						}
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

func doCollect(sink api.Sink, item interface{}, stats StatManager, omitIfEmpty bool, sendSingle bool, tp *template.Template, ctx api.StreamContext) {
	stats.IncTotalRecordsIn()
	stats.ProcessTimeStart()
	defer stats.ProcessTimeEnd()
	logger := ctx.GetLogger()
	outdatas := getOutData(stats, ctx, item, omitIfEmpty, sendSingle, tp)

	for _, outdata := range outdatas {
		if err := sink.Collect(ctx, outdata); err != nil {
			stats.IncTotalExceptions()
			logger.Warnf("sink node %s instance %d publish %s error: %v", ctx.GetOpId(), ctx.GetInstanceId(), outdata, err)
		} else {
			stats.IncTotalRecordsOut()
		}
	}
}

func getOutData(stats StatManager, ctx api.StreamContext, item interface{}, omitIfEmpty bool, sendSingle bool, tp *template.Template) [][]byte {
	logger := ctx.GetLogger()
	var outdatas [][]byte
	switch val := item.(type) {
	case []byte:
		if omitIfEmpty && string(val) == "[{}]" {
			return nil
		}
		var (
			err error
			j   []map[string]interface{}
		)
		if sendSingle || tp != nil {
			j, err = extractInput(val)
			if err != nil {
				logger.Warnf("sink node %s instance %d publish %s error: %v", ctx.GetOpId(), ctx.GetInstanceId(), val, err)
				stats.IncTotalExceptions()
				return nil
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
					return nil
				}
				outdatas = append(outdatas, output.Bytes())
			} else {
				outdatas = [][]byte{val}
			}
		} else {
			for _, r := range j {
				if tp != nil {
					var output bytes.Buffer
					err := tp.Execute(&output, r)
					if err != nil {
						logger.Warnf("sink node %s instance %d publish %s decode template error: %v", ctx.GetOpId(), ctx.GetInstanceId(), val, err)
						stats.IncTotalExceptions()
						return nil
					}
					outdatas = append(outdatas, output.Bytes())
				} else {
					if ot, e := json.Marshal(r); e != nil {
						logger.Warnf("sink node %s instance %d publish %s marshal error: %v", ctx.GetOpId(), ctx.GetInstanceId(), r, e)
						stats.IncTotalExceptions()
						return nil
					} else {
						outdatas = append(outdatas, ot)
					}
				}
			}
		}

	case error:
		outdatas = [][]byte{[]byte(fmt.Sprintf(`[{"error":"%s"}]`, val.Error()))}
	default:
		outdatas = [][]byte{[]byte(fmt.Sprintf(`[{"error":"result is not a string but found %#v"}]`, val))}
	}
	return outdatas
}

func doCollectCacheTuple(sink api.Sink, item *CacheTuple, stats StatManager, retryInterval, retryCount int, omitIfEmpty bool, sendSingle bool, tp *template.Template, signalCh chan<- int, ctx api.StreamContext) {
	stats.IncTotalRecordsIn()
	stats.ProcessTimeStart()
	defer stats.ProcessTimeEnd()
	logger := ctx.GetLogger()
	outdatas := getOutData(stats, ctx, item.data, omitIfEmpty, sendSingle, tp)
	for _, outdata := range outdatas {
	outerloop:
		for {
			select {
			case <-ctx.Done():
				logger.Infof("sink node %s instance %d stops data resending", ctx.GetOpId(), ctx.GetInstanceId())
				return
			default:
				if err := sink.Collect(ctx, outdata); err != nil {
					stats.IncTotalExceptions()
					logger.Warnf("sink node %s instance %d publish %s error: %v", ctx.GetOpId(), ctx.GetInstanceId(), outdata, err)
					if retryInterval > 0 && retryCount > 0 {
						retryCount--
						time.Sleep(time.Duration(retryInterval) * time.Millisecond)
						logger.Debugf("try again")
					} else {
						break outerloop
					}
				} else {
					logger.Debugf("success")
					stats.IncTotalRecordsOut()
					select {
					case signalCh <- item.index:
					default:
						logger.Warnf("sink cache missing response for %d", item.index)
					}

					break outerloop
				}
			}
		}
	}
}

func doGetSink(name string, action map[string]interface{}) (api.Sink, error) {
	var (
		s   api.Sink
		err error
	)
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
		s, err = plugins.GetSink(name)
		if err != nil {
			return nil, err
		}
	}
	err = s.Configure(action)
	if err != nil {
		return nil, err
	}
	return s, nil
}

//Override defaultNode
func (m *SinkNode) AddOutput(_ chan<- interface{}, name string) error {
	return fmt.Errorf("fail to add output %s, sink %s cannot add output", name, m.name)
}

//Override defaultNode
func (m *SinkNode) Broadcast(_ interface{}) error {
	return fmt.Errorf("sink %s cannot add broadcast", m.name)
}

func (m *SinkNode) drainError(errCh chan<- error, err error, ctx api.StreamContext, logger api.Logger) {
	go func() {
		select {
		case errCh <- err:
			ctx.GetLogger().Errorf("error in sink %s", err)
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
	if m.tch != nil {
		close(m.tch)
		m.tch = nil
	}
}

// Only called when checkpoint enabled
func (m *SinkNode) SaveCache() {
	m.tch <- struct{}{}
}
