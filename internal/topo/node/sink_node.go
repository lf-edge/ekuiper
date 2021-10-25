// Copyright 2022 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package node

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/binder/io"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/transform"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/errorx"
	"strings"
	"sync"
	"time"
)

type SinkConf struct {
	Concurrency       int    `json:"concurrency"`
	RunAsync          bool   `json:"runAsync"`
	RetryInterval     int    `json:"retryInterval"`
	RetryCount        int    `json:"retryCount"`
	CacheLength       int    `json:"cacheLength"`
	CacheSaveInterval int    `json:"cacheSaveInterval"`
	Omitempty         bool   `json:"omitIfEmpty"`
	SendSingle        bool   `json:"sendSingle"`
	DataTemplate      string `json:"dataTemplate"`
}

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
		if t, err := cast.ToInt(c, cast.STRICT); err != nil || t <= 0 {
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

// NewSinkNodeWithSink Only for mock source, do not use it in production
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
		sconf := &SinkConf{
			Concurrency:       1,
			RunAsync:          false,
			RetryInterval:     1000,
			RetryCount:        0,
			CacheLength:       1024,
			CacheSaveInterval: 1000,
			Omitempty:         false,
			SendSingle:        false,
			DataTemplate:      "",
		}
		err := cast.MapToStruct(m.options, sconf)
		if err != nil {
			result <- fmt.Errorf("read properties %v fail with error: %v", m.options, err)
			return
		}
		if sconf.Concurrency <= 0 {
			logger.Warnf("invalid type for concurrency property, should be positive integer but found %t", sconf.Concurrency)
			sconf.Concurrency = 1
		}
		m.concurrency = sconf.Concurrency
		if sconf.RetryInterval <= 0 {
			logger.Warnf("invalid type for retryInterval property, should be positive integer but found %t", sconf.RetryInterval)
			sconf.RetryInterval = 1000
		}
		if sconf.RetryCount < 0 {
			logger.Warnf("invalid type for retryCount property, should be positive integer but found %t", sconf.RetryCount)
			sconf.RetryCount = 3
		}
		if sconf.CacheLength < 0 {
			logger.Warnf("invalid type for cacheLength property, should be positive integer but found %t", sconf.CacheLength)
			sconf.CacheLength = 1024
		}
		if sconf.CacheSaveInterval < 0 {
			logger.Warnf("invalid type for cacheSaveInterval property, should be positive integer but found %t", sconf.CacheSaveInterval)
			sconf.CacheSaveInterval = 1000
		}

		tf, err := transform.GenTransform(sconf.DataTemplate)
		if err != nil {
			msg := fmt.Sprintf("property dataTemplate %v is invalid: %v", sconf.DataTemplate, err)
			logger.Warnf(msg)
			result <- fmt.Errorf(msg)
			return
		}
		ctx = context.WithValue(ctx.(*context.DefaultContext), context.TransKey, tf)

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

				stats, err := NewStatManager(ctx, "sink")
				if err != nil {
					m.drainError(result, err, ctx, logger)
					return
				}
				m.mutex.Lock()
				m.statManagers = append(m.statManagers, stats)
				m.mutex.Unlock()

				if conf.Config.Sink.DisableCache {
					for {
						select {
						case data := <-m.input:
							if temp, processed := m.preprocess(data); processed {
								break
							} else {
								data = temp
							}
							stats.SetBufferLength(int64(len(m.input)))
							if sconf.RunAsync {
								go doCollect(ctx, sink, data, stats, sconf, nil)
							} else {
								doCollect(ctx, sink, data, stats, sconf, nil)
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
						cache = NewCheckpointbasedCache(m.input, sconf.CacheLength, m.tch, result, ctx)
					} else {
						cache = NewTimebasedCache(m.input, sconf.CacheLength, sconf.CacheSaveInterval, result, ctx)
					}
					for {
						select {
						case data := <-cache.Out:
							if temp, processed := m.preprocess(data.data); processed {
								break
							} else {
								data.data = temp
							}
							stats.SetBufferLength(int64(len(m.input)))
							if sconf.RunAsync {
								go doCollect(ctx, sink, data, stats, sconf, cache.Complete)
							} else {
								doCollect(ctx, sink, data, stats, sconf, cache.Complete)
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

func doCollect(ctx api.StreamContext, sink api.Sink, item interface{}, stats StatManager, sconf *SinkConf, signalCh chan<- int) {
	stats.IncTotalRecordsIn()
	stats.ProcessTimeStart()
	defer stats.ProcessTimeEnd()
	var outs []map[string]interface{}
	switch val := item.(type) {
	case error:
		outs = []map[string]interface{}{
			{"error": val.Error()},
		}
	case []map[string]interface{}:
		outs = val
	default:
		outs = []map[string]interface{}{
			{"error": fmt.Sprintf("result is not a map slice but found %#v", val)},
		}
	}
	if sconf.Omitempty && (item == nil || len(outs) == 0) {
		ctx.GetLogger().Debugf("receive empty in sink")
		return
	}
	if !sconf.SendSingle {
		doCollectData(ctx, sink, outs, stats, sconf, signalCh)
	} else {
		for _, d := range outs {
			if sconf.Omitempty && (d == nil || len(d) == 0) {
				ctx.GetLogger().Debugf("receive empty in sink")
				continue
			}
			doCollectData(ctx, sink, d, stats, sconf, signalCh)
		}
	}
}

// doCollectData outData must be map or []map
func doCollectData(ctx api.StreamContext, sink api.Sink, outData interface{}, stats StatManager, sconf *SinkConf, signalCh chan<- int) {
	retries := sconf.RetryCount
	for {
		select {
		case <-ctx.Done():
			ctx.GetLogger().Infof("sink node %s instance %d stops data resending", ctx.GetOpId(), ctx.GetInstanceId())
			return
		default:
			if err := sink.Collect(ctx, outData); err != nil {
				stats.IncTotalExceptions()
				ctx.GetLogger().Warnf("sink node %s instance %d publish %s error: %v", ctx.GetOpId(), ctx.GetInstanceId(), outData, err)
				if sconf.RetryInterval > 0 && retries > 0 && strings.HasPrefix(err.Error(), errorx.IOErr) {
					retries--
					time.Sleep(time.Duration(sconf.RetryInterval) * time.Millisecond)
					ctx.GetLogger().Debugf("try again")
				} else {
					return
				}
			} else {
				ctx.GetLogger().Debugf("success")
				stats.IncTotalRecordsOut()
				if signalCh != nil {
					cacheTuple, ok := outData.(*CacheTuple)
					if !ok {
						ctx.GetLogger().Warnf("got none cache tuple %v, should not happen", outData)
					}
					select {
					case signalCh <- cacheTuple.index:
					default:
						ctx.GetLogger().Warnf("sink cache missing response for %d", cacheTuple.index)
					}
				}
				return
			}
		}
	}
}

func getSink(name string, action map[string]interface{}) (api.Sink, error) {
	var (
		s   api.Sink
		err error
	)
	s, err = io.Sink(name)
	if s != nil {
		err = s.Configure(action)
		if err != nil {
			return nil, err
		}
		return s, nil
	} else {
		if err != nil {
			return nil, err
		} else {
			return nil, fmt.Errorf("sink %s not found", name)
		}
	}
}

// AddOutput Override defaultNode
func (m *SinkNode) AddOutput(_ chan<- interface{}, name string) error {
	return fmt.Errorf("fail to add output %s, sink %s cannot add output", name, m.name)
}

// Broadcast Override defaultNode
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

// SaveCache Only called when checkpoint enabled
func (m *SinkNode) SaveCache() {
	m.tch <- struct{}{}
}
