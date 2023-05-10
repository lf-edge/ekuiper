// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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
	"strings"
	"sync"

	"github.com/lf-edge/ekuiper/internal/binder/io"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/node/cache"
	nodeConf "github.com/lf-edge/ekuiper/internal/topo/node/conf"
	"github.com/lf-edge/ekuiper/internal/topo/node/metric"
	"github.com/lf-edge/ekuiper/internal/topo/transform"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/errorx"
	"github.com/lf-edge/ekuiper/pkg/infra"
	"github.com/lf-edge/ekuiper/pkg/message"
)

type SinkConf struct {
	Concurrency  int      `json:"concurrency"`
	RunAsync     bool     `json:"runAsync"` // deprecated, will remove in the next release
	Omitempty    bool     `json:"omitIfEmpty"`
	SendSingle   bool     `json:"sendSingle"`
	DataTemplate string   `json:"dataTemplate"`
	Format       string   `json:"format"`
	SchemaId     string   `json:"schemaId"`
	Delimiter    string   `json:"delimiter"`
	BufferLength int      `json:"bufferLength"`
	Fields       []string `json:"fields"`
	conf.SinkConf
}

type SinkNode struct {
	*defaultSinkNode
	// static
	sinkType string
	mutex    sync.RWMutex
	// configs (also static for sinks)
	options map[string]interface{}
	isMock  bool
	// states varies after restart
	sinks []api.Sink
}

func NewSinkNode(name string, sinkType string, props map[string]interface{}) *SinkNode {
	bufferLength := 1024
	if c, ok := props["bufferLength"]; ok {
		if t, err := cast.ToInt(c, cast.STRICT); err != nil || t <= 0 {
			// invalid property bufferLength
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
	go func() {
		err := infra.SafeRun(func() error {
			sconf, err := m.parseConf(logger)
			if err != nil {
				return err
			}

			tf, err := transform.GenTransform(sconf.DataTemplate, sconf.Format, sconf.SchemaId, sconf.Delimiter, sconf.Fields)
			if err != nil {
				msg := fmt.Sprintf("property dataTemplate %v is invalid: %v", sconf.DataTemplate, err)
				logger.Warnf(msg)
				return fmt.Errorf(msg)
			}
			ctx = context.WithValue(ctx.(*context.DefaultContext), context.TransKey, tf)

			m.reset()
			logger.Infof("open sink node %d instances", m.concurrency)
			for i := 0; i < m.concurrency; i++ { // workers
				go func(instance int) {
					panicOrError := infra.SafeRun(func() error {
						var (
							sink api.Sink
							err  error
						)
						if !m.isMock {
							logger.Debugf("Trying to get sink for rule %s with options %v\n", ctx.GetRuleId(), m.options)
							sink, err = getSink(m.sinkType, m.options)
							if err != nil {
								return err
							}
							logger.Debugf("Successfully get the sink %s", m.sinkType)
							m.mutex.Lock()
							m.sinks = append(m.sinks, sink)
							m.mutex.Unlock()
							logger.Debugf("Now is to open sink for rule %s.\n", ctx.GetRuleId())
							if err := sink.Open(ctx); err != nil {
								return err
							}
							logger.Debugf("Successfully open sink for rule %s.\n", ctx.GetRuleId())
						} else {
							sink = m.sinks[instance]
						}

						stats, err := metric.NewStatManager(ctx, "sink")
						if err != nil {
							return err
						}
						m.mutex.Lock()
						m.statManagers = append(m.statManagers, stats)
						m.mutex.Unlock()

						if !sconf.EnableCache {
							for {
								select {
								case data := <-m.input:
									if temp, processed := m.preprocess(data); !processed {
										data = temp
									} else {
										break
									}
									stats.SetBufferLength(int64(len(m.input)))
									stats.IncTotalRecordsIn()
									if sconf.RunAsync {
										conf.Log.Warnf("RunAsync is deprecated and ignored.")
									}
									err := doCollect(ctx, sink, data, stats, sconf)
									if err != nil {
										logger.Warnf("sink collect error: %v", err)
									}
								case <-ctx.Done():
									logger.Infof("sink node %s instance %d done", m.name, instance)
									if err := sink.Close(ctx); err != nil {
										logger.Warnf("close sink node %s instance %d fails: %v", m.name, instance, err)
									}
									return nil
								}
							}
						} else {
							logger.Infof("Creating sink cache")
							if sconf.RunAsync { // async mode, the ack must have an id
								// is not supported and validated in the configure, should not go here
								return fmt.Errorf("async mode is not supported for cache sink")
							} else { // sync mode, the ack is already in order
								dataCh := make(chan []map[string]interface{}, sconf.BufferLength)
								c := cache.NewSyncCache(ctx, dataCh, result, stats, &sconf.SinkConf, sconf.BufferLength)
								for {
									select {
									case data := <-m.input:
										if temp, processed := m.preprocess(data); !processed {
											data = temp
										} else {
											break
										}
										stats.IncTotalRecordsIn()
										outs := itemToMap(data)
										if sconf.Omitempty && (data == nil || len(outs) == 0) {
											ctx.GetLogger().Debugf("receive empty in sink")
											return nil
										}
										select {
										case dataCh <- outs:
										case <-ctx.Done():
										}
									case data := <-c.Out:
										stats.ProcessTimeStart()
										ack := true
										err := doCollectMaps(ctx, sink, sconf, data, stats)
										// Only recoverable error should be cached
										if err != nil {
											if strings.HasPrefix(err.Error(), errorx.IOErr) { // do not log to prevent a lot of logs!
												ack = false
											} else {
												ctx.GetLogger().Warnf("sink node %s instance %d publish %s error: %v", ctx.GetOpId(), ctx.GetInstanceId(), data, err)
											}
										} else {
											ctx.GetLogger().Debugf("sent data to MQTT: %v", data)
										}
										select {
										case c.Ack <- ack:
										case <-ctx.Done():
										}
										stats.ProcessTimeEnd()
									case <-ctx.Done():
										logger.Infof("sink node %s instance %d done", m.name, instance)
										if err := sink.Close(ctx); err != nil {
											logger.Warnf("close sink node %s instance %d fails: %v", m.name, instance, err)
										}
										return nil
									}
								}
							}

						}
					})
					if panicOrError != nil {
						infra.DrainError(ctx, panicOrError, result)
					}
				}(i)
			}
			return nil
		})
		if err != nil {
			infra.DrainError(ctx, err, result)
		}
	}()
}

func (m *SinkNode) parseConf(logger api.Logger) (*SinkConf, error) {
	sconf := &SinkConf{
		Concurrency:  1,
		RunAsync:     false,
		Omitempty:    false,
		SendSingle:   false,
		DataTemplate: "",
		SinkConf:     *conf.Config.Sink,
		BufferLength: 1024,
	}
	err := cast.MapToStruct(m.options, sconf)
	if err != nil {
		return nil, fmt.Errorf("read properties %v fail with error: %v", m.options, err)
	}
	if sconf.Concurrency <= 0 {
		logger.Warnf("invalid type for concurrency property, should be positive integer but found %t", sconf.Concurrency)
		sconf.Concurrency = 1
	}
	m.concurrency = sconf.Concurrency
	if sconf.Format == "" {
		sconf.Format = "json"
	} else if sconf.Format != message.FormatJson && sconf.Format != message.FormatProtobuf && sconf.Format != message.FormatBinary && sconf.Format != message.FormatCustom && sconf.Format != message.FormatDelimited {
		logger.Warnf("invalid type for format property, should be json protobuf or binary but found %s", sconf.Format)
		sconf.Format = "json"
	}
	err = cast.MapToStruct(m.options, &sconf.SinkConf)
	if err != nil {
		return nil, fmt.Errorf("read properties %v to cache conf fail with error: %v", m.options, err)
	}
	if sconf.SinkConf.EnableCache && sconf.RunAsync {
		conf.Log.Warnf("RunAsync is deprecated and ignored.")
		return nil, fmt.Errorf("cache is not supported for async sink, do not use enableCache and runAsync properties together")
	}
	err = sconf.SinkConf.Validate()
	if err != nil {
		return nil, fmt.Errorf("invalid cache properties: %v", err)
	}
	return sconf, err
}

func (m *SinkNode) reset() {
	if !m.isMock {
		m.sinks = nil
	}
	m.statManagers = nil
}

func doCollect(ctx api.StreamContext, sink api.Sink, item interface{}, stats metric.StatManager, sconf *SinkConf) error {
	stats.ProcessTimeStart()
	defer stats.ProcessTimeEnd()
	outs := itemToMap(item)
	if sconf.Omitempty && (item == nil || len(outs) == 0) {
		ctx.GetLogger().Debugf("receive empty in sink")
		return nil
	}
	return doCollectMaps(ctx, sink, sconf, outs, stats)
}

func doCollectMaps(ctx api.StreamContext, sink api.Sink, sconf *SinkConf, outs []map[string]interface{}, stats metric.StatManager) error {
	if !sconf.SendSingle {
		return doCollectData(ctx, sink, outs, stats)
	} else {
		var err error
		for _, d := range outs {
			if sconf.Omitempty && (d == nil || len(d) == 0) {
				ctx.GetLogger().Debugf("receive empty in sink")
				continue
			}
			newErr := doCollectData(ctx, sink, d, stats)
			if newErr != nil {
				err = newErr
			}
		}
		return err
	}
}

func itemToMap(item interface{}) []map[string]interface{} {
	var outs []map[string]interface{}
	switch val := item.(type) {
	case error:
		outs = []map[string]interface{}{
			{"error": val.Error()},
		}
		break
	case xsql.Collection: // The order is important here, because some element is both a collection and a row, such as WindowTuples, JoinTuples, etc.
		outs = val.ToMaps()
		break
	case xsql.Row:
		outs = []map[string]interface{}{
			val.ToMap(),
		}
		break
	case []map[string]interface{}: // for test only
		outs = val
		break
	default:
		outs = []map[string]interface{}{
			{"error": fmt.Sprintf("result is not a map slice but found %#v", val)},
		}
	}
	return outs
}

// doCollectData outData must be map or []map
func doCollectData(ctx api.StreamContext, sink api.Sink, outData interface{}, stats metric.StatManager) error {
	select {
	case <-ctx.Done():
		ctx.GetLogger().Infof("sink node %s instance %d stops data resending", ctx.GetOpId(), ctx.GetInstanceId())
		return nil
	default:
		if err := sink.Collect(ctx, outData); err != nil {
			stats.IncTotalExceptions(err.Error())
			return err
		} else {
			ctx.GetLogger().Debugf("success")
			stats.IncTotalRecordsOut()
			return nil
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
		newAction := nodeConf.GetSinkConf(name, action)
		err = s.Configure(newAction)
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
