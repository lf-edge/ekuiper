// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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

//go:build ignore_now

package node

import (
	"fmt"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/topo/node/cache"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node/metric"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
)

type SinkNode struct {
	*defaultSinkNode
	// static
	sinkType string
	// configs (also static for sinks)
	options map[string]interface{}
	isMock  bool
	// states varies after restart
	sink api.Sink
}

func NewSinkNode(name string, sinkType string, props map[string]interface{}) *SinkNode {
	return &SinkNode{
		defaultSinkNode: newDefaultSinkNode(name, propsToNodeOption(props)),
		sinkType:        sinkType,
		options:         props,
	}
}

// NewSinkNodeWithSink Only for mock source, do not use it in production
func NewSinkNodeWithSink(name string, sink api.Sink, props map[string]interface{}) *SinkNode {
	return &SinkNode{
		defaultSinkNode: newDefaultSinkNode(name, propsToNodeOption(props)),
		options:         props,
		isMock:          true,
		sink:            sink,
	}
}

func (m *SinkNode) Open(ctx api.StreamContext, result chan<- error) {
	m.ctx = ctx
	logger := ctx.GetLogger()
	logger.Debugf("open sink node %s", m.name)
	go func() {
		err := infra.SafeRun(func() error {
			sconf, err := ParseConf(logger, m.options)
			m.concurrency = sconf.Concurrency
			if err != nil {
				return err
			}
			if err != nil {
				msg := fmt.Sprintf("property dataTemplate %v is invalid: %v", sconf.DataTemplate, err)
				logger.Warnf(msg)
				return fmt.Errorf(msg)
			}

			m.reset()
			logger.Infof("open sink node %d instances with batchSize %d", m.concurrency, sconf.BatchSize)

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
						m.sink = sink
						logger.Debugf("Now is to open sink for rule %s.\n", ctx.GetRuleId())
						if err := sink.Connect(ctx); err != nil {
							return err
						}
						logger.Debugf("Successfully open sink for rule %s.\n", ctx.GetRuleId())
					} else {
						sink = m.sink
					}

					m.statManager = metric.NewStatManager(ctx, "sink")

					// The sink flow is: receive -> batch -> cache -> send.
					// In the outside loop, send received data to batch/cache by dataCh and receive data be dataOutCh
					// Only need to deal with dataOutCh in the outer loop
					dataCh := make(chan []map[string]interface{}, sconf.BufferLength)
					var (
						dataOutCh <-chan []map[string]interface{}
						resendCh  chan []map[string]interface{}

						c  *cache.SyncCache
						rq *cache.SyncCache
					)
					logger.Infof("sink node %s instance %d starts with conf %+v", m.name, instance, *sconf)

					if !sconf.EnableCache {
						dataOutCh = dataCh
					} else {
						c = cache.NewSyncCache(ctx, dataCh, result, &sconf.SinkConf, sconf.BufferLength)
						if sconf.ResendAlterQueue {
							resendCh = make(chan []map[string]interface{}, sconf.BufferLength)
							rq = cache.NewSyncCache(ctx, resendCh, result, &sconf.SinkConf, sconf.BufferLength)
						}
						dataOutCh = c.Out
					}

					receiveQ := func(data interface{}) {
						processed := false
						if data, processed = m.preprocess(data); processed {
							return
						}
						m.statManager.IncTotalRecordsIn()
						m.statManager.SetBufferLength(bufferLen(dataCh, dataOutCh, c, rq))
						outs := itemToMap(data)
						if sconf.Omitempty && (data == nil || len(outs) == 0) {
							ctx.GetLogger().Debugf("receive empty in sink")
							return
						}
						select {
						case dataCh <- outs:
						default:
							ctx.GetLogger().Warnf("sink node %s instance %d buffer is full, drop data %v", m.name, instance, outs)
						}
						if resendCh != nil {
							select {
							case resendCh <- nil:
								ctx.GetLogger().Debugf("resend signal sent")
							case <-ctx.Done():
							}
						}
					}
					normalQ := func(data []map[string]interface{}) {
						m.statManager.ProcessTimeStart()
						m.statManager.SetBufferLength(bufferLen(dataCh, dataOutCh, c, rq))
						ctx.GetLogger().Debugf("sending data: %v", data)
						err := doCollectMaps(ctx, sink, sconf, data, m.statManager, false)
						if sconf.EnableCache {
							ack := checkAck(ctx, data, err)
							if sconf.ResendAlterQueue {
								// If ack is false, add it to the resend queue
								if !ack {
									select {
									case resendCh <- data:
									case <-ctx.Done():
									}
								}
								// Always ack for the normal queue as fail items are handled by the resend queue
								select {
								case c.Ack <- true:
									m.statManager.SetBufferLength(bufferLen(dataCh, dataOutCh, c, rq) - 1)
								case <-ctx.Done():
								}
							} else {
								select {
								case c.Ack <- ack:
									if ack { // -1 because the signal length is changed async, just calculate it here
										m.statManager.SetBufferLength(bufferLen(dataCh, dataOutCh, c, rq) - 1)
									}
								case <-ctx.Done():
								}
							}
						}
						m.statManager.ProcessTimeEnd()
					}

					resendQ := func(data []map[string]interface{}) {
						ctx.GetLogger().Debugf("resend data: %v", data)
						m.statManager.SetBufferLength(bufferLen(dataCh, dataOutCh, c, rq))
						if sconf.ResendIndicatorField != "" {
							for _, item := range data {
								item[sconf.ResendIndicatorField] = true
							}
						}
						err := doCollectMaps(ctx, sink, sconf, data, m.statManager, true)
						ack := checkAck(ctx, data, err)
						select {
						case rq.Ack <- ack:
							if ack {
								m.statManager.SetBufferLength(bufferLen(dataCh, dataOutCh, c, rq) - 1)
							}
						case <-ctx.Done():
						}
					}

					doneQ := func() {
						logger.Infof("sink node %s instance %d done", m.name, instance)
						if err := sink.Close(ctx); err != nil {
							logger.Warnf("close sink node %s instance %d fails: %v", m.name, instance, err)
						}
					}

					if resendCh == nil { // no resend strategy
						for {
							select {
							case data := <-m.input:
								receiveQ(data)
							case data := <-dataOutCh:
								normalQ(data)
							case <-ctx.Done():
								doneQ()
								return nil
							}
						}
					} else {
						if sconf.ResendPriority == 0 {
							for {
								select {
								case data := <-m.input:
									receiveQ(data)
								case data := <-dataOutCh:
									normalQ(data)
								case data := <-rq.Out:
									resendQ(data)
								case <-ctx.Done():
									doneQ()
									return nil
								}
							}
						} else if sconf.ResendPriority < 0 { // normal queue has higher priority
							for {
								select {
								case data := <-m.input:
									receiveQ(data)
								case data := <-dataOutCh:
									normalQ(data)
								default:
									select {
									case data := <-m.input:
										receiveQ(data)
									case data := <-dataOutCh:
										normalQ(data)
									case data := <-rq.Out:
										resendQ(data)
									case <-ctx.Done():
										doneQ()
										return nil
									}
								}
							}
						} else {
							for {
								select {
								case data := <-m.input:
									receiveQ(data)
								case data := <-rq.Out:
									resendQ(data)
								default:
									select {
									case data := <-m.input:
										receiveQ(data)
									case data := <-dataOutCh:
										normalQ(data)
									case data := <-rq.Out:
										resendQ(data)
									case <-ctx.Done():
										doneQ()
										return nil
									}
								}
							}
						}
					}
				})
				if panicOrError != nil {
					infra.DrainError(ctx, panicOrError, result)
				}
			}(0)

			return nil
		})
		if err != nil {
			infra.DrainError(ctx, err, result)
		}
	}()
}

func bufferLen(dataCh chan []map[string]interface{}, dataOutCh <-chan []map[string]interface{}, c *cache.SyncCache, rq *cache.SyncCache) int64 {
	l := len(dataCh)
	if dataCh != dataOutCh {
		l += len(dataOutCh)
	}
	if c != nil {
		l += c.CacheLength
	}
	if rq != nil {
		l += rq.CacheLength
	}
	return int64(l)
}

func checkAck(ctx api.StreamContext, data interface{}, err error) bool {
	if err != nil {
		if errorx.IsIOError(err) { // do not log to prevent a lot of logs!
			return false
		} else {
			ctx.GetLogger().Warnf("sink node %s instance %d publish %s error: %v", ctx.GetOpId(), ctx.GetInstanceId(), data, err)
		}
	} else {
		ctx.GetLogger().Debugf("sent data: %v", data)
	}
	return true
}

func (m *SinkNode) reset() {
	if !m.isMock {
		m.sink = nil
	}
	m.statManager = nil
}

func doCollectMaps(ctx api.StreamContext, sink api.Sink, sconf *SinkConf, outs []map[string]interface{}, stats metric.StatManager, isResend bool) error {
	if !sconf.SendSingle {
		return doCollectData(ctx, sink, outs, stats, isResend)
	} else {
		var err error
		for _, d := range outs {
			if sconf.Omitempty && (d == nil || len(d) == 0) {
				ctx.GetLogger().Debugf("receive empty in sink")
				continue
			}
			newErr := doCollectData(ctx, sink, d, stats, isResend)
			if newErr != nil {
				err = newErr
			}
		}
		return err
	}
}

// doCollectData outData must be map or []map
func doCollectData(ctx api.StreamContext, sink api.Sink, outData interface{}, stats metric.StatManager, isResend bool) error {
	select {
	case <-ctx.Done():
		ctx.GetLogger().Infof("sink node %s instance %d stops data resending", ctx.GetOpId(), ctx.GetInstanceId())
		return nil
	default:
		if isResend {
			return resendDataToSink(ctx, sink, outData, stats)
		} else {
			return sendDataToSink(ctx, sink, outData, stats)
		}
	}
}

func sendDataToSink(ctx api.StreamContext, sink api.Sink, outData interface{}, stats metric.StatManager) error {
	if err := sink.Collect(ctx, outData); err != nil {
		stats.IncTotalExceptions(err.Error())
		return err
	} else {
		ctx.GetLogger().Debugf("success")
		stats.IncTotalRecordsOut()
		switch outs := outData.(type) {
		case []map[string]interface{}:
			stats.IncTotalMessagesProcessed(int64(len(outs)))
		default:
			stats.IncTotalMessagesProcessed(1)
		}
		return nil
	}
}

func resendDataToSink(ctx api.StreamContext, sink api.Sink, outData interface{}, stats metric.StatManager) error {
	ctx.GetLogger().Debugf("start resend")
	var err error
	switch st := sink.(type) {
	case api.ResendSink:
		err = st.CollectResend(ctx, outData)
	default:
		err = st.Collect(ctx, outData)
	}
	if err != nil {
		stats.IncTotalExceptions(err.Error())
		return err
	} else {
		ctx.GetLogger().Debugf("success resend")
		stats.IncTotalRecordsOut()
		switch outs := outData.(type) {
		case []map[string]interface{}:
			stats.IncTotalMessagesProcessed(int64(len(outs)))
		default:
			stats.IncTotalMessagesProcessed(1)
		}
		return nil
	}
}

// AddOutput Override defaultNode
func (m *SinkNode) AddOutput(_ chan<- interface{}, name string) error {
	return fmt.Errorf("fail to add output %s, sink %s cannot add output", name, m.name)
}

// Broadcast Override defaultNode
func (m *SinkNode) Broadcast(_ interface{}) {
	// do nothing, may be called by checkpoint
}
