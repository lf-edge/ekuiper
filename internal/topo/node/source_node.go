// Copyright 2021 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/plugin"
	"github.com/lf-edge/ekuiper/internal/topo/source"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"sync"
)

type SourceNode struct {
	*defaultNode
	streamType   ast.StreamType
	sourceType   string
	options      *ast.Options
	bufferLength int
	props        map[string]interface{}
	mutex        sync.RWMutex
	sources      []api.Source
}

func NewSourceNode(name string, st ast.StreamType, options *ast.Options) *SourceNode {
	t := options.TYPE
	if t == "" {
		if st == ast.TypeStream {
			t = "mqtt"
		} else if st == ast.TypeTable {
			t = "file"
		}
	}
	return &SourceNode{
		streamType: st,
		sourceType: t,
		defaultNode: &defaultNode{
			name:        name,
			outputs:     make(map[string]chan<- interface{}),
			concurrency: 1,
		},
		options: options,
	}
}

const OffsetKey = "$$offset"

func (m *SourceNode) Open(ctx api.StreamContext, errCh chan<- error) {
	m.ctx = ctx
	logger := ctx.GetLogger()
	logger.Infof("open source node %s with option %v", m.name, m.options)
	go func() {
		props := getSourceConf(ctx, m.sourceType, m.options)
		m.props = props
		if c, ok := props["concurrency"]; ok {
			if t, err := cast.ToInt(c, cast.STRICT); err != nil || t <= 0 {
				logger.Warnf("invalid type for concurrency property, should be positive integer but found %t", c)
			} else {
				m.concurrency = t
			}
		}
		bl := 102400
		if c, ok := props["bufferLength"]; ok {
			if t, err := cast.ToInt(c, cast.STRICT); err != nil || t <= 0 {
				logger.Warnf("invalid type for bufferLength property, should be positive integer but found %t", c)
			} else {
				bl = t
			}
		}
		m.bufferLength = bl
		// Set retain size for table type
		if m.options.RETAIN_SIZE > 0 && m.streamType == ast.TypeTable {
			props["$retainSize"] = m.options.RETAIN_SIZE
		}
		m.reset()
		logger.Infof("open source node %d instances", m.concurrency)
		for i := 0; i < m.concurrency; i++ { // workers
			go func(instance int) {
				//Do open source instances
				var (
					si     *sourceInstance
					buffer *DynamicChannelBuffer
					err    error
				)

				si, err = getSourceInstance(m, instance)
				if err != nil {
					m.drainError(errCh, err, ctx, logger)
					return
				}
				m.mutex.Lock()
				m.sources = append(m.sources, si.source)
				m.mutex.Unlock()
				buffer = si.dataCh

				defer func() {
					logger.Infof("source %s done", m.name)
					m.close(ctx, logger)
					buffer.Close()
				}()

				stats, err := NewStatManager("source", ctx)
				if err != nil {
					m.drainError(errCh, err, ctx, logger)
					return
				}
				m.mutex.Lock()
				m.statManagers = append(m.statManagers, stats)
				m.mutex.Unlock()
				logger.Infof("Start source %s instance %d successfully", m.name, instance)
				for {
					select {
					case <-ctx.Done():
						return
					case err := <-si.errorCh:
						m.drainError(errCh, err, ctx, logger)
						return
					case data := <-buffer.Out:
						stats.IncTotalRecordsIn()
						stats.ProcessTimeStart()
						tuple := &xsql.Tuple{Emitter: m.name, Message: data.Message(), Timestamp: conf.GetNowInMilli(), Metadata: data.Meta()}
						stats.ProcessTimeEnd()
						logger.Debugf("source node %s is sending tuple %+v of timestamp %d", m.name, tuple, tuple.Timestamp)
						//blocking
						m.Broadcast(tuple)
						stats.IncTotalRecordsOut()
						stats.SetBufferLength(int64(buffer.GetLength()))
						if rw, ok := si.source.(api.Rewindable); ok {
							if offset, err := rw.GetOffset(); err != nil {
								m.drainError(errCh, err, ctx, logger)
							} else {
								err = ctx.PutState(OffsetKey, offset)
								if err != nil {
									m.drainError(errCh, err, ctx, logger)
								}
								logger.Debugf("Source save offset %v", offset)
							}
						}
						logger.Debugf("source node %s has consumed tuple of timestamp %d", m.name, tuple.Timestamp)
					}
				}
			}(i)
		}
	}()
}

func (m *SourceNode) reset() {
	m.statManagers = nil
}

func doGetSource(t string) (api.Source, error) {
	var (
		s   api.Source
		err error
	)
	switch t {
	case "mqtt":
		s = &source.MQTTSource{}
	case "httppull":
		s = &source.HTTPPullSource{}
	case "file":
		s = &source.FileSource{}
	default:
		s, err = plugin.GetSource(t)
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (m *SourceNode) drainError(errCh chan<- error, err error, ctx api.StreamContext, logger api.Logger) {
	select {
	case errCh <- err:
		logger.Debugf("sent error: %v", err)
	case <-ctx.Done():
	}
	return
}

func (m *SourceNode) close(ctx api.StreamContext, logger api.Logger) {
	if !m.options.SHARED {
		for _, s := range m.sources {
			if err := s.Close(ctx); err != nil {
				logger.Warnf("close source fails: %v", err)
			}
		}
	} else {
		removeSourceInstance(m)
	}
}
