// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/internal"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/converter"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/node/metric"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/infra"
	"strings"
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
	preprocessOp UnOperation
}

func NewSourceNode(name string, st ast.StreamType, op UnOperation, options *ast.Options, sendError bool) *SourceNode {
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
			sendError:   sendError,
		},
		preprocessOp: op,
		options:      options,
	}
}

func (m *SourceNode) Open(ctx api.StreamContext, errCh chan<- error) {
	m.ctx = ctx
	logger := ctx.GetLogger()
	logger.Infof("open source node %s with option %v", m.name, m.options)
	go func() {
		panicOrError := infra.SafeRun(func() error {
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
			format := fmt.Sprintf("%v", props["format"])
			schemaFile := ""
			schemaId := m.options.SCHEMAID
			if schemaId != "" {
				r := strings.Split(schemaId, ".")
				if len(r) != 2 {
					return fmt.Errorf("invalid schemaId: %s", schemaId)
				}
				schemaFile = r[0]
			}
			converter, err := converter.GetOrCreateConverter(format, schemaFile, schemaId)
			if err != nil {
				msg := fmt.Sprintf("cannot get converter from format %s, schemaId %s: %v", format, schemaId, err)
				logger.Warnf(msg)
				return fmt.Errorf(msg)
			}
			ctx = context.WithValue(ctx.(*context.DefaultContext), internal.DecodeKey, converter)
			m.reset()
			logger.Infof("open source node with props %v, concurrency: %d, bufferLength: %d", conf.Printable(m.props), m.concurrency, m.bufferLength)
			for i := 0; i < m.concurrency; i++ { // workers
				go func(instance int) {
					poe := infra.SafeRun(func() error {
						//Do open source instances
						var (
							si     *sourceInstance
							buffer *DynamicChannelBuffer
							err    error
						)

						si, err = getSourceInstance(m, instance)
						if err != nil {
							return err
						}
						m.mutex.Lock()
						m.sources = append(m.sources, si.source)
						m.mutex.Unlock()
						buffer = si.dataCh

						defer func() {
							logger.Infof("source %s done", m.name)
							m.close()
							buffer.Close()
						}()

						stats, err := metric.NewStatManager(ctx, "source")
						if err != nil {
							return err
						}
						m.mutex.Lock()
						m.statManagers = append(m.statManagers, stats)
						m.mutex.Unlock()
						logger.Infof("Start source %s instance %d successfully", m.name, instance)
						for {
							select {
							case <-ctx.Done():
								return nil
							case err := <-si.errorCh:
								return err
							case data := <-buffer.Out:
								stats.IncTotalRecordsIn()
								stats.ProcessTimeStart()
								tuple := &xsql.Tuple{Emitter: m.name, Message: data.Message(), Timestamp: conf.GetNowInMilli(), Metadata: data.Meta()}
								processedData := m.preprocessOp.Apply(ctx, tuple, nil, nil)
								stats.ProcessTimeEnd()
								//blocking
								switch val := processedData.(type) {
								case nil:
									continue
								case error:
									logger.Errorf("Source %s preprocess error: %s", ctx.GetOpId(), val)
									m.Broadcast(val)
									stats.IncTotalExceptions()
								default:
									m.Broadcast(val)
								}
								stats.IncTotalRecordsOut()
								stats.SetBufferLength(int64(buffer.GetLength()))
								if rw, ok := si.source.(api.Rewindable); ok {
									if offset, err := rw.GetOffset(); err != nil {
										infra.DrainError(ctx, err, errCh)
									} else {
										err = ctx.PutState(internal.OffsetKey, offset)
										if err != nil {
											return err
										}
										logger.Debugf("Source save offset %v", offset)
									}
								}
							}
						}
					})
					if poe != nil {
						infra.DrainError(ctx, poe, errCh)
					}
				}(i)
			}
			return nil
		})
		if panicOrError != nil {
			infra.DrainError(ctx, panicOrError, errCh)
		}
	}()
}

func (m *SourceNode) reset() {
	m.statManagers = nil
}

func (m *SourceNode) close() {
	if m.options.SHARED {
		removeSourceInstance(m)
	}
}
