// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/converter"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	nodeConf "github.com/lf-edge/ekuiper/internal/topo/node/conf"
	"github.com/lf-edge/ekuiper/internal/topo/node/metric"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/infra"
)

type SourceNode struct {
	*defaultNode
	streamType   ast.StreamType
	sourceType   string
	options      *ast.Options
	bufferLength int
	props        map[string]interface{}
	preprocessOp UnOperation
	schema       map[string]*ast.JsonStreamField
	IsWildcard   bool
	IsSchemaless bool
	si           *sourceInstance
}

func NewSourceNode(name string, st ast.StreamType, op UnOperation, options *ast.Options, rOptions *api.RuleOption, isWildcard, isSchemaless bool, schema map[string]*ast.JsonStreamField) *SourceNode {
	t := options.TYPE
	if t == "" {
		if st == ast.TypeStream {
			t = "mqtt"
		} else if st == ast.TypeTable {
			t = "file"
		}
	}
	return &SourceNode{
		streamType:   st,
		sourceType:   t,
		defaultNode:  newDefaultNode(name, rOptions),
		preprocessOp: op,
		options:      options,
		schema:       schema,
		IsWildcard:   isWildcard,
		IsSchemaless: isSchemaless,
	}
}

func (m *SourceNode) SetProps(props map[string]interface{}) {
	m.props = props
}

const OffsetKey = "$$offset"

func (m *SourceNode) Open(ctx api.StreamContext, errCh chan<- error) {
	m.ctx = ctx
	logger := ctx.GetLogger()
	logger.Infof("open source node %s with option %v", m.name, m.options)
	go func() {
		panicOrError := infra.SafeRun(func() error {
			props := nodeConf.GetSourceConf(m.sourceType, m.options)
			// merge the props
			for k, v := range m.props {
				props[k] = v
			}
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
			if m.streamType == ast.TypeTable {
				props["isTable"] = true
			}
			props["delimiter"] = m.options.DELIMITER
			m.options.Schema = nil
			m.options.IsWildCard = m.IsWildcard
			m.options.IsSchemaLess = m.IsSchemaless
			if m.schema != nil {
				m.options.RuleID = ctx.GetRuleId()
				m.options.Schema = m.schema
				m.options.StreamName = m.name
			}
			converterTool, err := converter.GetOrCreateConverter(m.options)
			if err != nil {
				msg := fmt.Sprintf("cannot get converter from format %s, schemaId %s: %v", m.options.FORMAT, m.options.SCHEMAID, err)
				logger.Warnf(msg)
				return fmt.Errorf(msg)
			}
			ctx = context.WithValue(ctx.(*context.DefaultContext), context.DecodeKey, converterTool)
			m.reset()
			logger.Infof("open source node with props %v, concurrency: %d, bufferLength: %d", conf.Printable(m.props), m.concurrency, m.bufferLength)

			go func(instance int) {
				poe := infra.SafeRun(func() error {
					// Do open source instances
					var (
						si     *sourceInstance
						buffer *DynamicChannelBuffer
						err    error
					)

					m.statManager = metric.NewStatManager(ctx, "source")

					si, err = getSourceInstance(m, instance)
					if err != nil {
						return err
					}
					buffer = si.dataCh
					m.si = si

					defer func() {
						logger.Infof("source %s done", m.name)
						m.close()
						buffer.Close()
					}()
					logger.Infof("Start source %s instance %d successfully", m.name, instance)
					for {
						select {
						case <-ctx.Done():
							// We should clear the schema after we close the topo in order to avoid the following problem:
							// 1. stop the rule
							// 2. change the schema
							// 3. restart the rule
							// As the schema has changed, it will be error if we hold the old schema here
							// TODO: fetch the latest stream schema after we open the topo
							m.schema = nil
							return nil
						case err := <-si.errorCh:
							return err
						case data := <-buffer.Out:
							if t, ok := data.(*xsql.ErrorSourceTuple); ok {
								logger.Errorf("Source %s error: %v", ctx.GetOpId(), t.Error)
								m.statManager.IncTotalExceptions(t.Error.Error())
								continue
							}
							m.statManager.IncTotalRecordsIn()
							rcvTime := conf.GetNow()
							if !data.Timestamp().IsZero() {
								rcvTime = data.Timestamp()
							}
							m.statManager.SetProcessTimeStart(rcvTime)
							tuple := &xsql.Tuple{Emitter: m.name, Message: data.Message(), Timestamp: rcvTime.UnixMilli(), Metadata: data.Meta()}
							var processedData interface{}
							if m.preprocessOp != nil {
								processedData = m.preprocessOp.Apply(ctx, tuple, nil, nil)
							} else {
								processedData = tuple
							}
							m.statManager.ProcessTimeEnd()
							// blocking
							switch val := processedData.(type) {
							case nil:
								continue
							case error:
								logger.Errorf("Source %s preprocess error: %s", ctx.GetOpId(), val)
								m.Broadcast(val)
								m.statManager.IncTotalExceptions(val.Error())
							default:
								m.Broadcast(val)
								m.statManager.IncTotalRecordsOut()
								m.statManager.IncTotalMessagesProcessed(1)
							}
							m.statManager.SetBufferLength(int64(buffer.GetLength()))
							if rw, ok := si.source.(api.Rewindable); ok {
								if offset, err := rw.GetOffset(); err != nil {
									infra.DrainError(ctx, err, errCh)
								} else {
									err = ctx.PutState(OffsetKey, offset)
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
			}(0)
			return nil
		})
		if panicOrError != nil {
			infra.DrainError(ctx, panicOrError, errCh)
		}
	}()
}

func (m *SourceNode) reset() {
	m.statManager = nil
}

func (m *SourceNode) GetSource() api.Source {
	return m.si.source
}

func (m *SourceNode) close() {
	if m.options.SHARED {
		removeSourceInstance(m)
	}
}
