// Copyright 2024 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/internal/topo/node/metric"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/infra"
	"github.com/lf-edge/ekuiper/pkg/stat"
)

// SourceConnectorNode is a node that connects to an external source
// The SourceNode is an all-in-one source node that support connect and decode and more.
// The SourceConnectorNode is a node that only connects to external source and does not decode.
type SourceConnectorNode struct {
	*defaultNode

	s       api.SourceConnector
	buffLen int
}

// NewSourceConnectorNode creates a SourceConnectorNode
func NewSourceConnectorNode(name string, ss api.SourceConnector, dataSource string, props map[string]any, rOpt *api.RuleOption) (*SourceConnectorNode, error) {
	m := &SourceConnectorNode{
		defaultNode: newDefaultNode(name, rOpt),
		s:           ss,
		buffLen:     rOpt.BufferLength,
	}
	return m, m.setup(dataSource, props)
}

// Setup read configuration and validate and initialize the sourceConnector
func (m *SourceConnectorNode) setup(dataSource string, props map[string]any) error {
	// Initialize sourceConnector
	err := m.s.Configure(dataSource, props)
	if err != nil {
		return err
	}
	return nil
}

// TODO manage connection, use connection entity later
// Connection must be able to retry. There is another metrics to record the connection status.(connected, retry count, connect time, disconnect time)
// connect and auto reconnect

// Open will be invoked by topo. It starts reading data.
func (m *SourceConnectorNode) Open(ctx api.StreamContext, ctrlCh chan<- error) {
	m.ctx = ctx
	ctx.GetLogger().Infof("Opening source connector %s", m.name)
	// create stat manager
	m.statManager = metric.NewStatManager(ctx, "source")
	if able, ok := m.s.(stat.StatsAble); ok {
		able.SetupStats(m.statManager)
	}
	go m.Run(ctx, ctrlCh)
}

func (m *SourceConnectorNode) Run(ctx api.StreamContext, ctrlCh chan<- error) {
	defer m.s.Close(ctx)
	poe := infra.SafeRun(func() error {
		err := m.s.Connect(ctx)
		if err != nil {
			return err
		}
		// subscribe and send data through channel
		// Align to old code, use a channel to send data
		buffer := make(chan api.SourceTuple, m.buffLen)
		go m.s.Open(ctx, buffer, ctrlCh)
		err = m.s.Subscribe(ctx)
		if err != nil {
			return err
		}
		for {
			m.statManager.SetBufferLength(int64(len(buffer)))
			select {
			case <-ctx.Done():
				ctx.GetLogger().Infof("source connector %s is finished", m.name)
				return nil
			case vu8 := <-buffer:
				ctx.GetLogger().Debugf("source connector %s receive data %+v", m.name, vu8)
				if e, ok := vu8.(*xsql.ErrorSourceTuple); ok {
					m.statManager.IncTotalExceptions(e.Error.Error())
					break
				}
				m.statManager.ProcessTimeStart()
				m.statManager.IncTotalRecordsIn()
				if raw, ok := vu8.(api.RawTuple); ok && raw.Raw() != nil {
					tuple := &xsql.Tuple{Emitter: m.name, Raw: raw.Raw(), Timestamp: vu8.Timestamp().UnixMilli(), Metadata: vu8.Meta()}
					m.Broadcast(tuple)
					m.statManager.IncTotalRecordsOut()
				} else {
					err := fmt.Errorf("expect api.RawTuple but got %T", vu8)
					m.Broadcast(&xsql.ErrorSourceTuple{
						Error: err,
					})
					m.statManager.IncTotalExceptions(err.Error())
				}
				m.statManager.IncTotalMessagesProcessed(1)
				m.statManager.ProcessTimeEnd()
			}
		}
	})
	if poe != nil {
		infra.DrainError(ctx, poe, ctrlCh)
	}
}
