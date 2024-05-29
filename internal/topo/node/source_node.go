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
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

// SourceNode is a node that connects to an external source
// The SourceNode is an all-in-one source node that support connect and decode and more.
// The SourceConnectorNode is a node that only connects to external source and does not decode.
type SourceNode struct {
	*defaultNode

	s        api.Source
	interval time.Duration
}

type c struct {
	Interval time.Duration `json:"interval"`
}

// NewSourceNode creates a SourceConnectorNode
func NewSourceNode(ctx api.StreamContext, name string, ss api.Source, props map[string]any, rOpt *def.RuleOption) (*SourceNode, error) {
	err := ss.Provision(ctx, props)
	if err != nil {
		return nil, err
	}
	ctx.GetLogger().Infof("provision source %s with props %+v", name, props)
	cc := &c{}
	err = cast.MapToStruct(props, cc)
	if err != nil {
		return nil, err
	}
	m := &SourceNode{
		defaultNode: newDefaultNode(name, rOpt),
		s:           ss,
		interval:    cc.Interval,
	}
	switch st := ss.(type) {
	case api.Bounded:
		st.SetEofIngest(m.ingestEof)
	}
	switch ss.(type) {
	case api.PullTupleSource, api.PullBytesSource:
		if cc.Interval < 1 {
			return nil, fmt.Errorf("interval should be larger than 1ms for pull source")
		}
	}
	return m, nil
}

// TODO manage connection, use connection entity later
// Connection must be able to retry. There is another metrics to record the connection status.(connected, retry count, connect time, disconnect time)
// connect and auto reconnect

// Open will be invoked by topo. It starts reading data.
func (m *SourceNode) Open(ctx api.StreamContext, ctrlCh chan<- error) {
	m.prepareExec(ctx, ctrlCh, "source")
	go m.Run(ctx, ctrlCh)
}

func (m *SourceNode) ingestBytes(ctx api.StreamContext, data []byte, meta map[string]any, ts time.Time) {
	ctx.GetLogger().Debugf("source connector %s receive data %+v", m.name, data)
	m.statManager.ProcessTimeStart()
	m.statManager.IncTotalRecordsIn()
	tuple := &xsql.RawTuple{Emitter: m.name, Rawdata: data, Timestamp: ts, Metadata: meta}
	m.Broadcast(tuple)
	m.statManager.IncTotalRecordsOut()
	m.statManager.IncTotalMessagesProcessed(1)
	m.statManager.ProcessTimeEnd()
}

func (m *SourceNode) ingestAnyTuple(ctx api.StreamContext, data any, meta map[string]any, ts time.Time) {
	ctx.GetLogger().Debugf("source connector %s receive data %+v", m.name, data)
	m.statManager.ProcessTimeStart()
	m.statManager.IncTotalRecordsIn()
	switch mess := data.(type) {
	// Maps are expected from user extension
	case map[string]any:
		m.ingestMap(mess, meta, ts)
	case []map[string]any:
		for _, mm := range mess {
			m.ingestMap(mm, meta, ts)
		}
	// expected from file which send out any tuple type
	case []byte:
		tuple := &xsql.RawTuple{Emitter: m.name, Rawdata: mess, Timestamp: ts, Metadata: meta}
		m.Broadcast(tuple)
	// Source tuples are expected from memory
	case *xsql.Tuple:
		m.ingestTuple(mess, ts)
	case []*xsql.Tuple:
		for _, mm := range mess {
			m.ingestTuple(mm, ts)
		}
	default:
		// should never happen
		panic(fmt.Sprintf("receive wrong data %v", data))
	}
	m.statManager.IncTotalMessagesProcessed(1)
	m.statManager.ProcessTimeEnd()
}

func (m *SourceNode) ingestMap(t map[string]any, meta map[string]any, ts time.Time) {
	tuple := &xsql.Tuple{Emitter: m.name, Message: t, Timestamp: ts, Metadata: meta}
	m.Broadcast(tuple)
	m.statManager.IncTotalRecordsOut()
}

func (m *SourceNode) ingestTuple(t *xsql.Tuple, ts time.Time) {
	tuple := &xsql.Tuple{Emitter: m.name, Message: t.Message, Timestamp: ts, Metadata: t.Metadata}
	m.Broadcast(tuple)
	m.statManager.IncTotalRecordsOut()
}

func (m *SourceNode) ingestError(ctx api.StreamContext, err error) {
	ctx.GetLogger().Error(err)
	m.Broadcast(err)
	m.statManager.IncTotalExceptions(err.Error())
}

func (m *SourceNode) ingestEof(ctx api.StreamContext) {
	ctx.GetLogger().Infof("send out EOF")
	m.Broadcast(xsql.EOFTuple(0))
}

// Run Subscribe could be a long-running function
func (m *SourceNode) Run(ctx api.StreamContext, ctrlCh chan<- error) {
	defer func() {
		m.s.Close(ctx)
		m.Close()
	}()
	poe := infra.SafeRun(func() error {
		err := m.s.Connect(ctx)
		if err != nil {
			return err
		}
		switch ss := m.s.(type) {
		case api.BytesSource:
			err = ss.Subscribe(ctx, m.ingestBytes, m.ingestError)
		case api.TupleSource:
			err = ss.Subscribe(ctx, m.ingestAnyTuple, m.ingestError)
		case api.PullBytesSource, api.PullTupleSource:
			err = m.runPull(ctx)
		}
		if err != nil {
			return err
		}
		return nil
	})
	if poe != nil {
		infra.DrainError(ctx, poe, ctrlCh)
	}
	<-ctx.Done()
}

func (m *SourceNode) runPull(ctx api.StreamContext) error {
	m.doPull(ctx, timex.GetNow())
	ticker := timex.GetTicker(m.interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case tc := <-ticker.C:
				ctx.GetLogger().Debugf("source pull at %v", tc.UnixMilli())
				m.doPull(ctx, tc)
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

func (m *SourceNode) doPull(ctx api.StreamContext, tc time.Time) {
	switch ss := m.s.(type) {
	case api.PullBytesSource:
		ss.Pull(ctx, tc, m.ingestBytes, m.ingestError)
	case api.PullTupleSource:
		ss.Pull(ctx, tc, m.ingestAnyTuple, m.ingestError)
	}
}

func (m *SourceNode) Close() {
	m.defaultNode.Close()
}
