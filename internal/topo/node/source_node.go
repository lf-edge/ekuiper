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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/lf-edge/ekuiper/v2/internal/io/memory/pubsub"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/sig"
	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node/tracenode"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

// SourceNode is a node that connects to an external source
// The SourceNode is an all-in-one source node that support connect and decode and more.
// The SourceConnectorNode is a node that only connects to external source and does not decode.
type SourceNode struct {
	*defaultNode

	s         api.Source
	interval  time.Duration
	notifySub bool
}

type sourceConf struct {
	Interval cast.DurationConf `json:"interval"`
}

// NewSourceNode creates a SourceConnectorNode
func NewSourceNode(ctx api.StreamContext, name string, ss api.Source, props map[string]any, rOpt *def.RuleOption) (*SourceNode, error) {
	err := ss.Provision(ctx, props)
	if err != nil {
		return nil, err
	}
	ctx.GetLogger().Infof("provision source %s with props %+v", name, props)
	if sit, ok := ss.(model.InfoNode); ok {
		ss = sit.TransformType()
	}
	cc := &sourceConf{}
	err = cast.MapToStruct(props, cc)
	if err != nil {
		return nil, err
	}
	m := &SourceNode{
		defaultNode: newDefaultNode(name, rOpt),
		s:           ss,
		interval:    time.Duration(cc.Interval),
		notifySub:   rOpt.NotifySub,
	}
	switch st := ss.(type) {
	case api.Bounded:
		st.SetEofIngest(m.ingestEof)
	}
	return m, nil
}

// Open will be invoked by topo. It starts reading data.
func (m *SourceNode) Open(ctx api.StreamContext, ctrlCh chan<- error) {
	m.prepareExec(ctx, ctrlCh, "source")
	go m.Run(ctx, ctrlCh)
}

func (m *SourceNode) ingestBytes(ctx api.StreamContext, data []byte, meta map[string]any, ts time.Time) {
	ctx.GetLogger().Debugf("source connector %s receive data %+v", m.name, data)
	m.onProcessStart(ctx, nil)
	if meta == nil {
		meta = make(map[string]any)
	}
	tuple := &xsql.RawTuple{Emitter: m.name, Rawdata: data, Timestamp: ts, Metadata: meta}
	m.traceStart(ctx, meta, tuple)
	m.Broadcast(tuple)
	m.onSend(ctx, tuple)
	m.onProcessEnd(ctx)
	_ = m.updateState(ctx)
}

func (m *SourceNode) traceStart(ctx api.StreamContext, meta map[string]any, tuple xsql.HasTracerCtx) {
	if !ctx.IsTraceEnabled() {
		return
	}
	var (
		traced   bool
		traceCtx api.StreamContext
		span     trace.Span
	)
	setType := false
	opts := make([]trace.SpanStartOption, 0)
	rawKind, ok := meta["sourceKind"]
	if ok {
		kind, ok := rawKind.(string)
		if ok && kind == "neuron" {
			opts = append(opts, trace.WithAttributes(attribute.String("span.mytype", "data-collection")))
			setType = true
		}
	}
	if !setType {
		opts = append(opts, trace.WithAttributes(attribute.String("span.mytype", "data-processing")))
	}
	// If read from parent trace
	if tid, ok := meta["traceId"]; ok {
		traced, traceCtx, span = tracenode.StartTraceByID(ctx, tid.(string), opts...)
	} else {
		strategy := tracenode.ExtractStrategy(ctx)
		if strategy != topoContext.AlwaysTraceStrategy {
			return
		}
		traced, traceCtx, span = tracenode.StartTraceBackground(ctx, ctx.GetOpId(), opts...)
		meta["traceId"] = span.SpanContext().TraceID()
	}
	if traced {
		tracenode.RecordRowOrCollection(tuple, span)
		m.span = span
		m.spanCtx = traceCtx
	}
}

func (m *SourceNode) ingestAnyTuple(ctx api.StreamContext, data any, meta map[string]any, ts time.Time) {
	ctx.GetLogger().Debugf("source connector %s receive data %+v", m.name, data)
	m.onProcessStart(ctx, nil)
	if meta == nil {
		meta = make(map[string]any)
	}
	switch mess := data.(type) {
	// Maps are expected from user extension
	case map[string]any:
		m.ingestMap(mess, meta, ts)
	case xsql.Message:
		m.ingestMap(mess, meta, ts)
	case []map[string]any:
		for _, mm := range mess {
			m.ingestMap(mm, meta, ts)
		}
	// expected from file which send out any tuple type
	case []byte:
		tuple := &xsql.RawTuple{Emitter: m.name, Rawdata: mess, Timestamp: ts, Metadata: meta}
		m.traceStart(ctx, meta, tuple)
		m.Broadcast(tuple)
		m.onSend(ctx, tuple)
	// Source tuples are expected from memory
	case *xsql.Tuple:
		m.ingestTuple(mess, ts)
	case []*xsql.Tuple:
		for _, mm := range mess {
			m.ingestTuple(mm, ts)
		}
	case []pubsub.MemTuple:
		for _, mm := range mess {
			m.ingestTuple(mm.(*xsql.Tuple), ts)
		}
	default:
		// should never happen
		panic(fmt.Sprintf("receive wrong data %v", data))
	}
	m.onProcessEnd(ctx)
	_ = m.updateState(ctx)
}

func (m *SourceNode) connectionStatusChange(status string, message string) {
	// TODO only send out error when status change from connected?
	m.ctx.GetLogger().Debugf("receive status %s message %s", status, message)
	if status == api.ConnectionDisconnected {
		m.ingestError(m.ctx, fmt.Errorf("disconnected: %s", message))
	}
	m.statManager.SetConnectionState(status, message)
}

func (m *SourceNode) ingestMap(t map[string]any, meta map[string]any, ts time.Time) {
	tuple := &xsql.Tuple{Emitter: m.name, Message: t, Timestamp: ts, Metadata: meta}
	m.traceStart(m.ctx, meta, tuple)
	m.Broadcast(tuple)
	m.onSend(m.ctx, tuple)
}

func (m *SourceNode) ingestTuple(t *xsql.Tuple, ts time.Time) {
	tuple := &xsql.Tuple{Emitter: m.name, Message: t.Message, Timestamp: ts, Metadata: t.Metadata, Ctx: t.Ctx}
	// If receiving tuple, its source is still in the system. So continue tracing
	traced, spanCtx, span := tracenode.TraceInput(m.ctx, tuple, m.name)
	if traced {
		tracenode.RecordRowOrCollection(tuple, span)
		m.span = span
		m.spanCtx = spanCtx
	}
	m.Broadcast(tuple)
	m.onSend(m.ctx, tuple)
}

func (m *SourceNode) ingestError(ctx api.StreamContext, err error) {
	m.onError(ctx, err)
}

func (m *SourceNode) ingestEof(ctx api.StreamContext) {
	ctx.GetLogger().Infof("send out EOF")
	m.Broadcast(xsql.EOFTuple(0))
}

// GetSource only used for test
func (m *SourceNode) GetSource() api.Source {
	return m.s
}

const (
	OffsetKey = "$$offset"
)

func (m *SourceNode) Rewind(ctx api.StreamContext) error {
	s := m.s
	if rw, ok := s.(api.Rewindable); ok {
		if offset, err := ctx.GetState(OffsetKey); err != nil {
			return err
		} else if offset != nil {
			ctx.GetLogger().Infof("Source rewind from %v", offset)
			err = rw.Rewind(offset)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *SourceNode) updateState(ctx api.StreamContext) error {
	s := m.s
	if rw, ok := s.(api.Rewindable); ok {
		state, err := rw.GetOffset()
		if err != nil {
			return err
		}
		return ctx.PutState(OffsetKey, state)
	}
	return nil
}

// Run Subscribe could be a long-running function
func (m *SourceNode) Run(ctx api.StreamContext, ctrlCh chan<- error) {
	defer func() {
		m.s.Close(ctx)
		m.Close()
		if m.notifySub && sig.Ctrl != nil {
			sig.Ctrl.Rem(m.name)
		}
	}()
	poe := infra.SafeRun(func() error {
		// Blocking and wait for connection. The connect will call the dial and retry if fails
		err := m.s.Connect(ctx, m.connectionStatusChange)
		if err != nil {
			return err
		}
		if err := m.Rewind(ctx); err != nil {
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
		if m.notifySub && sig.Ctrl != nil {
			sig.Ctrl.Add(m.name)
		}
		return nil
	})
	if poe != nil {
		infra.DrainError(ctx, poe, ctrlCh)
	}
	<-ctx.Done()
}

func (m *SourceNode) runPull(ctx api.StreamContext) error {
	err := m.doPull(ctx, timex.GetNow())
	if err != nil {
		return err
	}
	if m.interval > 0 {
		ticker := timex.GetTicker(m.interval)
		go func() {
			defer ticker.Stop()
			for {
				select {
				case tc := <-ticker.C:
					ctx.GetLogger().Debugf("source pull at %v", tc.UnixMilli())
					e := m.doPull(ctx, tc)
					if e != nil {
						m.ingestError(ctx, e)
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	return nil
}

func (m *SourceNode) doPull(ctx api.StreamContext, tc time.Time) error {
	return infra.SafeRun(func() error {
		switch ss := m.s.(type) {
		case api.PullBytesSource:
			ss.Pull(ctx, tc, m.ingestBytes, m.ingestError)
		case api.PullTupleSource:
			ss.Pull(ctx, tc, m.ingestAnyTuple, m.ingestError)
		}
		return nil
	})
}
