// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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

package topo

import (
	"context"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/checkpoint"
	kctx "github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/node"
	"github.com/lf-edge/ekuiper/internal/topo/node/metric"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/infra"
	"strconv"
	"sync"
)

type Topo struct {
	sources            []node.DataSourceNode
	sinks              []*node.SinkNode
	ctx                api.StreamContext
	cancel             context.CancelFunc
	drain              chan error
	ops                []node.OperatorNode
	name               string
	qos                api.Qos
	checkpointInterval int
	store              api.Store
	coordinator        *checkpoint.Coordinator
	topo               *api.PrintableTopo
	mu                 sync.Mutex
}

func NewWithNameAndQos(name string, qos api.Qos, checkpointInterval int) (*Topo, error) {
	tp := &Topo{
		name:               name,
		qos:                qos,
		checkpointInterval: checkpointInterval,
		topo: &api.PrintableTopo{
			Sources: make([]string, 0),
			Edges:   make(map[string][]interface{}),
		},
	}
	return tp, nil
}

func (s *Topo) GetContext() api.StreamContext {
	return s.ctx
}

// Cancel may be called multiple times so must be idempotent
func (s *Topo) Cancel() {
	s.mu.Lock()
	defer s.mu.Unlock()
	// completion signal
	infra.DrainError(s.ctx, nil, s.drain)
	if s.cancel != nil {
		s.cancel()
	}
	s.store = nil
	s.coordinator = nil
}

func (s *Topo) AddSrc(src node.DataSourceNode) *Topo {
	s.sources = append(s.sources, src)
	s.topo.Sources = append(s.topo.Sources, fmt.Sprintf("source_%s", src.GetName()))
	return s
}

func (s *Topo) AddSink(inputs []api.Emitter, snk *node.SinkNode) *Topo {
	for _, input := range inputs {
		input.AddOutput(snk.GetInput())
		snk.AddInputCount()
		s.addEdge(input.(api.TopNode), snk, "sink")
	}
	s.sinks = append(s.sinks, snk)
	return s
}

func (s *Topo) AddOperator(inputs []api.Emitter, operator node.OperatorNode) *Topo {
	for _, input := range inputs {
		input.AddOutput(operator.GetInput())
		operator.AddInputCount()
		s.addEdge(input.(api.TopNode), operator, "op")
	}
	s.ops = append(s.ops, operator)
	return s
}

func (s *Topo) addEdge(from api.TopNode, to api.TopNode, toType string) {
	fromType := "op"
	if _, ok := from.(node.DataSourceNode); ok {
		fromType = "source"
	}
	f := fmt.Sprintf("%s_%s", fromType, from.GetName())
	t := fmt.Sprintf("%s_%s", toType, to.GetName())
	e, ok := s.topo.Edges[f]
	if !ok {
		e = make([]interface{}, 0)
	}
	s.topo.Edges[f] = append(e, t)
}

// prepareContext setups internal context before
// stream starts execution.
func (s *Topo) prepareContext() {
	if s.ctx == nil || s.ctx.Err() != nil {
		contextLogger := conf.Log.WithField("rule", s.name)
		ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
		s.ctx, s.cancel = ctx.WithCancel()
	}
}

func (s *Topo) Open() <-chan error {

	//if stream has opened, do nothing
	if s.ctx != nil && s.ctx.Err() == nil {
		s.ctx.GetLogger().Infoln("rule is already running, do nothing")
		return s.drain
	}
	s.prepareContext() // ensure context is set
	s.drain = make(chan error)
	log := s.ctx.GetLogger()
	log.Infoln("Opening stream")
	go func() {
		err := infra.SafeRun(func() error {
			s.mu.Lock()
			defer s.mu.Unlock()
			var err error
			if s.store, err = state.CreateStore(s.name, s.qos); err != nil {
				return fmt.Errorf("topo %s create store error %v", s.name, err)
			}
			s.enableCheckpoint()
			// open stream sink, after log sink is ready.
			for _, snk := range s.sinks {
				snk.Open(s.ctx.WithMeta(s.name, snk.GetName(), s.store), s.drain)
			}

			//apply operators, if err bail
			for _, op := range s.ops {
				op.Exec(s.ctx.WithMeta(s.name, op.GetName(), s.store), s.drain)
			}

			// open source, if err bail
			for _, source := range s.sources {
				source.Open(s.ctx.WithMeta(s.name, source.GetName(), s.store), s.drain)
			}

			// activate checkpoint
			if s.coordinator != nil {
				s.coordinator.Activate()
			}
			return nil
		})
		if err != nil {
			infra.DrainError(s.ctx, err, s.drain)
		}
	}()

	return s.drain
}

func (s *Topo) enableCheckpoint() error {
	if s.qos >= api.AtLeastOnce {
		var sources []checkpoint.StreamTask
		for _, r := range s.sources {
			sources = append(sources, r)
		}
		var ops []checkpoint.NonSourceTask
		for _, r := range s.ops {
			ops = append(ops, r)
		}
		var sinks []checkpoint.SinkTask
		for _, r := range s.sinks {
			sinks = append(sinks, r)
		}
		c := checkpoint.NewCoordinator(s.name, sources, ops, sinks, s.qos, s.store, s.checkpointInterval, s.ctx)
		s.coordinator = c
	}
	return nil
}

func (s *Topo) GetCoordinator() *checkpoint.Coordinator {
	return s.coordinator
}

func (s *Topo) GetMetrics() (keys []string, values []interface{}) {
	for _, sn := range s.sources {
		for ins, metrics := range sn.GetMetrics() {
			for i, v := range metrics {
				keys = append(keys, "source_"+sn.GetName()+"_"+strconv.Itoa(ins)+"_"+metric.MetricNames[i])
				values = append(values, v)
			}
		}
	}
	for _, so := range s.ops {
		for ins, metrics := range so.GetMetrics() {
			for i, v := range metrics {
				keys = append(keys, "op_"+so.GetName()+"_"+strconv.Itoa(ins)+"_"+metric.MetricNames[i])
				values = append(values, v)
			}
		}
	}
	for _, sn := range s.sinks {
		for ins, metrics := range sn.GetMetrics() {
			for i, v := range metrics {
				keys = append(keys, "sink_"+sn.GetName()+"_"+strconv.Itoa(ins)+"_"+metric.MetricNames[i])
				values = append(values, v)
			}
		}
	}
	return
}

func (s *Topo) RemoveMetrics() {
	for _, sn := range s.sources {
		sn.RemoveMetrics(s.name)
	}
	for _, so := range s.ops {
		so.RemoveMetrics(s.name)
	}
	for _, sn := range s.sinks {
		sn.RemoveMetrics(s.name)
	}
}

func (s *Topo) GetTopo() *api.PrintableTopo {
	return s.topo
}
