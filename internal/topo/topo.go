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

package topo

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	rotatelogs "github.com/yisaer/file-rotatelogs"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/checkpoint"
	kctx "github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/node"
	"github.com/lf-edge/ekuiper/internal/topo/node/metric"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/infra"
)

type Topo struct {
	sources     []node.DataSourceNode
	sinks       []*node.SinkNode
	ctx         api.StreamContext
	cancel      context.CancelFunc
	drain       chan error
	ops         []node.OperatorNode
	name        string
	options     *api.RuleOption
	store       api.Store
	coordinator *checkpoint.Coordinator
	topo        *api.PrintableTopo
	mu          sync.Mutex
	hasOpened   atomic.Bool
}

func NewWithNameAndOptions(name string, options *api.RuleOption) (*Topo, error) {
	tp := &Topo{
		name:    name,
		options: options,
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

func (s *Topo) NewTopoWithSucceededCtx() *Topo {
	n := &Topo{}
	n.ctx = s.ctx
	n.cancel = s.cancel
	return n
}

func (s *Topo) GetName() string {
	return s.name
}

// Cancel may be called multiple times so must be idempotent
func (s *Topo) Cancel() {
	s.hasOpened.Store(false)
	s.mu.Lock()
	defer s.mu.Unlock()
	// completion signal
	infra.DrainError(s.ctx, nil, s.drain)
	if s.cancel != nil {
		s.cancel()
	}
	s.store = nil
	s.coordinator = nil
	for _, src := range s.sources {
		switch rt := src.(type) {
		case node.MergeableTopo:
			rt.Close(s.name)
		}
	}
}

func (s *Topo) AddSrc(src node.DataSourceNode) *Topo {
	s.sources = append(s.sources, src)
	switch rt := src.(type) {
	case node.MergeableTopo:
		rt.MergeSrc(s.topo)
	default:
		s.topo.Sources = append(s.topo.Sources, fmt.Sprintf("source_%s", src.GetName()))
	}
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
		// add rule id to make operator name unique
		ch, opName := operator.GetInput()
		_ = input.AddOutput(ch, fmt.Sprintf("%s_%s", s.name, opName))
		operator.AddInputCount()
		switch rt := input.(type) {
		case node.MergeableTopo:
			rt.LinkTopo(s.topo, operator.GetName())
		case api.TopNode:
			s.addEdge(rt, operator, "op")
		}
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
		if s.options != nil && (s.options.Debug || s.options.LogFilename != "") {
			contextLogger.Logger = &logrus.Logger{
				Out:          conf.Log.Out,
				Hooks:        conf.Log.Hooks,
				Level:        conf.Log.Level,
				Formatter:    conf.Log.Formatter,
				ReportCaller: conf.Log.ReportCaller,
				ExitFunc:     conf.Log.ExitFunc,
				BufferPool:   conf.Log.BufferPool,
			}
			if conf.Config.Basic.Debug || s.options.Debug {
				contextLogger.Logger.SetLevel(logrus.DebugLevel)
			}
			if s.options.LogFilename != "" {
				logDir, _ := conf.GetLogLoc()

				file := path.Join(logDir, path.Base(s.options.LogFilename))
				output, err := rotatelogs.New(
					file+".%Y-%m-%d_%H-%M-%S",
					rotatelogs.WithLinkName(file),
					rotatelogs.WithRotationTime(time.Hour*time.Duration(conf.Config.Basic.RotateTime)),
					rotatelogs.WithMaxAge(time.Hour*time.Duration(conf.Config.Basic.MaxAge)),
				)
				if err != nil {
					conf.Log.Warnf("Create rule log file failed: %s", file)
				} else if conf.Config.Basic.ConsoleLog {
					contextLogger.Logger.SetOutput(io.MultiWriter(output, os.Stdout))
				} else if !conf.Config.Basic.ConsoleLog {
					contextLogger.Logger.SetOutput(output)
				}
			}
		}
		ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
		ctx = kctx.WithValue(ctx, kctx.RuleStartKey, conf.GetNowInMilli())
		s.ctx, s.cancel = ctx.WithCancel()
	}
}

func (s *Topo) Open() <-chan error {
	// if stream has opened, do nothing
	if s.hasOpened.Load() && !conf.IsTesting {
		s.ctx.GetLogger().Infoln("rule is already running, do nothing")
		return s.drain
	}
	s.hasOpened.Store(true)
	s.prepareContext() // ensure context is set
	s.drain = make(chan error)
	log := s.ctx.GetLogger()
	log.Infoln("Opening stream")
	go func() {
		err := infra.SafeRun(func() error {
			s.mu.Lock()
			defer s.mu.Unlock()
			var err error
			if s.store, err = state.CreateStore(s.name, s.options.Qos); err != nil {
				return fmt.Errorf("topo %s create store error %v", s.name, err)
			}
			s.enableCheckpoint(s.ctx)
			// open stream sink, after log sink is ready.
			for _, snk := range s.sinks {
				snk.Open(s.ctx.WithMeta(s.name, snk.GetName(), s.store), s.drain)
			}

			for _, op := range s.ops {
				op.Exec(s.ctx.WithMeta(s.name, op.GetName(), s.store), s.drain)
			}

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

func (s *Topo) HasOpen() bool {
	return s.hasOpened.Load()
}

func (s *Topo) enableCheckpoint(ctx api.StreamContext) {
	if s.options.Qos >= api.AtLeastOnce {
		var (
			sources []checkpoint.StreamTask
			ops     []checkpoint.NonSourceTask
		)
		for _, r := range s.sources {
			switch rt := r.(type) {
			case checkpoint.StreamTask:
				sources = append(sources, r.(checkpoint.StreamTask))
			case checkpoint.SourceSubTopoTask:
				rt.EnableCheckpoint(&sources, &ops)
			default: // should never happen
				ctx.GetLogger().Errorf("source %s is not a checkpoint task", r.GetName())
			}
		}
		for _, r := range s.ops {
			ops = append(ops, r)
		}
		var sinks []checkpoint.SinkTask
		for _, r := range s.sinks {
			sinks = append(sinks, r)
		}
		c := checkpoint.NewCoordinator(s.name, sources, ops, sinks, s.options.Qos, s.store, s.options.CheckpointInterval, s.ctx)
		s.coordinator = c
	}
}

func (s *Topo) GetCoordinator() *checkpoint.Coordinator {
	return s.coordinator
}

func (s *Topo) GetMetrics() (keys []string, values []any) {
	for _, sn := range s.sources {
		switch st := sn.(type) {
		case node.MergeableTopo:
			skeys, svalues := st.SubMetrics()
			keys = append(keys, skeys...)
			values = append(values, svalues...)
		default:
			for i, v := range sn.GetMetrics() {
				keys = append(keys, "source_"+sn.GetName()+"_0_"+metric.MetricNames[i])
				values = append(values, v)
			}
		}
	}
	for _, so := range s.ops {
		for i, v := range so.GetMetrics() {
			keys = append(keys, "op_"+so.GetName()+"_0_"+metric.MetricNames[i])
			values = append(values, v)
		}
	}
	for _, sn := range s.sinks {
		for i, v := range sn.GetMetrics() {
			keys = append(keys, "sink_"+sn.GetName()+"_0_"+metric.MetricNames[i])
			values = append(values, v)
		}
	}
	return
}

func (s *Topo) RemoveMetrics() {
	conf.Log.Infof("start removing %v metrics", s.name)
	for _, sn := range s.sources {
		sn.RemoveMetrics(s.name)
	}
	for _, so := range s.ops {
		so.RemoveMetrics(s.name)
	}
	for _, sn := range s.sinks {
		sn.RemoveMetrics(s.name)
	}
	conf.Log.Infof("finish removing %v metrics", s.name)
}

func (s *Topo) GetTopo() *api.PrintableTopo {
	return s.topo
}

func (s *Topo) ResetStreamOffset(name string, input map[string]interface{}) error {
	for _, source := range s.sources {
		if source.GetName() == name {
			if sn, ok := source.(node.SourceInstanceNode); ok {
				src := sn.GetSource()
				if r, ok := src.(api.Rewindable); ok {
					return r.ResetOffset(input)
				}
			}
		}
	}
	return fmt.Errorf("stream %v not found in topo", name)
}
