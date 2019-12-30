package xstream

import (
	"context"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/contexts"
	"github.com/emqx/kuiper/xstream/nodes"
	"github.com/emqx/kuiper/xstream/operators"
)

type TopologyNew struct {
	sources []*nodes.SourceNode
	sinks   []*nodes.SinkNode
	ctx     api.StreamContext
	cancel  context.CancelFunc
	drain   chan error
	ops     []api.Operator
	name    string
}

func NewWithName(name string) *TopologyNew {
	tp := &TopologyNew{
		name:  name,
		drain: make(chan error),
	}
	return tp
}

func (s *TopologyNew) GetContext() api.StreamContext {
	return s.ctx
}

func (s *TopologyNew) Cancel() {
	s.cancel()
}

func (s *TopologyNew) AddSrc(src *nodes.SourceNode) *TopologyNew {
	s.sources = append(s.sources, src)
	return s
}

func (s *TopologyNew) AddSink(inputs []api.Emitter, snk *nodes.SinkNode) *TopologyNew {
	for _, input := range inputs {
		input.AddOutput(snk.GetInput())
	}
	s.sinks = append(s.sinks, snk)
	return s
}

func (s *TopologyNew) AddOperator(inputs []api.Emitter, operator api.Operator) *TopologyNew {
	for _, input := range inputs {
		input.AddOutput(operator.GetInput())
	}
	s.ops = append(s.ops, operator)
	return s
}

func Transform(op operators.UnOperation, name string, bufferLength int) *operators.UnaryOperator {
	operator := operators.New(name, bufferLength)
	operator.SetOperation(op)
	return operator
}

// prepareContext setups internal context before
// stream starts execution.
func (s *TopologyNew) prepareContext() {
	if s.ctx == nil || s.ctx.Err() != nil {
		contextLogger := common.Log.WithField("rule", s.name)
		ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
		s.ctx, s.cancel = ctx.WithCancel()
	}
}

func (s *TopologyNew) drainErr(err error) {
	go func() { s.drain <- err }()
}

func (s *TopologyNew) Open() <-chan error {
	s.prepareContext() // ensure context is set
	log := s.ctx.GetLogger()
	log.Infoln("Opening stream")

	// open stream
	go func() {
		// open stream sink, after log sink is ready.
		for _, snk := range s.sinks {
			snk.Open(s.ctx.WithMeta(s.name, snk.GetName()), s.drain)
		}

		//apply operators, if err bail
		for _, op := range s.ops {
			op.Exec(s.ctx.WithMeta(s.name, op.GetName()), s.drain)
		}

		// open source, if err bail
		for _, node := range s.sources {
			node.Open(s.ctx.WithMeta(s.name, node.GetName()), s.drain)
		}
	}()

	return s.drain
}

func (s *TopologyNew) GetMetrics() map[string]interface{} {
	result := make(map[string]interface{})
	for _, node := range s.sources {
		for k, v := range node.GetMetrics() {
			result[k] = v
		}
	}
	for _, node := range s.ops {
		for k, v := range node.GetMetrics() {
			result[k] = v
		}
	}
	for _, node := range s.sinks {
		for k, v := range node.GetMetrics() {
			result[k] = v
		}
	}
	return result
}
