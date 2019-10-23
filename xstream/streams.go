package xstream

import (
	"context"
	"engine/common"
	"engine/xstream/operators"
)

type TopologyNew struct {
	sources []Source
	sinks []Sink
	ctx context.Context
	cancel context.CancelFunc
	drain chan error
	ops []Operator
	name string
}

func NewWithName(name string) *TopologyNew {
	tp := &TopologyNew{name: name}
	return tp
}

func (s *TopologyNew) GetContext() context.Context {
	return s.ctx
}

func (s *TopologyNew) Cancel(){
	s.cancel()
}

func (s *TopologyNew) AddSrc(src Source) *TopologyNew {
	s.sources = append(s.sources, src)
	return s
}

func (s *TopologyNew) AddSink(inputs []Emitter, snk Sink) *TopologyNew {
	for _, input := range inputs{
		input.AddOutput(snk.GetInput())
	}
	s.sinks = append(s.sinks, snk)
	return s
}

func (s *TopologyNew) AddOperator(inputs []Emitter, operator Operator) *TopologyNew {
	for _, input := range inputs{
		input.AddOutput(operator.GetInput())
	}
	s.ops = append(s.ops, operator)
	return s
}

func Transform(op operators.UnOperation, name string) *operators.UnaryOperator {
	operator := operators.New(name)
	operator.SetOperation(op)
	return operator
}

func (s *TopologyNew) Map(f interface{}) *TopologyNew {
	log := common.GetLogger(s.ctx)
	op, err := MapFunc(f)
	if err != nil {
		log.Println(err)
	}
	return s.Transform(op)
}

// Filter takes a predicate user-defined func that filters the stream.
// The specified function must be of type:
//   func (T) bool
// If the func returns true, current item continues downstream.
func (s *TopologyNew) Filter(f interface{}) *TopologyNew {
	op, err := FilterFunc(f)
	if err != nil {
		s.drainErr(err)
	}
	return s.Transform(op)
}

// Transform is the base method used to apply transfomrmative
// unary operations to streamed elements (i.e. filter, map, etc)
// It is exposed here for completeness, use the other more specific methods.
func (s *TopologyNew) Transform(op operators.UnOperation) *TopologyNew {
	operator := operators.New("default")
	operator.SetOperation(op)
	s.ops = append(s.ops, operator)
	return s
}

// prepareContext setups internal context before
// stream starts execution.
func (s *TopologyNew) prepareContext() {
	if s.ctx == nil || s.ctx.Err() != nil {
		s.ctx, s.cancel = context.WithCancel(context.Background())
		contextLogger := common.Log.WithField("rule", s.name)
		s.ctx = context.WithValue(s.ctx, common.LoggerKey, contextLogger)
	}
}

func (s *TopologyNew) drainErr(err error) {
	go func() { s.drain <- err }()
}

func (s *TopologyNew) Open() <-chan error {
	s.prepareContext() // ensure context is set
	log := common.GetLogger(s.ctx)
	log.Println("Opening stream")

	// open stream
	go func() {
		// open source, if err bail
		for _, src := range s.sources{
			if err := src.Open(s.ctx); err != nil {
				s.drainErr(err)
				log.Println("Closing stream")
				return
			}
		}

		//apply operators, if err bail
		for _, op := range s.ops {
			if err := op.Exec(s.ctx); err != nil {
				s.drainErr(err)
				log.Println("Closing stream")
				return
			}
		}
		sinkErr := make(chan error)
		defer func() {
			log.Println("Closing sinkErr channel")
			close(sinkErr)
		}()
		// open stream sink, after log sink is ready.
		for _, snk := range s.sinks{
			snk.Open(s.ctx, sinkErr)
		}
		select {
		case err := <- sinkErr:
			log.Println("Closing stream")
			s.drain <- err
		}
	}()

	return s.drain
}