package xstream

import (
	"context"
	"engine/common"
	"engine/xstream/operators"
)

var log = common.Log

type TopologyNew struct {
	sources []Source
	sinks []Sink
	ctx context.Context

	drain chan error
	ops []Operator
}

func New() (*TopologyNew) {
	tp := &TopologyNew{}
	return tp
}

func (tp *TopologyNew) AddSrc(src Source) (*TopologyNew) {
	tp.sources = append(tp.sources, src)
	return tp
}

func (tp *TopologyNew) AddSink(inputs []Emitter, snk Sink) (*TopologyNew) {
	for _, input := range inputs{
		input.AddOutput(snk.GetInput())
	}
	tp.sinks = append(tp.sinks, snk)
	return tp
}

func (tp *TopologyNew) AddOperator(inputs []Emitter, operator Operator) (*TopologyNew) {
	for _, input := range inputs{
		input.AddOutput(operator.GetInput())
	}
	tp.ops = append(tp.ops, operator)
	return tp
}

func Transform(op operators.UnOperation, name string) *operators.UnaryOperator {
	operator := operators.New(name)
	operator.SetOperation(op)
	return operator
}

func (tp *TopologyNew) Map(f interface{}) (*TopologyNew){
	op, err := MapFunc(f)
	if err != nil {
		log.Println(err)
	}
	return tp.Transform(op)
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
	if s.ctx == nil {
		s.ctx = context.TODO()
	}
}

func (s *TopologyNew) drainErr(err error) {
	go func() { s.drain <- err }()
}

func (s *TopologyNew) Open() <-chan error {
	s.prepareContext() // ensure context is set

	log.Println("Opening stream")

	// open stream
	go func() {
		// open source, if err bail
		for _, src := range s.sources{
			if err := src.Open(s.ctx); err != nil {
				s.drainErr(err)
				return
			}
		}

		//apply operators, if err bail
		for _, op := range s.ops {
			if err := op.Exec(s.ctx); err != nil {
				s.drainErr(err)
				return
			}
		}

		// open stream sink, after log sink is ready.
		for _, snk := range s.sinks{
			select {
			case err := <-snk.Open(s.ctx):
				log.Println("Closing stream")
				s.drain <- err
			}
		}

	}()

	return s.drain
}