package operators

import (
	"fmt"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/nodes"
	"sync"
)

// UnOperation interface represents unary operations (i.e. Map, Filter, etc)
type UnOperation interface {
	Apply(ctx api.StreamContext, data interface{}) interface{}
}

// UnFunc implements UnOperation as type func (context.Context, interface{})
type UnFunc func(api.StreamContext, interface{}) interface{}

// Apply implements UnOperation.Apply method
func (f UnFunc) Apply(ctx api.StreamContext, data interface{}) interface{} {
	return f(ctx, data)
}

type UnaryOperator struct {
	op          UnOperation
	concurrency int
	input       chan interface{}
	outputs     map[string]chan<- interface{}
	mutex       sync.RWMutex
	cancelled   bool
	name        string
}

// NewUnary creates *UnaryOperator value
func New(name string) *UnaryOperator {
	// extract logger
	o := new(UnaryOperator)

	o.concurrency = 1
	o.input = make(chan interface{}, 1024)
	o.outputs = make(map[string]chan<- interface{})
	o.name = name
	return o
}

func (o *UnaryOperator) GetName() string {
	return o.name
}

// SetOperation sets the executor operation
func (o *UnaryOperator) SetOperation(op UnOperation) {
	o.op = op
}

// SetConcurrency sets the concurrency level for the operation
func (o *UnaryOperator) SetConcurrency(concurr int) {
	o.concurrency = concurr
	if o.concurrency < 1 {
		o.concurrency = 1
	}
}

func (o *UnaryOperator) AddOutput(output chan<- interface{}, name string) error {
	if _, ok := o.outputs[name]; !ok {
		o.outputs[name] = output
	} else {
		return fmt.Errorf("fail to add output %s, operator %s already has an output of the same name", name, o.name)
	}
	return nil
}

func (o *UnaryOperator) GetInput() (chan<- interface{}, string) {
	return o.input, o.name
}

// Exec is the entry point for the executor
func (o *UnaryOperator) Exec(ctx api.StreamContext, errCh chan<- error) {
	log := ctx.GetLogger()
	log.Debugf("Unary operator %s is started", o.name)

	if len(o.outputs) <= 0 {
		go func() { errCh <- fmt.Errorf("no output channel found") }()
		return
	}

	// validate p
	if o.concurrency < 1 {
		o.concurrency = 1
	}

	go func() {
		var barrier sync.WaitGroup
		wgDelta := o.concurrency
		barrier.Add(wgDelta)

		for i := 0; i < o.concurrency; i++ { // workers
			go func(wg *sync.WaitGroup, instance int) {
				defer wg.Done()
				o.doOp(ctx.WithInstance(instance), errCh)
			}(&barrier, i)
		}

		wait := make(chan struct{})
		go func() {
			defer close(wait)
			barrier.Wait()
		}()

		select {
		case <-wait:
			if o.cancelled {
				log.Infof("Component cancelling...")
				return
			}
		case <-ctx.Done():
			log.Infof("UnaryOp %s done.", o.name)
			return
		}
	}()
}

func (o *UnaryOperator) doOp(ctx api.StreamContext, errCh chan<- error) {
	log := ctx.GetLogger()
	if o.op == nil {
		log.Infoln("Unary operator missing operation")
		return
	}
	exeCtx, cancel := ctx.WithCancel()

	defer func() {
		log.Infof("unary operator %s instance %d done, cancelling future items", o.name, ctx.GetInstanceId())
		cancel()
	}()

	for {
		select {
		// process incoming item
		case item := <-o.input:
			result := o.op.Apply(exeCtx, item)

			switch val := result.(type) {
			case nil:
				continue
			case error: //TODO error handling
				log.Infoln(val)
				log.Infoln(val.Error())
				continue
			default:
				nodes.Broadcast(o.outputs, val, ctx)
			}

		// is cancelling
		case <-ctx.Done():
			log.Infof("unary operator %s instance %d cancelling....", o.name, ctx.GetInstanceId())
			o.mutex.Lock()
			cancel()
			o.cancelled = true
			o.mutex.Unlock()
			return
		}
	}
}
