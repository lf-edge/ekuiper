package operators

import (
	"context"
	"engine/common"
	"fmt"
	"sync"
)

// UnOperation interface represents unary operations (i.e. Map, Filter, etc)
type UnOperation interface {
	Apply(ctx context.Context, data interface{}) interface{}
}

// UnFunc implements UnOperation as type func (context.Context, interface{})
type UnFunc func(context.Context, interface{}) interface{}

// Apply implements UnOperation.Apply method
func (f UnFunc) Apply(ctx context.Context, data interface{}) interface{} {
	return f(ctx, data)
}



type UnaryOperator struct {
	op          UnOperation
	concurrency int
	input       chan interface{}
	outputs     map[string]chan<- interface{}
	mutex       sync.RWMutex
	cancelled   bool
	name 		string
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

func (o *UnaryOperator) AddOutput(output chan<- interface{}, name string) {
	if _, ok := o.outputs[name]; !ok{
		o.outputs[name] = output
	}else{
		common.Log.Warnf("fail to add output %s, operator %s already has an output of the same name", name, o.name)
	}
}

func (o *UnaryOperator) GetInput() (chan<- interface{}, string) {
	return o.input, o.name
}

// Exec is the entry point for the executor
func (o *UnaryOperator) Exec(ctx context.Context) (err error) {
	log := common.GetLogger(ctx)
	log.Printf("Unary operator %s is started", o.name)

	if len(o.outputs) <= 0 {
		err = fmt.Errorf("no output channel found")
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
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				o.doOp(ctx)
			}(&barrier)
		}

		wait := make(chan struct{})
		go func() {
			defer close(wait)
			barrier.Wait()
		}()

		select {
		case <-wait:
			if o.cancelled {
				log.Printf("Component cancelling...")
				return
			}
		case <-ctx.Done():
			log.Printf("UnaryOp %s done.", o.name)
			return
		}
	}()

	return nil
}

func (o *UnaryOperator) doOp(ctx context.Context) {
	log := common.GetLogger(ctx)
	if o.op == nil {
		log.Println("Unary operator missing operation")
		return
	}
	exeCtx, cancel := context.WithCancel(ctx)

	defer func() {
		log.Infof("unary operator %s done, cancelling future items", o.name)
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
			//case api.StreamError:
			//	fmt.Println( val)
			//	fmt.Println( val)
			//	if item := val.Item(); item != nil {
			//		select {
			//		case o.output <- *item:
			//		case <-exeCtx.Done():
			//			return
			//		}
			//	}
			//	continue
			//case api.PanicStreamError:
			//	util.Logfn(o.logf, val)
			//	autoctx.Err(o.errf, api.StreamError(val))
			//	panic(val)
			//case api.CancelStreamError:
			//	util.Logfn(o.logf, val)
			//	autoctx.Err(o.errf, api.StreamError(val))
			//	return
			case error:
				log.Println(val)
				log.Println(val.Error())
				continue

			default:
				for _, output := range o.outputs{
					select {
					case output <- val:
					}
				}
			}

		// is cancelling
		case <-exeCtx.Done():
			log.Printf("unary operator %s cancelling....", o.name)
			o.mutex.Lock()
			cancel()
			o.cancelled = true
			o.mutex.Unlock()
			return
		}
	}
}
