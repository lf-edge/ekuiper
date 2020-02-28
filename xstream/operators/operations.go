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
	op           UnOperation
	concurrency  int
	input        chan interface{}
	outputs      map[string]chan<- interface{}
	mutex        sync.RWMutex
	cancelled    bool
	name         string
	statManagers []nodes.StatManager
}

// NewUnary creates *UnaryOperator value
func New(name string, bufferLength int) *UnaryOperator {
	// extract logger
	o := new(UnaryOperator)

	o.concurrency = 1
	o.input = make(chan interface{}, bufferLength)
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
	//reset status
	o.statManagers = nil

	for i := 0; i < o.concurrency; i++ { // workers
		instance := i
		go o.doOp(ctx.WithInstance(instance), errCh)
	}
}

func (o *UnaryOperator) doOp(ctx api.StreamContext, errCh chan<- error) {
	logger := ctx.GetLogger()
	if o.op == nil {
		logger.Infoln("Unary operator missing operation")
		return
	}
	exeCtx, cancel := ctx.WithCancel()

	defer func() {
		logger.Infof("unary operator %s instance %d done, cancelling future items", o.name, ctx.GetInstanceId())
		cancel()
	}()

	stats, err := nodes.NewStatManager("op", ctx)
	if err != nil {
		select {
		case errCh <- err:
		case <-ctx.Done():
			logger.Infof("unary operator %s cancelling....", o.name)
			o.mutex.Lock()
			cancel()
			o.cancelled = true
			o.mutex.Unlock()
		}
		return
	}
	o.mutex.Lock()
	o.statManagers = append(o.statManagers, stats)
	o.mutex.Unlock()

	for {
		select {
		// process incoming item
		case item := <-o.input:
			stats.IncTotalRecordsIn()
			stats.ProcessTimeStart()
			result := o.op.Apply(exeCtx, item)

			switch val := result.(type) {
			case nil:
				continue
			case error: //TODO error handling
				logger.Infoln(val)
				logger.Infoln(val.Error())
				stats.IncTotalExceptions()
				continue
			default:
				stats.ProcessTimeEnd()
				nodes.Broadcast(o.outputs, val, ctx)
				stats.IncTotalRecordsOut()
				stats.SetBufferLength(int64(len(o.input)))
			}
		// is cancelling
		case <-ctx.Done():
			logger.Infof("unary operator %s instance %d cancelling....", o.name, ctx.GetInstanceId())
			o.mutex.Lock()
			cancel()
			o.cancelled = true
			o.mutex.Unlock()
			return
		}
	}
}

func (m *UnaryOperator) GetMetrics() (result [][]interface{}) {
	for _, stats := range m.statManagers {
		result = append(result, stats.GetMetrics())
	}
	return result
}
