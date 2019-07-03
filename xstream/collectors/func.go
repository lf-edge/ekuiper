package collectors

import (
	"context"
	"engine/common"
	"errors"
)

var log = common.Log
// CollectorFunc is a function used to colllect
// incoming stream data. It can be used as a
// stream sink.
type CollectorFunc func(interface{}) error

// FuncCollector is a colletor that uses a function
// to collect data.  The specified function must be
// of type:
//   CollectorFunc
type FuncCollector struct {
	input chan interface{}
	//logf  api.LogFunc
	//errf  api.ErrorFunc
	f     CollectorFunc
	name  string
}

// Func creates a new value *FuncCollector that
// will use the specified function parameter to
// collect streaming data.
func Func(f CollectorFunc) *FuncCollector {
	return &FuncCollector{f: f, input: make(chan interface{}, 1024)}
}

func (c *FuncCollector) GetName() string  {
	return c.name
}

func (c *FuncCollector) GetInput() (chan<- interface{}, string)  {
	return c.input, c.name
}

// Open is the starting point that starts the collector
func (c *FuncCollector) Open(ctx context.Context) <-chan error {
	//c.logf = autoctx.GetLogFunc(ctx)
	//c.errf = autoctx.GetErrFunc(ctx)

	log.Println("Opening func collector")
	result := make(chan error)

	if c.f == nil {
		err := errors.New("Func collector missing function")
		log.Println(err)
		go func() { result <- err }()
		return result
	}

	go func() {
		defer func() {
			log.Println("Closing func collector")
			close(result)
		}()

		for {
			select {
			case item, opened := <-c.input:
				if !opened {
					return
				}
				if err := c.f(item); err != nil {
					log.Println(err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return result
}
