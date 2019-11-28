package collectors

import (
	"engine/xstream/api"
	"errors"
)

// CollectorFunc is a function used to colllect
// incoming stream data. It can be used as a
// stream sink.
type CollectorFunc func(api.StreamContext, interface{}) error

// FuncCollector is a colletor that uses a function
// to collect data.  The specified function must be
// of type:
//   CollectorFunc
type FuncCollector struct {
	f     CollectorFunc
}

// Func creates a new value *FuncCollector that
// will use the specified function parameter to
// collect streaming data.
func Func(f CollectorFunc) *FuncCollector {
	return &FuncCollector{f: f}
}

func (c *FuncCollector) Configure(props map[string]interface{}) error{
	//do nothing
	return nil
}

// Open is the starting point that starts the collector
func (c *FuncCollector) Open(ctx api.StreamContext) error {
	log := ctx.GetLogger()
	log.Infoln("Opening func collector")

	if c.f == nil {
		return errors.New("func collector missing function")
	}
	return nil
}

func (c *FuncCollector) Collect(ctx api.StreamContext, item interface{}) error {
	return c.f(ctx, item)
}

func (c *FuncCollector) Close(api.StreamContext) error {
	return nil
}