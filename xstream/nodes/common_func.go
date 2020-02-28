package nodes

import (
	"github.com/emqx/kuiper/xstream/api"
	"sync"
)

//Blocking broadcast
func Broadcast(outputs map[string]chan<- interface{}, val interface{}, ctx api.StreamContext) {
	logger := ctx.GetLogger()
	var wg sync.WaitGroup
	wg.Add(len(outputs))
	for n, out := range outputs {
		go func(output chan<- interface{}) {
			output <- val
			wg.Done()
			logger.Debugf("broadcast from %s to %s done", ctx.GetOpId(), n)
		}(out)
	}
	logger.Debugf("broadcasting from %s", ctx.GetOpId())
	wg.Wait()
}
