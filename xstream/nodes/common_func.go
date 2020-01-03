package nodes

import (
	"github.com/emqx/kuiper/xstream/api"
	"sync"
)

//Blocking broadcast
func Broadcast(outputs map[string]chan<- interface{}, val interface{}, ctx api.StreamContext) {
	logger := ctx.GetLogger()
	var barrier sync.WaitGroup
	barrier.Add(len(outputs))
	for n, out := range outputs {
		go func(wg *sync.WaitGroup){
			out <- val
			wg.Done()
			logger.Debugf("broadcast from %s to %s done", ctx.GetOpId(), n)
		}(&barrier)
	}
	logger.Debugf("broadcasting from %s", ctx.GetOpId())
	barrier.Wait()
}


