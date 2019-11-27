package nodes

import (
	"github.com/emqx/kuiper/xstream/api"
)

func Broadcast(outputs map[string]chan<- interface{}, val interface{}, ctx api.StreamContext) int {
	count := 0
	logger := ctx.GetLogger()
	for n, out := range outputs {
		select {
		case out <- val:
			count++
		default: //TODO channel full strategy?
			logger.Errorf("send output from %s to %s fail: channel full", ctx.GetOpId(), n)
		}
	}
	return count
}


