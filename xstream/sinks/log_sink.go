package sinks

import (
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/collectors"
	"fmt"
	"sync"
	"time"
)

// log action, no properties now
// example: {"log":{}}
func NewLogSink() *collectors.FuncCollector {
	return collectors.Func(func(ctx api.StreamContext, data interface{}) error {
		log := ctx.GetLogger()
		log.Infof("sink result for rule %s: %s", ctx.GetRuleId(), data)
		return nil
	})
}

type QueryResult struct {
	Results []string
	LastFetch time.Time
	Mux sync.Mutex
}

var QR = &QueryResult{LastFetch:time.Now()}

func NewLogSinkToMemory() *collectors.FuncCollector {
	QR.Results = make([]string, 10)
	return collectors.Func(func(ctx api.StreamContext, data interface{}) error {
		QR.Mux.Lock()
		QR.Results = append(QR.Results, fmt.Sprintf("%s", data))
		QR.Mux.Unlock()
		return nil
	})
}