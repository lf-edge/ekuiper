package sinks

import (
	"fmt"
	"github.com/emqx/kuiper/internal/topo/collectors"
	"github.com/emqx/kuiper/pkg/api"
	"sync"
	"time"
)

// NewLogSink log action, no properties now
// example: {"log":{}}
func NewLogSink() *collectors.FuncCollector {
	return collectors.Func(func(ctx api.StreamContext, data interface{}) error {
		log := ctx.GetLogger()
		log.Infof("sink result for rule %s: %s", ctx.GetRuleId(), data)
		return nil
	})
}

type QueryResult struct {
	Results   []string
	LastFetch time.Time
	Mux       sync.Mutex
}

var QR = &QueryResult{LastFetch: time.Now()}

func NewLogSinkToMemory() *collectors.FuncCollector {
	QR.Results = make([]string, 10)
	return collectors.Func(func(ctx api.StreamContext, data interface{}) error {
		QR.Mux.Lock()
		QR.Results = append(QR.Results, fmt.Sprintf("%s", data))
		QR.Mux.Unlock()
		return nil
	})
}
