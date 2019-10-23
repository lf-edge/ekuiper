package sinks

import (
	"context"
	"engine/common"
	"engine/xstream/collectors"
	"fmt"
	"sync"
	"time"
)

// log action, no properties now
// example: {"log":{}}
func NewLogSink(name string, ruleId string) *collectors.FuncCollector {
	return collectors.Func(name, func(ctx context.Context, data interface{}) error {
		log := common.GetLogger(ctx)
		log.Printf("sink result for rule %s: %s", ruleId, data)
		return nil
	})
}

type QueryResult struct {
	Results []string
	LastFetch time.Time
	Mux sync.Mutex
}

var QR = &QueryResult{LastFetch:time.Now()}

func NewLogSinkToMemory(name string, ruleId string) *collectors.FuncCollector {
	QR.Results = make([]string, 10)
	return collectors.Func(name, func(ctx context.Context, data interface{}) error {
		QR.Mux.Lock()
		QR.Results = append(QR.Results, fmt.Sprintf("%s", data))
		QR.Mux.Unlock()
		return nil
	})
}