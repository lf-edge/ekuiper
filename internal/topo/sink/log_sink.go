// Copyright 2021 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sink

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/topo/collector"
	"github.com/lf-edge/ekuiper/pkg/api"
	"sync"
	"time"
)

// NewLogSink log action, no properties now
// example: {"log":{}}
func NewLogSink() *collector.FuncCollector {
	return collector.Func(func(ctx api.StreamContext, data interface{}) error {
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

func NewLogSinkToMemory() *collector.FuncCollector {
	QR.Results = make([]string, 10)
	return collector.Func(func(ctx api.StreamContext, data interface{}) error {
		QR.Mux.Lock()
		QR.Results = append(QR.Results, fmt.Sprintf("%s", data))
		QR.Mux.Unlock()
		return nil
	})
}
