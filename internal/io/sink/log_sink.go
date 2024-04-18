// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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
	"sync"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/topo/collector"
)

// NewLogSink log action, no properties now
// example: {"log":{}}
func NewLogSink() api.Sink {
	return collector.Func(func(ctx api.StreamContext, data any) error {
		ctx.GetLogger().Infof("sink result for rule %s: %s", ctx.GetRuleId(), data)
		return nil
	})
}

type QueryResult struct {
	Results   []string
	LastFetch time.Time
	Mux       sync.Mutex
}

var QR = &QueryResult{LastFetch: time.Now()}

func NewLogSinkToMemory() api.Sink {
	QR.Results = make([]string, 0, 10)
	return collector.Func(func(ctx api.StreamContext, data any) error {
		result, ok := data.(string)
		if !ok {
			return fmt.Errorf("result is not a string but got %v", data)
		}
		QR.Mux.Lock()
		QR.Results = append(QR.Results, result)
		QR.Mux.Unlock()
		return nil
	})
}
