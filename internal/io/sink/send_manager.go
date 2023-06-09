// Copyright 2023 EMQ Technologies Co., Ltd.
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
	"context"
	"fmt"

	"github.com/lf-edge/ekuiper/internal/conf"
)

type SendManager struct {
	lingerInterval int
	batchSize      int
	bufferCh       chan map[string]interface{}
	buffer         []map[string]interface{}
	outputCh       chan []map[string]interface{}
	currIndex      int
	finished       bool
}

func NewSendManager(batchSize, lingerInterval int) (*SendManager, error) {
	if batchSize < 1 && lingerInterval < 1 {
		return nil, fmt.Errorf("either batchSize or lingerInterval should be larger than 0")
	}
	sm := &SendManager{
		batchSize:      batchSize,
		lingerInterval: lingerInterval,
	}
	if batchSize == 0 {
		batchSize = 1024
	}
	sm.buffer = make([]map[string]interface{}, batchSize)
	sm.bufferCh = make(chan map[string]interface{})
	sm.outputCh = make(chan []map[string]interface{}, 16)
	return sm, nil
}

func (sm *SendManager) RecvData(d map[string]interface{}) {
	sm.bufferCh <- d
}

func (sm *SendManager) Run(ctx context.Context) {
	defer sm.finish()
	switch {
	case sm.batchSize > 0 && sm.lingerInterval > 0:
		sm.runWithTickerAndBatchSize(ctx)
	case sm.batchSize > 0 && sm.lingerInterval == 0:
		sm.runWithBatchSize(ctx)
	case sm.batchSize == 0 && sm.lingerInterval > 0:
		sm.runWithTicker(ctx)
	}
}

func (sm *SendManager) runWithTicker(ctx context.Context) {
	ticker := conf.GetTicker(int64(sm.lingerInterval))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case d := <-sm.bufferCh:
			sm.appendDataInBuffer(d, false)
		case <-ticker.C:
			sm.send()
		}
	}
}

func (sm *SendManager) runWithBatchSize(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case d := <-sm.bufferCh:
			sm.appendDataInBuffer(d, true)
		}
	}
}

func (sm *SendManager) runWithTickerAndBatchSize(ctx context.Context) {
	ticker := conf.GetTicker(int64(sm.lingerInterval))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case d := <-sm.bufferCh:
			sm.appendDataInBuffer(d, true)
		case <-ticker.C:
			sm.send()
		}
	}
}

func (sm *SendManager) send() {
	if sm.currIndex < 1 {
		return
	}
	list := make([]map[string]interface{}, sm.currIndex)
	for i := 0; i < sm.currIndex; i++ {
		list[i] = sm.buffer[i]
	}
	sm.currIndex = 0
	sm.outputCh <- list
}

func (sm *SendManager) appendDataInBuffer(d map[string]interface{}, sendData bool) {
	if sm.currIndex >= len(sm.buffer) {
		// The buffer should be enlarged if the data length is larger than capacity during runWithTicker
		sm.buffer = append(sm.buffer, d)
	} else {
		sm.buffer[sm.currIndex] = d
	}
	sm.currIndex++
	if sendData && sm.currIndex >= sm.batchSize {
		sm.send()
	}
}

func (sm *SendManager) GetOutputChan() <-chan []map[string]interface{} {
	return sm.outputCh
}

func (sm *SendManager) finish() {
	sm.finished = true
}
