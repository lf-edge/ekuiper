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

package util

import (
	"context"
	"fmt"
	"time"
)

type SendManager struct {
	lingerInterval int
	batchSize      int
	bufferCh       chan interface{}
	buffer         []interface{}
	outputCh       chan []interface{}
	notifyCh       chan interface{}
}

func NewSendManager(batchSize, lingerInterval int) (*SendManager, error) {
	if batchSize < 1 && lingerInterval < 1 {
		return nil, fmt.Errorf("either batchSize or lingerInterval should be larger than 0")
	}
	sm := &SendManager{
		batchSize:      batchSize,
		lingerInterval: lingerInterval,
	}
	sm.buffer = make([]interface{}, 0, batchSize)
	sm.bufferCh = make(chan interface{})
	sm.outputCh = make(chan []interface{}, 16)
	sm.notifyCh = make(chan interface{}, 16)
	return sm, nil
}

func (sm *SendManager) RecvData(d interface{}) {
	sm.bufferCh <- d
}

func (sm *SendManager) Run(ctx context.Context) {
	if sm.lingerInterval > 0 {
		sm.runWithTicker(ctx)
	} else {
		sm.runWithoutTicker(ctx)
	}
}

func (sm *SendManager) runWithoutTicker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case d := <-sm.bufferCh:
			sm.buffer = append(sm.buffer, d)
			if len(sm.buffer) >= sm.batchSize {
				sm.send()
			}
		}
	}
}

func (sm *SendManager) runWithTicker(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(sm.lingerInterval) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case d := <-sm.bufferCh:
			sm.buffer = append(sm.buffer, d)
			if len(sm.buffer) >= sm.batchSize {
				sm.send()
			}
		case <-ticker.C:
			sm.send()
		}
	}
}

func (sm *SendManager) send() {
	if len(sm.buffer) < 1 {
		return
	}
	list := make([]interface{}, len(sm.buffer))
	for i, item := range sm.buffer {
		list[i] = item
	}
	sm.buffer = make([]interface{}, 0, sm.batchSize)
	sm.outputCh <- list
}

func (sm *SendManager) GetOutputChan() <-chan []interface{} {
	return sm.outputCh
}

func (sm *SendManager) SendNotify(notify interface{}) {
	sm.notifyCh <- notify
}

func (sm *SendManager) GetNotifyChan() <-chan interface{} {
	return sm.notifyCh
}
