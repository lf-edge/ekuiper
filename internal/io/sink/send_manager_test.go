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
	"testing"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/lf-edge/ekuiper/internal/conf"
)

func TestSendManager(t *testing.T) {
	testcases := []struct {
		sendCount      int
		batchSize      int
		lingerInterval int
		err            string
		expectItems    int
	}{
		{
			batchSize:      0,
			lingerInterval: 0,
			err:            "either batchSize or lingerInterval should be larger than 0",
		},
		{
			sendCount:      3,
			batchSize:      3,
			lingerInterval: 0,
			expectItems:    3,
		},
		{
			sendCount:      4,
			batchSize:      10,
			lingerInterval: 100,
			expectItems:    4,
		},
		{
			sendCount:      4,
			batchSize:      0,
			lingerInterval: 100,
			expectItems:    4,
		},
	}
	mc := conf.Clock.(*clock.Mock)
	for i, tc := range testcases {
		mc.Set(mc.Now())
		testF := func() error {
			sm, err := NewSendManager(tc.batchSize, tc.lingerInterval)
			if len(tc.err) > 0 {
				if err == nil || err.Error() != tc.err {
					return fmt.Errorf("expect err:%v, actual: %v", tc.err, err)
				}
				return nil
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go sm.Run(ctx)
			for i := 0; i < tc.sendCount; i++ {
				sm.RecvData(map[string]interface{}{})
				mc.Add(30 * time.Millisecond)
			}
			r := <-sm.GetOutputChan()
			if len(r) != tc.expectItems {
				return fmt.Errorf("testcase %v expect %v output data, actual %v", i, tc.expectItems, len(r))
			}
			return nil
		}
		if err := testF(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSendEmpty(t *testing.T) {
	sm, err := NewSendManager(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	sm.outputCh = make(chan []map[string]interface{})
	// test shouldn't be blocked
	sm.send()
}

func TestCancelRun(t *testing.T) {
	testcases := []struct {
		batchSize      int
		lingerInterval int
	}{
		{
			batchSize:      0,
			lingerInterval: 1,
		},
		{
			batchSize:      3,
			lingerInterval: 0,
		},
		{
			batchSize:      10,
			lingerInterval: 100,
		},
	}
	for _, tc := range testcases {
		sm, err := NewSendManager(tc.batchSize, tc.lingerInterval)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		c := make(chan struct{})
		go func() {
			sm.Run(ctx)
			c <- struct{}{}
		}()
		cancel()
		<-c
		if !sm.finished {
			t.Fatal("send manager should be finished")
		}
	}
}

func TestEnlargeSendManagerCap(t *testing.T) {
	sm, err := NewSendManager(0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	count := 1025
	for i := 0; i < count; i++ {
		go sm.RecvData(map[string]interface{}{})
		sm.appendDataInBuffer(<-sm.bufferCh, false)
	}
	if len(sm.buffer) != count {
		t.Fatal(fmt.Sprintf("sm buffer should be %v", count))
	}
	if sm.currIndex != count {
		t.Fatal(fmt.Sprintf("sm index should be %v", count))
	}
	originCap := cap(sm.buffer)
	originLen := len(sm.buffer)
	sm.send()
	if sm.currIndex != 0 || originCap != cap(sm.buffer) || originLen != len(sm.buffer) {
		t.Fatal("sm buffer capacity shouldn't be changed after send")
	}
}
