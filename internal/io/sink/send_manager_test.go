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
			batchSize:      3,
			lingerInterval: 0,
			expectItems:    3,
		},
		{
			batchSize:      10,
			lingerInterval: 100,
			expectItems:    4,
		},
	}
	mc := conf.Clock.(*clock.Mock)
	for _, tc := range testcases {
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
			go func() {
				for i := 0; i < tc.batchSize; i++ {
					sm.RecvData(map[string]interface{}{})
					mc.Add(30 * time.Millisecond)
				}
			}()
			mc.Add(time.Duration(tc.lingerInterval) * time.Millisecond)
			r := <-sm.GetOutputChan()
			if len(r) != tc.expectItems {
				return fmt.Errorf("expect %v output data, actual %v", tc.expectItems, len(r))
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
