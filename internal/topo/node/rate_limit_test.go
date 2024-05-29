// Copyright 2024 EMQ Technologies Co., Ltd.
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

package node

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/topotest/mockclock"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestNewRateLimit(t *testing.T) {
	_, err := NewRateLimitOp("test", &def.RuleOption{BufferLength: 10, SendError: true}, 1*time.Second)
	assert.NoError(t, err)
	_, err = NewRateLimitOp("test", &def.RuleOption{BufferLength: 10, SendError: true}, 1*time.Nanosecond)
	assert.Error(t, err)
	assert.EqualError(t, err, "interval should be larger than 1ms")
}

func TestRateLimit(t *testing.T) {
	testcases := []struct {
		name        string
		sendCount   int
		interval    time.Duration
		expectItems []any
	}{
		{ // sending gap is 300ms
			name:      "normal",
			sendCount: 10,
			interval:  time.Second,
			expectItems: []any{
				&xsql.RawTuple{
					Rawdata: []byte{3},
				},
				&xsql.RawTuple{
					Rawdata: []byte{6},
				},
				&xsql.RawTuple{
					Rawdata: []byte{9},
				},
			},
		},
	}
	mc := mockclock.GetMockClock()
	for i, tc := range testcases {
		t.Run(fmt.Sprintf("testcase %d", i), func(t *testing.T) {
			op, err := NewRateLimitOp("test", &def.RuleOption{BufferLength: 10, SendError: true}, tc.interval)
			assert.NoError(t, err)
			out := make(chan any, 10)
			err = op.AddOutput(out, "test")
			assert.NoError(t, err)
			ctx, cancel := mockContext.NewMockContext("test1", "batch_test").WithCancel()
			errCh := make(chan error)
			op.Exec(ctx, errCh)
			for i := 0; i < tc.sendCount; i++ {
				op.input <- &xsql.RawTuple{
					Rawdata: []byte{uint8(i)},
				}
				mc.Add(300 * time.Millisecond)
			}
			cancel()
			// make sure op has done all sending
			for {
				processed := op.statManager.GetMetrics()[2]
				if processed == int64(tc.sendCount) {
					close(out)
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			r := make([]any, 0, len(tc.expectItems))
			for ele := range out {
				r = append(r, ele)
			}
			assert.Equal(t, tc.expectItems, r)
		})
	}
}
