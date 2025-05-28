// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/maps"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func TestCacheRun(t *testing.T) {
	testx.InitEnv("cacheOp")
	deleteCachedb()
	timex.Set(0)
	// prepare data
	tuples := make([]any, 20)
	for i := 0; i < 20; i++ {
		tuples[i] = &xsql.Tuple{
			Emitter:   "test",
			Timestamp: time.UnixMilli(int64(i)),
			Message:   map[string]any{"key": "value"},
			Metadata:  map[string]any{"topic": "demo"},
		}
	}

	tests := []struct {
		name         string
		sendUntil    int
		receiveCount int
		lastReceive  any
	}{
		{ // 0
			name:         "in channel",
			sendUntil:    2,
			receiveCount: 1,
			lastReceive:  tuples[0],
		},
		{ // 1
			name:         "in disk buffer",
			sendUntil:    5,
			receiveCount: 3,
			lastReceive:  tuples[3],
		},
		{
			name:         "disk overflow",
			sendUntil:    19,
			receiveCount: 2,
			lastReceive:  tuples[5],
		},
		{
			name: "pull by time",
			// no send
			sendUntil:    19,
			receiveCount: 1,
			lastReceive:  tuples[6],
		},
		{
			name: "pull by time, receive after dropped tuple",
			// no send
			sendUntil:    19,
			receiveCount: 1,
			lastReceive:  tuples[13],
		},
		{
			name: "receive all",
			// no send
			sendUntil:    19,
			receiveCount: 5,
			lastReceive:  tuples[18],
		},
		{
			name: "send in no buffer",
			// no send
			sendUntil:    20,
			receiveCount: 1,
			lastReceive:  tuples[19],
		},
	}

	ctx := mockContext.NewMockContext("testCache", "op1")
	cacheOp, err := NewCacheOp(ctx, "test", &def.RuleOption{BufferLength: 10, SendError: true}, &model.SinkConf{
		MemoryCacheThreshold: 2,
		MaxDiskCache:         4,
		BufferPageSize:       2,
		EnableCache:          true,
		ResendInterval:       cast.DurationConf(10 * time.Millisecond),
	})
	assert.NoError(t, err)
	// In sink_node planner, set this buffer length
	out := make(chan any, 2)
	err = cacheOp.AddOutput(out, "test")
	assert.NoError(t, err)
	index := 0
	errCh := make(chan error)
	cacheOp.Exec(ctx, errCh)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var r any
			for index < tt.sendUntil {
				cacheOp.input <- tuples[index]
				index++
			}
			timex.Add(100 * time.Millisecond)
			for { // wait until all processed
				processed := cacheOp.statManager.GetMetrics()[2]
				if processed == int64(tt.sendUntil) {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if tt.receiveCount > 0 {
				for j := 0; j < tt.receiveCount-1; j++ {
					a := <-out
					timex.Add(20 * time.Millisecond)
					ctx.GetLogger().Infof("receive %d", a.(*xsql.Tuple).Timestamp)
					if j%2 == 0 { // because channel length is 2, so need to wait for each 2
						time.Sleep(10 * time.Millisecond)
					}
				}
				r = <-out
				ctx.GetLogger().Infof("receive %d", r.(*xsql.Tuple).Timestamp)
			}
			assert.Equal(t, tt.lastReceive, r)
		})
	}
}

func TestRunError(t *testing.T) {
	ctx, cancel := mockContext.NewMockContext("testError", "op1").WithCancel()
	// Test multiple output error
	testx.InitEnv("cacheErr")
	op, err := NewCacheOp(ctx, "test", &def.RuleOption{BufferLength: 10, SendError: true}, &model.SinkConf{
		MemoryCacheThreshold: 2,
		MaxDiskCache:         4,
		BufferPageSize:       2,
		EnableCache:          true,
		ResendInterval:       cast.DurationConf(10 * time.Millisecond),
	})
	assert.NoError(t, err)
	err = op.AddOutput(make(chan any, 2), "output1")
	assert.NoError(t, err)
	err = op.AddOutput(make(chan any, 2), "output2")
	assert.NoError(t, err)
	errCh := make(chan error, 1)
	op.Exec(ctx, errCh)
	err = <-errCh
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "cache op should have only 1 output but got"), err.Error())
	// Test done
	maps.Clear(op.outputs)
	err = op.AddOutput(make(chan any, 2), "output1")
	assert.NoError(t, err)
	op.Exec(ctx, errCh)
	cancel()
	assert.Equal(t, 0, len(errCh))
}

func deleteCachedb() {
	loc, err := conf.GetDataLoc()
	if err != nil {
		fmt.Println(err)
	}
	err = os.RemoveAll(filepath.Join(loc, "cache.db"))
	if err != nil {
		fmt.Println(err)
	}
}
