// Copyright 2024-2026 EMQ Technologies Co., Ltd.
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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/checkpoint"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestOutputs(t *testing.T) {
	n := newDefaultNode("test", &def.RuleOption{})
	err := n.AddOutput(make(chan any), "rule.1_test")
	assert.NoError(t, err)
	err = n.AddOutput(make(chan any), "rule.2_test")
	assert.NoError(t, err)
	err = n.RemoveOutput("rule.1")
	assert.NoError(t, err)
	err = n.RemoveOutput("rule.4")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(n.outputs))
	assert.Len(t, n.outputSlice, 1)
	assert.Equal(t, "rule.2_test", n.outputSlice[0].name)
}

func TestMultipleOutputsBroadcast(t *testing.T) {
	ctx := mockContext.NewMockContext("multi", "op1")
	n := newDefaultNode("test", &def.RuleOption{})
	n.ctx = ctx
	output1 := make(chan any, 10)
	output2 := make(chan any, 10)
	err := n.AddOutput(output1, "rule.1_test")
	require.NoError(t, err)
	err = n.AddOutput(output2, "rule.2_test")
	require.NoError(t, err)
	tc := []struct {
		name string
		data any
	}{
		{
			name: "row broadcast",
			data: &xsql.Tuple{
				Ctx:       nil,
				Emitter:   "test",
				Message:   map[string]any{"a": 20},
				Timestamp: time.UnixMilli(123456789),
			},
		},
		{
			name: "collection broadcast",
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{
						Ctx:       nil,
						Emitter:   "test",
						Message:   map[string]any{"a": 30},
						Timestamp: time.UnixMilli(123456789),
					},
				},
			},
		},
		{
			name: "buffer data",
			data: &checkpoint.BufferOrEvent{
				Data: &xsql.Tuple{
					Emitter:   "test2",
					Message:   map[string]any{"a": 40},
					Timestamp: time.UnixMilli(123456789),
				},
				Channel: "test2",
			},
		},
	}
	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			var result1, result2 any
			wg := &sync.WaitGroup{}
			wg.Add(3)
			go func() {
				defer wg.Done()
				n.Broadcast(tt.data)
			}()
			go func() {
				defer wg.Done()
				result1 = <-output1
			}()
			go func() {
				defer wg.Done()
				result2 = <-output2
			}()
			wg.Wait()
			assert.False(t, result1 == result2)
			assert.Equal(t, tt.data, result1)
			assert.Equal(t, tt.data, result2)
		})
	}
}

func BenchmarkBroadcastOutputs(b *testing.B) {
	for _, outputCount := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("outputs_%d", outputCount), func(b *testing.B) {
			ctx := mockContext.NewMockContext("benchmark", "broadcast")
			n := newDefaultNode("test", &def.RuleOption{})
			n.ctx = ctx
			outputs := make([]chan any, outputCount)
			for i := range outputs {
				outputs[i] = make(chan any, 1)
				require.NoError(b, n.AddOutput(outputs[i], fmt.Sprintf("output_%d", i)))
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				n.Broadcast(i)
				for _, output := range outputs {
					<-output
				}
			}
		})
	}
}
