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

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	mockContext "github.com/lf-edge/ekuiper/pkg/mock/context"
)

func TestRun(t *testing.T) {
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
		{
			sendCount:      6,
			batchSize:      3,
			lingerInterval: 3000,
			expectItems:    3,
		},
	}
	mc := conf.Clock.(*clock.Mock)
	for i, tc := range testcases {
		mc.Set(mc.Now())
		t.Run(fmt.Sprintf("testcase %d", i), func(t *testing.T) {
			op, err := NewBatchOp("test", &api.RuleOption{BufferLength: 10, SendError: true}, tc.batchSize, tc.lingerInterval)
			if len(tc.err) > 0 {
				assert.Error(t, err)
				assert.Equal(t, tc.err, err.Error())
				return
			}
			assert.NoError(t, err)
			out := make(chan any, 100)
			err = op.AddOutput(out, "test")
			assert.NoError(t, err)
			ctx := mockContext.NewMockContext("test1", "batch_test")
			errCh := make(chan error)
			op.Exec(ctx, errCh)
			for i := 0; i < tc.sendCount; i++ {
				op.input <- &xsql.Tuple{
					Emitter: "test",
					Message: map[string]any{
						"b": i,
					},
				}
				mc.Add(30 * time.Millisecond)
			}
			r := <-out
			w, ok := r.(*xsql.WindowTuples)
			assert.True(t, ok)
			assert.Equal(t, tc.expectItems, len(w.Content))
		})
	}
}
