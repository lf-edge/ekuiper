// Copyright 2026 EMQ Technologies Co., Ltd.
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
	"testing"
	"time"

	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
)

func TestHoppingIncAggOverdueCloseDoesNotEnqueueToFullTaskChannel(t *testing.T) {
	now := time.Now()
	window := &IncAggWindow{StartTime: now.Add(-2 * time.Second)}
	op := &HoppingWindowIncAggOp{
		Length: time.Second,
		taskCh: make(chan *IncAggOpTask, 1),
		HoppingWindowIncAggOpState: HoppingWindowIncAggOpState{
			CurrWindowList: []*IncAggWindow{},
		},
	}
	op.taskCh <- &IncAggOpTask{}
	done := make(chan struct{})
	go func() {
		op.scheduleWindowClose(topoContext.Background(), make(chan error, 1), window, now)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("overdue window close blocked on its own full task channel")
	}
}
