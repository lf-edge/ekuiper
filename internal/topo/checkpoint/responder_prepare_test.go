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

package checkpoint_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/checkpoint"
	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
)

var errInjectedPrepare = errors.New("injected prepare failure")

// failingPrepareTask fails PrepareCheckpoint while recording downstream
// broadcasts, modeling an operator whose state cannot be materialized.
type failingPrepareTask struct {
	ctx       api.StreamContext
	mu        sync.Mutex
	broadcast []any
	snapshots []int64
}

func (t *failingPrepareTask) GetName() string {
	return "window"
}

func (t *failingPrepareTask) GetStreamContext() api.StreamContext {
	return t.ctx
}

func (t *failingPrepareTask) SetQos(_ def.Qos) {}

func (t *failingPrepareTask) PrepareCheckpoint() error {
	return errInjectedPrepare
}

func (t *failingPrepareTask) Broadcast(data any) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.broadcast = append(t.broadcast, data)
}

func (t *failingPrepareTask) Snapshot(checkpointID int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.snapshots = append(t.snapshots, checkpointID)
	return nil
}

func (t *failingPrepareTask) SaveSnapshot(_ int64) error {
	return nil
}

// TestPrepareFailureStillPropagatesBarrier requires scheme A semantics: when
// PrepareCheckpoint fails, the checkpoint barrier is still propagated
// downstream (so downstream alignments can terminate) while DEC is reported
// to the coordinator (so the checkpoint is discarded, never persisted).
func TestPrepareFailureStillPropagatesBarrier(t *testing.T) {
	task := &failingPrepareTask{ctx: topoContext.Background()}
	signals := make(chan *checkpoint.Signal, 4)
	executor := checkpoint.NewResponderExecutor(signals, task)

	err := executor.TriggerCheckpoint(7)
	if !errors.Is(err, errInjectedPrepare) {
		t.Fatalf("expected prepare error, got %v", err)
	}
	select {
	case signal := <-signals:
		if signal.Message != checkpoint.DEC {
			t.Fatalf("expected DEC signal, got %v", signal.Message)
		}
		if signal.CheckpointId != 7 {
			t.Fatalf("unexpected checkpoint id in signal: %d", signal.CheckpointId)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("DEC signal was not reported to the coordinator")
	}
	task.mu.Lock()
	defer task.mu.Unlock()
	found := false
	for _, data := range task.broadcast {
		if barrier, ok := data.(*checkpoint.Barrier); ok && barrier.CheckpointId == 7 {
			found = true
		}
	}
	if !found {
		t.Fatalf("failed checkpoint barrier was not propagated downstream: %#v", task.broadcast)
	}
}
