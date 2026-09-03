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
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
)

func TestResponderSendsACKAndHoldsGuard(t *testing.T) {
	ctx := topoContext.Background().WithMeta("rule", "source", &state.MemoryStore{})
	task := &guardedTask{name: "source", ctx: ctx}
	signals := make(chan *checkpoint.Signal, 1)
	responder := checkpoint.NewResponderExecutor(signals, task)

	if err := responder.TriggerCheckpoint(1); err != nil {
		t.Fatal(err)
	}
	signal := receiveSignal(t, signals)
	if signal.Message != checkpoint.ACK || signal.CheckpointId != 1 || signal.OpId != "source" {
		t.Fatalf("unexpected signal: %#v", signal)
	}
	task.mu.Lock()
	defer task.mu.Unlock()
	if task.guardDepth != 0 {
		t.Fatalf("checkpoint guard was not released: depth %d", task.guardDepth)
	}
	if len(task.broadcasts) != 1 {
		t.Fatalf("barrier count mismatch: %d", len(task.broadcasts))
	}
}

func TestResponderSendsDECOnCaptureFailure(t *testing.T) {
	ctx := topoContext.Background().WithMeta("rule", "op", &state.MemoryStore{})
	if err := ctx.PutState("unsupported", make(chan int)); err != nil {
		t.Fatal(err)
	}
	task := &guardedTask{name: "op", ctx: ctx}
	signals := make(chan *checkpoint.Signal, 1)
	responder := checkpoint.NewResponderExecutor(signals, task)

	if err := responder.TriggerCheckpoint(2); err == nil {
		t.Fatal("capture failure must be returned")
	}
	signal := receiveSignal(t, signals)
	if signal.Message != checkpoint.DEC || signal.CheckpointId != 2 || signal.OpId != "op" {
		t.Fatalf("unexpected signal: %#v", signal)
	}
	task.mu.Lock()
	defer task.mu.Unlock()
	if task.guardDepth != 0 {
		t.Fatalf("checkpoint guard was not released: depth %d", task.guardDepth)
	}
}

func TestResponderSendsDECAfterBarrierOnGuardError(t *testing.T) {
	ctx := topoContext.Background().WithMeta("rule", "source", &state.MemoryStore{})
	task := &guardedTask{
		name:          "source",
		ctx:           ctx,
		checkpointErr: errors.New("offset unavailable"),
	}
	signals := make(chan *checkpoint.Signal, 1)
	responder := checkpoint.NewResponderExecutor(signals, task)

	if err := responder.TriggerCheckpoint(4); err == nil {
		t.Fatal("checkpoint guard error must be returned")
	}
	signal := receiveSignal(t, signals)
	if signal.Message != checkpoint.DEC || signal.CheckpointId != 4 || signal.OpId != "source" {
		t.Fatalf("unexpected signal: %#v", signal)
	}
	task.mu.Lock()
	defer task.mu.Unlock()
	if task.guardDepth != 0 {
		t.Fatalf("checkpoint guard was not released: depth %d", task.guardDepth)
	}
	if len(task.broadcasts) != 1 {
		t.Fatalf("barrier must propagate so downstream tasks can terminate the checkpoint: %#v", task.broadcasts)
	}
}

func TestResponderSendsDECAfterBarrierOnInvalidContext(t *testing.T) {
	baseCtx := topoContext.Background().WithMeta("rule", "source", &state.MemoryStore{})
	task := &guardedTask{
		name: "source",
		ctx:  &nonCheckpointContext{StreamContext: baseCtx},
	}
	signals := make(chan *checkpoint.Signal, 1)
	responder := checkpoint.NewResponderExecutor(signals, task)

	if err := responder.TriggerCheckpoint(5); err == nil {
		t.Fatal("invalid checkpoint context must be returned")
	}
	signal := receiveSignal(t, signals)
	if signal.Message != checkpoint.DEC || signal.CheckpointId != 5 || signal.OpId != "source" {
		t.Fatalf("unexpected signal: %#v", signal)
	}
	task.mu.Lock()
	defer task.mu.Unlock()
	if task.guardDepth != 0 {
		t.Fatalf("checkpoint guard was not released: depth %d", task.guardDepth)
	}
	if len(task.broadcasts) != 1 {
		t.Fatalf("barrier must propagate so downstream tasks can terminate the checkpoint: %#v", task.broadcasts)
	}
}

func TestResponderSendsDECOnSaveFailure(t *testing.T) {
	ctx := topoContext.Background().WithMeta("rule", "op", &failingFrozenStore{})
	task := &guardedTask{name: "op", ctx: ctx}
	signals := make(chan *checkpoint.Signal, 1)
	responder := checkpoint.NewResponderExecutor(signals, task)

	if err := responder.TriggerCheckpoint(3); err != nil {
		t.Fatal(err)
	}
	signal := receiveSignal(t, signals)
	if signal.Message != checkpoint.DEC || signal.CheckpointId != 3 || signal.OpId != "op" {
		t.Fatalf("unexpected signal: %#v", signal)
	}
}

func receiveSignal(t *testing.T, signals <-chan *checkpoint.Signal) *checkpoint.Signal {
	t.Helper()
	select {
	case signal := <-signals:
		return signal
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for checkpoint signal")
		return nil
	}
}

type guardedTask struct {
	name string
	ctx  api.StreamContext

	mu         sync.Mutex
	guardDepth int
	broadcasts []any

	checkpointErr error
}

type failingFrozenStore struct {
	state.MemoryStore
}

type nonCheckpointContext struct {
	api.StreamContext
}

func (s *failingFrozenStore) SaveFrozenState(_ int64, _ string, _ []byte) error {
	return errors.New("injected frozen state failure")
}

func (t *guardedTask) GetName() string {
	return t.name
}

func (t *guardedTask) GetStreamContext() api.StreamContext {
	return t.ctx
}

func (t *guardedTask) SetQos(_ def.Qos) {}

func (t *guardedTask) LockCheckpoint() {
	t.mu.Lock()
	t.guardDepth++
	t.mu.Unlock()
}

func (t *guardedTask) UnlockCheckpoint() {
	t.mu.Lock()
	t.guardDepth--
	t.mu.Unlock()
}

func (t *guardedTask) CheckpointError() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.checkpointErr
}

func (t *guardedTask) Broadcast(data any) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.guardDepth != 1 {
		panic("broadcast outside checkpoint guard")
	}
	t.broadcasts = append(t.broadcasts, data)
}
