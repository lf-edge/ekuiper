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
	"sync"
	"testing"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/topo/checkpoint"
	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
)

// recordingResponder records checkpoint triggers in call order.
type recordingResponder struct {
	name  string
	mu    sync.Mutex
	fired []int64
}

func (r *recordingResponder) TriggerCheckpoint(id int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fired = append(r.fired, id)
	return nil
}

func (r *recordingResponder) GetName() string {
	return r.name
}

func (r *recordingResponder) triggers() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int64(nil), r.fired...)
}

// abortDriver mimics the operator loop: fresh items go through Process,
// due items are drained via NextDue and fed back through Process, which is
// where rows reach the operator and markers fire. Every step is
// single-threaded and fully deterministic.
type abortDriver struct {
	t       *testing.T
	aligner *checkpoint.BarrierAligner
	ctx     api.StreamContext
	rows    []*checkpoint.BufferOrEvent
}

func (d *abortDriver) feed(item *checkpoint.BufferOrEvent) bool {
	d.t.Helper()
	return d.aligner.Process(item, d.ctx)
}

// drainNext serves one due item and records rows in drain order. It returns
// false when nothing is due.
func (d *abortDriver) drainNext() bool {
	d.t.Helper()
	due, ok := d.aligner.NextDue()
	if !ok {
		return false
	}
	if _, isBarrier := due.Data.(*checkpoint.Barrier); !isBarrier {
		if _, isRow := due.Data.(string); isRow {
			d.rows = append(d.rows, due)
		}
	}
	if consumed := d.aligner.Process(due, d.ctx); consumed && len(d.rows) > 0 && d.rows[len(d.rows)-1] == due {
		// Row mistakenly consumed: rows must flow to the operator.
		d.t.Fatalf("due row was consumed instead of flowing: %#v", due)
	}
	return true
}

func barrier(channel string, id int64) *checkpoint.BufferOrEvent {
	return &checkpoint.BufferOrEvent{Channel: channel, Data: &checkpoint.Barrier{CheckpointId: id, OpId: channel}}
}

func row(channel, data string) *checkpoint.BufferOrEvent {
	return &checkpoint.BufferOrEvent{Channel: channel, Data: data}
}

// TestBarrierAlignerReplaysBufferBeforeNewSnapshot covers abort ordering: a
// row buffered while aligning C1 must be processed before the C2 snapshot
// fires after C2 preempts the alignment.
func TestBarrierAlignerReplaysBufferBeforeNewSnapshot(t *testing.T) {
	responder := &recordingResponder{name: "window"}
	aligner := checkpoint.NewBarrierAligner(responder, 2)
	ctx := topoContext.Background()
	d := &abortDriver{t: t, aligner: aligner, ctx: ctx}

	buffered := row("left", "row")
	if !d.feed(barrier("left", 1)) {
		t.Fatal("barrier was not consumed")
	}
	if !d.feed(buffered) {
		t.Fatal("row from an aligning input was not buffered")
	}
	// C2 arrives from the right while C1 is still aligning: abort C1 and
	// defer C2. Nothing may trigger yet.
	if !d.feed(barrier("right", 2)) {
		t.Fatal("preempting barrier was not consumed")
	}
	if got := responder.triggers(); len(got) != 0 {
		t.Fatalf("trigger fired before the backlog drained: %v", got)
	}
	// The left C2 barrier completes the new alignment. The trigger still
	// must wait for the old row.
	if !d.feed(barrier("left", 2)) {
		t.Fatal("completing barrier was not consumed")
	}
	if got := responder.triggers(); len(got) != 0 {
		t.Fatalf("trigger fired before the old row drained: %v", got)
	}
	// Drain: the old row first, then the positioned trigger.
	if !d.drainNext() {
		t.Fatal("old row was never due")
	}
	if len(d.rows) != 1 || d.rows[0] != buffered {
		t.Fatalf("old row was not replayed first: %#v", d.rows)
	}
	if got := responder.triggers(); len(got) != 0 {
		t.Fatalf("trigger fired before the old row was processed: %v", got)
	}
	for d.drainNext() {
	}
	if got := responder.triggers(); len(got) != 1 || got[0] != 2 {
		t.Fatalf("expected exactly trigger 2 after replay, got %v", got)
	}
	if _, ok := aligner.NextDue(); ok {
		t.Fatal("unexpected backlog after the aborted alignment completed")
	}
}

// TestBarrierAlignerReplaysBufferOnSuccessfulAlignment locks in the normal
// path: rows buffered during alignment belong to the post-barrier epoch, so
// the trigger fires first and the rows drain after it.
func TestBarrierAlignerReplaysBufferOnSuccessfulAlignment(t *testing.T) {
	responder := &recordingResponder{name: "window"}
	aligner := checkpoint.NewBarrierAligner(responder, 2)
	ctx := topoContext.Background()
	d := &abortDriver{t: t, aligner: aligner, ctx: ctx}

	buffered := row("left", "row")
	if !d.feed(barrier("left", 1)) {
		t.Fatal("barrier was not consumed")
	}
	if !d.feed(buffered) {
		t.Fatal("row from an aligning input was not buffered")
	}
	if !d.feed(barrier("right", 1)) {
		t.Fatal("completing barrier was not consumed")
	}
	// No ordinary row precedes the trigger: it fires synchronously.
	if got := responder.triggers(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("expected immediate trigger 1, got %v", got)
	}
	if !d.drainNext() {
		t.Fatal("buffered row was never due after successful alignment")
	}
	if len(d.rows) != 1 || d.rows[0] != buffered {
		t.Fatalf("buffered row was not replayed: %#v", d.rows)
	}
	if _, ok := aligner.NextDue(); ok {
		t.Fatal("unexpected backlog after draining")
	}
	// The stream continues: fresh rows flow without queueing.
	if d.feed(row("left", "next-row")) {
		t.Fatal("fresh row was queued after the alignment completed")
	}
}
