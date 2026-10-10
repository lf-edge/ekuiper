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
	"testing"

	"github.com/lf-edge/ekuiper/v2/internal/topo/checkpoint"
	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
)

// TestBarrierAlignerOrdersPreRowsBeforePostRows proves the stable partition:
// rows that arrived before their channel's barrier drain before the trigger,
// while rows that arrived after it drain after, even when interleaved.
func TestBarrierAlignerOrdersPreRowsBeforePostRows(t *testing.T) {
	responder := &recordingResponder{name: "window"}
	aligner := checkpoint.NewBarrierAligner(responder, 2)
	ctx := topoContext.Background()
	d := &abortDriver{t: t, aligner: aligner, ctx: ctx}

	pre := row("right", "pre")
	post := row("left", "post")
	if !d.feed(barrier("left", 1)) {
		t.Fatal("barrier was not consumed")
	}
	if !d.feed(post) {
		t.Fatal("post-barrier row was not buffered")
	}
	if !d.feed(pre) {
		t.Fatal("pre-barrier row was not buffered")
	}
	if !d.feed(barrier("right", 1)) {
		t.Fatal("completing barrier was not consumed")
	}
	// The pre row precedes the trigger but the post row must not: no
	// immediate trigger is allowed here.
	if got := responder.triggers(); len(got) != 0 {
		t.Fatalf("trigger fired before the pre row drained: %v", got)
	}
	for d.drainNext() {
	}
	if len(d.rows) != 2 || d.rows[0] != pre || d.rows[1] != post {
		t.Fatalf("rows drained out of order: %#v", d.rows)
	}
	if got := responder.triggers(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("expected trigger 1 between the rows, got %v", got)
	}
}

// TestBarrierAlignerNestedPreemption covers C3 arriving while C2 is still
// deferring: the C1 row must precede the C3 trigger, and exactly one
// trigger may fire.
func TestBarrierAlignerNestedPreemption(t *testing.T) {
	responder := &recordingResponder{name: "window"}
	aligner := checkpoint.NewBarrierAligner(responder, 2)
	ctx := topoContext.Background()
	d := &abortDriver{t: t, aligner: aligner, ctx: ctx}

	old := row("left", "old")
	if !d.feed(barrier("left", 1)) {
		t.Fatal("barrier was not consumed")
	}
	if !d.feed(old) {
		t.Fatal("row was not buffered")
	}
	if !d.feed(barrier("right", 2)) {
		t.Fatal("preempting barrier was not consumed")
	}
	if !d.feed(barrier("left", 3)) {
		t.Fatal("nested barrier was not consumed")
	}
	if !d.feed(barrier("right", 3)) {
		t.Fatal("completing barrier was not consumed")
	}
	for d.drainNext() {
	}
	if len(d.rows) != 1 || d.rows[0] != old {
		t.Fatalf("old row was lost or duplicated: %#v", d.rows)
	}
	if got := responder.triggers(); len(got) != 1 || got[0] != 3 {
		t.Fatalf("expected exactly trigger 3, got %v", got)
	}
	if _, ok := aligner.NextDue(); ok {
		t.Fatal("unexpected backlog after nested preemption completed")
	}
}

// TestBarrierAlignerDropsStaleBarriers ensures duplicates and older epochs
// can never trigger again once a newer trigger fired.
func TestBarrierAlignerDropsStaleBarriers(t *testing.T) {
	responder := &recordingResponder{name: "window"}
	aligner := checkpoint.NewBarrierAligner(responder, 2)
	ctx := topoContext.Background()
	d := &abortDriver{t: t, aligner: aligner, ctx: ctx}

	if !d.feed(barrier("left", 1)) {
		t.Fatal("barrier was not consumed")
	}
	if !d.feed(barrier("right", 1)) {
		t.Fatal("completing barrier was not consumed")
	}
	if got := responder.triggers(); len(got) != 1 {
		t.Fatalf("expected immediate trigger 1, got %v", got)
	}
	// Duplicate and older barriers are stale.
	if !d.feed(barrier("left", 1)) {
		t.Fatal("stale barrier was not consumed")
	}
	if !d.feed(barrier("right", 0)) {
		t.Fatal("older barrier was not consumed")
	}
	for d.drainNext() {
	}
	if got := responder.triggers(); len(got) != 1 {
		t.Fatalf("stale barrier triggered again: %v", got)
	}
}
