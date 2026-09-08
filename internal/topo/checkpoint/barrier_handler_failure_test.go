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
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/v2/internal/topo/checkpoint"
	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
)

func TestBarrierAlignerReleasesInputsOnCheckpointFailure(t *testing.T) {
	aligner := checkpoint.NewBarrierAligner(&failingResponder{name: "window"}, 2)
	output := make(chan *checkpoint.BufferOrEvent, 1)
	aligner.SetOutput(output)
	ctx := topoContext.Background()

	aligner.Process(&checkpoint.BufferOrEvent{Channel: "left", Data: &checkpoint.Barrier{CheckpointId: 1, OpId: "left"}}, ctx)
	buffered := &checkpoint.BufferOrEvent{Channel: "left", Data: "row"}
	if !aligner.Process(buffered, ctx) {
		t.Fatal("row from an aligned input was not buffered")
	}
	aligner.Process(&checkpoint.BufferOrEvent{Channel: "right", Data: &checkpoint.Barrier{CheckpointId: 1, OpId: "right"}}, ctx)

	if aligner.Process(&checkpoint.BufferOrEvent{Channel: "left", Data: "next-row"}, ctx) {
		t.Fatal("input remained blocked after checkpoint failure")
	}
	select {
	case got := <-output:
		if got != buffered {
			t.Fatalf("unexpected replayed row: %#v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("buffered row was not replayed after checkpoint failure")
	}
}

type failingResponder struct {
	name string
}

func (r *failingResponder) TriggerCheckpoint(int64) error {
	return errors.New("injected checkpoint failure")
}

func (r *failingResponder) GetName() string {
	return r.name
}
