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

	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
)

// BenchmarkWindowV2InitialStatePublication measures the one-time context
// publication done when an operator starts or restores. Tuple processing
// mutates this published state in place and does not call PutState.
func BenchmarkWindowV2InitialStatePublication(b *testing.B) {
	ctx := topoContext.Background().WithMeta("benchmark", "window", &state.MemoryStore{})
	windowState := &SlidingWindowV2State{
		Scanner: &WindowScanner{},
	}
	if err := ctx.PutState(V2WindowInputsKey, windowState); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := ctx.PutState(V2WindowInputsKey, windowState); err != nil {
			b.Fatal(err)
		}
	}
}
