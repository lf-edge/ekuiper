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
	"time"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/checkpoint"
	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
)

func TestCoordinatorStopUnblocksForceSaveWaiter(t *testing.T) {
	ctx := topoContext.Background().WithMeta("rule", "coordinator", &state.MemoryStore{})
	coordinator := checkpoint.NewCoordinator("rule", nil, nil, nil, def.AtLeastOnce, &state.MemoryStore{}, time.Hour, ctx)
	if err := coordinator.Activate(); err != nil {
		t.Fatal(err)
	}
	notify, err := coordinator.ForceSaveState()
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Deactivate(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-notify:
	case <-time.After(time.Second):
		t.Fatal("coordinator stop left force-save waiter blocked")
	}
}
