// Copyright 2022-2026 EMQ Technologies Co., Ltd.
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

package operator

import (
	"encoding/gob"
	"fmt"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/xsql"
)

const leadOperatorStateKey = "$$lead_operator_state"

// Snapshots use row indexes instead of the runtime request-to-owner pointers.
type leadRequestSnapshot struct {
	Owner      int
	Remaining  int
	Default    interface{}
	IgnoreNull bool
}

type leadPendingSnapshot struct {
	Row        xsql.Row
	Unresolved int
}

type leadSnapshot struct {
	Pending       []leadPendingSnapshot
	Calls         map[int]map[string][]leadRequestSnapshot
	LastWatermark time.Time
}

func init() {
	// Keep the persisted name from before the buffer extraction, so existing
	// checkpoints can still be decoded after the Go type is renamed.
	gob.RegisterName("github.com/lf-edge/ekuiper/v2/internal/topo/operator.LeadOperatorState", leadSnapshot{})
}

func (b *leadBuffer) restore(ctx api.StreamContext) error {
	stored, err := ctx.GetState(leadOperatorStateKey)
	if err != nil {
		return err
	}
	if stored == nil {
		return nil
	}
	snapshot, ok := stored.(leadSnapshot)
	if !ok {
		return fmt.Errorf("invalid lead operator state %T", stored)
	}
	b.lastWatermark = snapshot.LastWatermark
	for _, pending := range snapshot.Pending {
		b.pending = append(b.pending, &pendingLeadRow{row: pending.Row.Clone(), unresolved: pending.Unresolved})
	}
	for callID, partitions := range snapshot.Calls {
		restored := make(map[string][]*leadRequest)
		for partition, requests := range partitions {
			for _, request := range requests {
				if request.Owner < 0 || request.Owner >= len(b.pending) {
					return fmt.Errorf("invalid lead owner index %d", request.Owner)
				}
				restored[partition] = append(restored[partition], &leadRequest{
					owner: b.pending[request.Owner], remaining: request.Remaining,
					dft: request.Default, ignoreNull: request.IgnoreNull,
				})
			}
		}
		b.requests[callID] = restored
	}
	return nil
}

func (b *leadBuffer) save(ctx api.StreamContext) error {
	ownerIndexes := make(map[*pendingLeadRow]int, len(b.pending))
	snapshot := leadSnapshot{Calls: make(map[int]map[string][]leadRequestSnapshot), LastWatermark: b.lastWatermark}
	for i, owner := range b.pending {
		ownerIndexes[owner] = i
		// Clone mutable analytic caches; subsequent input must not modify the
		// checkpoint while it is being saved asynchronously. Source data is immutable.
		snapshot.Pending = append(snapshot.Pending, leadPendingSnapshot{Row: owner.row.Clone(), Unresolved: owner.unresolved})
	}
	for callID, requestsByPartition := range b.requests {
		partitions := make(map[string][]leadRequestSnapshot)
		for partition, requests := range requestsByPartition {
			for _, request := range requests {
				owner, ok := ownerIndexes[request.owner]
				if !ok {
					return fmt.Errorf("lead request references an emitted row")
				}
				partitions[partition] = append(partitions[partition], leadRequestSnapshot{
					Owner: owner, Remaining: request.remaining, Default: request.dft, IgnoreNull: request.ignoreNull,
				})
			}
		}
		snapshot.Calls[callID] = partitions
	}
	return ctx.PutState(leadOperatorStateKey, snapshot)
}
