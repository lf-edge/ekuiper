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

package checkpoint

import (
	"sync"
	"testing"
)

// recordingStore extends lifecycleStore by recording SaveCheckpoint calls.
// It lives here to keep the shared lifecycle harness untouched.
type recordingStore struct {
	lifecycleStore
	saved []int64
}

func (s *recordingStore) SaveCheckpoint(checkpointID int64) error {
	s.saved = append(s.saved, checkpointID)
	return s.saveCheckpointErr
}

// TestDecThenLateAcksNeverPersist locks in the coordinator side of the
// failed-checkpoint protocol: once a checkpoint is canceled via DEC, late
// ACKs or a belated completion must never persist it as a valid checkpoint.
func TestDecThenLateAcksNeverPersist(t *testing.T) {
	store := &recordingStore{}
	coordinator := &Coordinator{
		pendingCheckpoints:   &sync.Map{},
		completedCheckpoints: &checkpointStore{maxNum: 3},
		store:                store,
		ctx:                  &lifecycleContext{logger: noopLogger{}},
	}
	const checkpointID = int64(41)
	coordinator.pendingCheckpoints.Store(checkpointID, &pendingCheckpoint{
		checkpointId: checkpointID,
		notYetAckTasks: map[string]bool{
			"source": true,
			"window": true,
		},
	})

	// DEC from one task cancels the checkpoint, as the Activate loop does.
	coordinator.cancel(checkpointID, "source")

	// Late ACKs arrive for the remaining task after the cancellation.
	if cp, ok := coordinator.pendingCheckpoints.Load(checkpointID); ok {
		cp.(*pendingCheckpoint).ack("window")
		if cp.(*pendingCheckpoint).isFullyAck() {
			coordinator.complete(checkpointID)
		}
	}
	// A belated completion attempt must be a no-op as well.
	coordinator.complete(checkpointID)

	if len(store.saved) != 0 {
		t.Fatalf("canceled checkpoint was persisted: %#v", store.saved)
	}
	if len(store.discarded) != 1 || store.discarded[0] != checkpointID {
		t.Fatalf("canceled checkpoint was not discarded exactly once: %#v", store.discarded)
	}
	if coordinator.completedCheckpoints.getCount() != 0 {
		t.Fatal("canceled checkpoint was recorded as complete")
	}
	if _, ok := coordinator.pendingCheckpoints.Load(checkpointID); ok {
		t.Fatal("canceled checkpoint remained pending")
	}
}
