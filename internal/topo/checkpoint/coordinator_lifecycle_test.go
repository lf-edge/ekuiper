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

func TestCanceledCheckpointIsRemovedImmediately(t *testing.T) {
	store := &lifecycleStore{}
	coordinator := &Coordinator{
		pendingCheckpoints: &sync.Map{},
		store:              store,
	}
	const checkpointID = int64(10)
	const olderCheckpointID = int64(9)
	coordinator.pendingCheckpoints.Store(olderCheckpointID, &pendingCheckpoint{
		checkpointId: olderCheckpointID,
		notYetAckTasks: map[string]bool{
			"source": true,
		},
	})
	coordinator.pendingCheckpoints.Store(checkpointID, &pendingCheckpoint{
		checkpointId: checkpointID,
		notYetAckTasks: map[string]bool{
			"source": true,
			"window": true,
		},
	})

	coordinator.cancel(checkpointID, "source")
	if len(store.discarded) != 2 {
		t.Fatalf("canceled checkpoint and its older predecessor were not both discarded: %#v", store.discarded)
	}
	if _, ok := coordinator.pendingCheckpoints.Load(checkpointID); ok {
		t.Fatal("canceled checkpoint remained pending")
	}
	if _, ok := coordinator.pendingCheckpoints.Load(olderCheckpointID); ok {
		t.Fatal("older checkpoint remained pending after a newer cancellation")
	}
}

func TestForceSaveCompletionIsBoundToCheckpointID(t *testing.T) {
	coordinator := &Coordinator{
		forceSaveStateNotify: make(chan any, 2),
	}
	const (
		oldCheckpointID   = int64(1)
		forceCheckpointID = int64(2)
	)
	coordinator.inForceSaveState.Store(true)
	coordinator.forceCheckpointID.Store(forceCheckpointID)

	coordinator.finishForceSaveState(oldCheckpointID)
	select {
	case <-coordinator.forceSaveStateNotify:
		t.Fatal("an older periodic checkpoint completed the force-save request")
	default:
	}
	if !coordinator.inForceSaveState.Load() {
		t.Fatal("force-save flag cleared by an older checkpoint")
	}

	coordinator.finishForceSaveState(forceCheckpointID)
	select {
	case <-coordinator.forceSaveStateNotify:
	default:
		t.Fatal("force-save checkpoint did not notify completion synchronously")
	}
	if coordinator.inForceSaveState.Load() {
		t.Fatal("force-save flag remained set after its checkpoint completed")
	}
}

type lifecycleStore struct {
	discarded []int64
}

func (s *lifecycleStore) SaveState(_ int64, _ string, _ map[string]interface{}) error {
	return nil
}

func (s *lifecycleStore) SaveCheckpoint(_ int64) error {
	return nil
}

func (s *lifecycleStore) GetOpState(_ string) (*sync.Map, error) {
	return &sync.Map{}, nil
}

func (s *lifecycleStore) Clean() error {
	return nil
}

func (s *lifecycleStore) SaveFrozenState(_ int64, _ string, _ []byte) error {
	return nil
}

func (s *lifecycleStore) DiscardFrozenState(checkpointID int64) {
	s.discarded = append(s.discarded, checkpointID)
}
