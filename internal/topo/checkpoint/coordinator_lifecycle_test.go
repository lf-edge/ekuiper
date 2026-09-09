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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
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
	notify := make(chan any)
	coordinator := &Coordinator{
		forceSaveStateNotify: notify,
	}
	const (
		oldCheckpointID   = int64(1)
		forceCheckpointID = int64(2)
	)
	coordinator.inForceSaveState.Store(true)
	coordinator.forceCheckpointID.Store(forceCheckpointID)

	coordinator.finishForceSaveState(oldCheckpointID)
	select {
	case <-notify:
		t.Fatal("an older periodic checkpoint completed the force-save request")
	default:
	}
	if !coordinator.inForceSaveState.Load() {
		t.Fatal("force-save flag cleared by an older checkpoint")
	}

	coordinator.finishForceSaveState(forceCheckpointID)
	select {
	case <-notify:
	default:
		t.Fatal("force-save checkpoint did not notify completion synchronously")
	}
	if coordinator.inForceSaveState.Load() {
		t.Fatal("force-save flag remained set after its checkpoint completed")
	}
}

func TestAbandonedForceSaveNotificationDoesNotAffectNextCall(t *testing.T) {
	first := make(chan any)
	coordinator := &Coordinator{forceSaveStateNotify: first}
	coordinator.inForceSaveState.Store(true)
	coordinator.forceCheckpointID.Store(1)
	coordinator.finishForceSaveState(1)

	second := make(chan any)
	coordinator.forceSaveStateNotify = second
	coordinator.inForceSaveState.Store(true)
	coordinator.forceCheckpointID.Store(2)
	select {
	case <-second:
		t.Fatal("second force-save call consumed the first call's completion")
	default:
	}
	coordinator.finishForceSaveState(2)
	select {
	case <-second:
	case <-time.After(time.Second):
		t.Fatal("second force-save call was not completed")
	}
	select {
	case <-first:
	default:
		t.Fatal("abandoned first completion was not independently closed")
	}
}

func TestCheckpointIDsRemainMonotonicAtSameTimestamp(t *testing.T) {
	coordinator := &Coordinator{
		pendingCheckpoints: &sync.Map{},
		store:              &lifecycleStore{},
		cleanThreshold:     100,
	}
	now := time.UnixMilli(100)
	first := coordinator.saveState(now, noopLogger{})
	second := coordinator.saveState(now, noopLogger{})
	if second != first+1 {
		t.Fatalf("checkpoint IDs are not monotonic: first %d, second %d", first, second)
	}
}

func TestSaveCheckpointFailureCancelsPendingCheckpoint(t *testing.T) {
	store := &lifecycleStore{saveCheckpointErr: errors.New("injected save failure")}
	coordinator := &Coordinator{
		pendingCheckpoints:   &sync.Map{},
		completedCheckpoints: &checkpointStore{maxNum: 3},
		store:                store,
		ctx:                  &lifecycleContext{logger: noopLogger{}},
	}
	const checkpointID = int64(30)
	coordinator.pendingCheckpoints.Store(checkpointID, &pendingCheckpoint{checkpointId: checkpointID})

	coordinator.complete(checkpointID)
	if _, ok := coordinator.pendingCheckpoints.Load(checkpointID); ok {
		t.Fatal("checkpoint remained pending after SaveCheckpoint failure")
	}
	if len(store.discarded) != 1 || store.discarded[0] != checkpointID {
		t.Fatalf("failed checkpoint was not discarded: %#v", store.discarded)
	}
	if coordinator.completedCheckpoints.getCount() != 0 {
		t.Fatal("failed checkpoint was recorded as complete")
	}
}

type lifecycleStore struct {
	discarded         []int64
	saveCheckpointErr error
}

func (s *lifecycleStore) SaveState(_ int64, _ string, _ map[string]interface{}) error {
	return nil
}

func (s *lifecycleStore) SaveCheckpoint(_ int64) error {
	return s.saveCheckpointErr
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

type lifecycleContext struct {
	api.StreamContext
	logger api.Logger
}

func (c *lifecycleContext) GetLogger() api.Logger {
	return c.logger
}

type noopLogger struct{}

func (noopLogger) Debug(...interface{})          {}
func (noopLogger) Info(...interface{})           {}
func (noopLogger) Warn(...interface{})           {}
func (noopLogger) Error(...interface{})          {}
func (noopLogger) Debugf(string, ...interface{}) {}
func (noopLogger) Infof(string, ...interface{})  {}
func (noopLogger) Warnf(string, ...interface{})  {}
func (noopLogger) Errorf(string, ...interface{}) {}
