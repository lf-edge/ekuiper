// Copyright 2021-2026 EMQ Technologies Co., Ltd.
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
	"fmt"

	"github.com/lf-edge/ekuiper/v2/pkg/infra"
)

type Responder interface {
	TriggerCheckpoint(checkpointId int64) error
	GetName() string
}

type ResponderExecutor struct {
	responder chan<- *Signal
	task      StreamTask
}

func NewResponderExecutor(responder chan<- *Signal, task StreamTask) *ResponderExecutor {
	return &ResponderExecutor{
		responder: responder,
		task:      task,
	}
}

func (re *ResponderExecutor) GetName() string {
	return re.task.GetName()
}

func (re *ResponderExecutor) TriggerCheckpoint(checkpointId int64) error {
	name := re.GetName()
	ctx := re.task.GetStreamContext()
	logger := ctx.GetLogger()
	logger.Debugf("Starting checkpoint %d on task %s", checkpointId, name)
	var guard CheckpointGuard
	if g, ok := re.task.(CheckpointGuard); ok {
		guard = g
		guard.LockCheckpoint()
	}
	unlockCheckpoint := func() {
		if guard != nil {
			guard.UnlockCheckpoint()
			guard = nil
		}
	}
	sendSignal := func(message Message) {
		signal := &Signal{
			Message: message,
			Barrier: Barrier{CheckpointId: checkpointId, OpId: name},
		}
		select {
		case re.responder <- signal:
		case <-ctx.Done():
		}
	}
	if preparer, ok := re.task.(CheckpointPreparer); ok {
		if err := preparer.PrepareCheckpoint(); err != nil {
			unlockCheckpoint()
			sendSignal(DEC)
			return fmt.Errorf("task %s cannot prepare checkpoint: %w", name, err)
		}
	}
	// create
	barrier := &Barrier{
		CheckpointId: checkpointId,
		OpId:         name,
	}
	// broadcast barrier
	if nonSink, ok := re.task.(NonSinkTask); ok {
		nonSink.Broadcast(barrier)
	}
	if validator, ok := re.task.(CheckpointStateValidator); ok {
		if checkpointErr := validator.CheckpointError(); checkpointErr != nil {
			unlockCheckpoint()
			sendSignal(DEC)
			return fmt.Errorf("task %s cannot checkpoint: %w", name, checkpointErr)
		}
	}
	sctx, ok := ctx.(StreamCheckpointContext)
	if !ok {
		unlockCheckpoint()
		sendSignal(DEC)
		return fmt.Errorf("invalid context for checkpoint responder, must be a StreamCheckpointContext")
	}
	// Save key state to the global state
	err := sctx.Snapshot(checkpointId)
	if err != nil {
		unlockCheckpoint()
		sendSignal(DEC)
		return err
	}
	unlockCheckpoint()
	go func() {
		state := ACK
		err := infra.SafeRun(func() error {
			return sctx.SaveSnapshot(checkpointId)
		})
		if err != nil {
			logger.Infof("save checkpoint error %s", err)
			state = DEC
		}

		sendSignal(state)
		logger.Debugf("Complete checkpoint %d on task %s", checkpointId, name)
	}()
	return nil
}
