// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/pkg/infra"
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
	ctx := re.task.GetStreamContext()
	logger := ctx.GetLogger()
	sctx, ok := ctx.(StreamCheckpointContext)
	if !ok {
		return fmt.Errorf("invalid context for checkpoint responder, must be a StreamCheckpointContext")
	}
	name := re.GetName()
	logger.Debugf("Starting checkpoint %d on task %s", checkpointId, name)
	// create
	barrier := &Barrier{
		CheckpointId: checkpointId,
		OpId:         name,
	}
	// broadcast barrier
	re.task.Broadcast(barrier)
	// Save key state to the global state
	err := sctx.Snapshot()
	if err != nil {
		return err
	}
	go infra.SafeRun(func() error {
		state := ACK
		err := sctx.SaveState(checkpointId)
		if err != nil {
			logger.Infof("save checkpoint error %s", err)
			state = DEC
		}

		signal := &Signal{
			Message: state,
			Barrier: Barrier{CheckpointId: checkpointId, OpId: name},
		}
		re.responder <- signal
		logger.Debugf("Complete checkpoint %d on task %s", checkpointId, name)
		return nil
	})
	return nil
}
