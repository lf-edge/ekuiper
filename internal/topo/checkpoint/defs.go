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
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
)

type StreamTask interface {
	GetName() string
	GetStreamContext() api.StreamContext
	SetQos(qos def.Qos)
}

type NonSinkTask interface {
	Broadcast(data any)
}

type NonSourceTask interface {
	StreamTask
	GetInputCount() int
	AddInputCount()

	SetBarrierHandler(BarrierHandler)
}

type SourceSubTopoTask interface {
	EnableCheckpoint(sources *[]StreamTask, ops *[]NonSourceTask)
}

type SinkTask interface {
	NonSourceTask
}

type BufferOrEvent struct {
	Data    interface{}
	Channel string
}

type StreamCheckpointContext interface {
	Snapshot(checkpointID int64) error
	SaveSnapshot(checkpointID int64) error
}

// CheckpointGuard serializes source ingestion with barrier emission and state
// capture. Non-source tasks do not need to implement it because they process a
// barrier on their event-loop goroutine.
type CheckpointGuard interface {
	LockCheckpoint()
	UnlockCheckpoint()
}

// CheckpointStateValidator reports whether task state is safe to capture.
// Responder calls it while the CheckpointGuard is held and after propagating
// the barrier, so every downstream task can still terminate the checkpoint.
type CheckpointStateValidator interface {
	CheckpointGuard
	CheckpointError() error
}

// CheckpointPreparer materializes operator state at the checkpoint boundary.
// Non-source tasks are called synchronously by their barrier handler, after
// alignment and before forwarding the barrier or taking the context snapshot.
type CheckpointPreparer interface {
	PrepareCheckpoint() error
}

type Message int

const (
	STOP Message = iota
	ACK
	DEC
	ForceSaveState
)

type Signal struct {
	Message Message
	Barrier
}

type Barrier struct {
	CheckpointId int64
	OpId         string
}
