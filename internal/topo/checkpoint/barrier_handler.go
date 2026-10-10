// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/contract/v2/api"
)

type BarrierHandler interface {
	Process(data *BufferOrEvent, ctx api.StreamContext) bool // If data is barrier return true, else return false
	// NextDue returns the next backlog row or barrier the operator loop must
	// handle before pulling fresh input, preserving arrival order. It is
	// non-blocking and returns false when nothing is due. Operators that may
	// observe more than one input must consult it on every loop iteration;
	// see BarrierAligner for the ordering contract.
	NextDue() (*BufferOrEvent, bool)
}

// For qos 1, simple track barriers
type BarrierTracker struct {
	mu                 sync.Mutex
	responder          Responder
	inputCount         int
	pendingCheckpoints map[int64]int
}

func NewBarrierTracker(responder Responder, inputCount int) *BarrierTracker {
	return &BarrierTracker{
		responder:          responder,
		inputCount:         inputCount,
		pendingCheckpoints: make(map[int64]int),
	}
}

func (h *BarrierTracker) Process(data *BufferOrEvent, ctx api.StreamContext) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	d := data.Data
	if b, ok := d.(*Barrier); ok {
		h.processBarrier(b, ctx)
		return true
	}
	return false
}

// NextDue always reports nothing: the tracker never holds rows back.
func (h *BarrierTracker) NextDue() (*BufferOrEvent, bool) {
	return nil, false
}

func (h *BarrierTracker) processBarrier(b *Barrier, ctx api.StreamContext) {
	logger := ctx.GetLogger()
	if h.inputCount == 1 {
		err := h.responder.TriggerCheckpoint(b.CheckpointId)
		if err != nil {
			logger.Errorf("trigger checkpoint for %s err: %s", h.responder.GetName(), err)
		}
		return
	}
	if c, ok := h.pendingCheckpoints[b.CheckpointId]; ok {
		c += 1
		if c == h.inputCount {
			err := h.responder.TriggerCheckpoint(b.CheckpointId)
			if err != nil {
				delete(h.pendingCheckpoints, b.CheckpointId)
				logger.Errorf("trigger checkpoint for %s err: %s", h.responder.GetName(), err)
				return
			}
			delete(h.pendingCheckpoints, b.CheckpointId)
			for cid := range h.pendingCheckpoints {
				if cid < b.CheckpointId {
					delete(h.pendingCheckpoints, cid)
				}
			}
		} else {
			h.pendingCheckpoints[b.CheckpointId] = c
		}
	} else {
		h.pendingCheckpoints[b.CheckpointId] = 1
	}
}

// For qos 2, align barriers across inputs while preserving row order.
//
// Ordering contract: rows are snapshotted exactly when their channel's
// barrier is snapshotted upstream. A row arriving after its channel's barrier
// for epoch E must be processed after E's trigger fires; a row arriving
// before it must be processed before. The aligner tags such rows (heldFor)
// and positions the trigger as a marker inside a single arrival-ordered
// backlog, so the operator loop drains rows strictly before the trigger by
// consulting NextDue before pulling fresh input. No row is ever held past
// the trigger it belongs after, and no trigger fires before its rows.
//
// Single-input operators keep the previous immediate behavior and never
// queue: barriers trigger inline and rows always flow.
type BarrierAligner struct {
	mu         sync.Mutex
	responder  Responder
	inputCount int
	// curEpoch is the epoch currently aligning or deferring; 0 means idle.
	curEpoch int64
	// firedEpoch is the last epoch whose trigger fired.
	firedEpoch int64
	// delivered tracks per-epoch barrier arrivals by channel.
	delivered map[int64]map[string]bool
	// armed marks epochs whose trigger marker is placed but not yet fired.
	armed map[int64]bool
	// backlog holds rows, barriers and trigger markers in arrival order.
	backlog []backlogEntry
	// inflight holds due items handed out via NextDue; when the operator
	// loop feeds them back through Process they bypass queueing.
	inflight map[*BufferOrEvent]bool
}

// backlogEntry is one queued item. Rows carry heldFor when they must follow
// the trigger of that epoch; barriers carry their epoch for adoption and
// completion bookkeeping; markers position a trigger after all preceding
// rows have been processed.
type backlogEntry struct {
	item    *BufferOrEvent
	heldFor int64
	barrier bool
	epoch   int64
	marker  bool
}

// triggerMarker orders a checkpoint trigger inside the backlog. It fires
// only after every row ahead of it has been processed by the operator.
type triggerMarker struct {
	checkpointID int64
}

func NewBarrierAligner(responder Responder, inputCount int) *BarrierAligner {
	return &BarrierAligner{
		responder:  responder,
		inputCount: inputCount,
		delivered:  make(map[int64]map[string]bool),
		armed:      make(map[int64]bool),
		inflight:   make(map[*BufferOrEvent]bool),
	}
}

func (h *BarrierAligner) Process(data *BufferOrEvent, ctx api.StreamContext) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.inflight[data] {
		delete(h.inflight, data)
		return h.processDue(data, ctx)
	}
	// Single-input operators keep immediate behavior and never queue.
	if h.inputCount == 1 {
		if b, ok := data.Data.(*Barrier); ok {
			if b.CheckpointId > h.firedEpoch {
				h.firedEpoch = b.CheckpointId
				if err := h.responder.TriggerCheckpoint(b.CheckpointId); err != nil {
					ctx.GetLogger().Errorf("trigger checkpoint for %s err: %s", h.responder.GetName(), err)
				}
			}
			return true
		}
		return false
	}
	switch d := data.Data.(type) {
	case *Barrier:
		h.enqueueBarrier(data, d, ctx)
		return true
	default:
		return h.enqueueRow(data)
	}
}

// processDue handles items previously handed out via NextDue. Rows flow
// straight to the operator, barriers need no further bookkeeping because
// arrivals were already recorded at enqueue time, and markers fire the
// positioned trigger. None of them re-queue.
func (h *BarrierAligner) processDue(data *BufferOrEvent, ctx api.StreamContext) bool {
	switch d := data.Data.(type) {
	case *Barrier:
		_ = d
		return true
	case triggerMarker:
		h.fireLocked(d.checkpointID, ctx)
		return true
	default:
		return false
	}
}

// enqueueRow holds a fresh row when ordering requires it, tagging rows that
// arrived after their channel's barrier of the current epoch so they follow
// that epoch's trigger. Otherwise the row flows immediately. It reports
// whether the row was queued.
func (h *BarrierAligner) enqueueRow(data *BufferOrEvent) bool {
	if len(h.backlog) == 0 && !h.delivered[h.curEpoch][data.Channel] {
		return false
	}
	var heldFor int64
	if h.delivered[h.curEpoch][data.Channel] {
		heldFor = h.curEpoch
	}
	h.backlog = append(h.backlog, backlogEntry{item: data, heldFor: heldFor})
	return true
}

// enqueueBarrier records a fresh barrier arrival, advances epochs, and
// completes the alignment when every input has delivered the epoch. Stale
// barriers are dropped. Higher epochs preempt an incomplete alignment: rows
// held for the preempted epoch are released as ordinary rows because that
// checkpoint can never complete.
func (h *BarrierAligner) enqueueBarrier(data *BufferOrEvent, b *Barrier, ctx api.StreamContext) {
	logger := ctx.GetLogger()
	if b.CheckpointId <= h.firedEpoch || b.CheckpointId < h.curEpoch {
		logger.Debugf("Aligner drops stale barrier %+v", b)
		return
	}
	if h.delivered[b.CheckpointId] == nil {
		h.delivered[b.CheckpointId] = make(map[string]bool)
	}
	h.delivered[b.CheckpointId][data.Channel] = true
	if b.CheckpointId > h.curEpoch {
		h.abortLocked(h.curEpoch)
		h.curEpoch = b.CheckpointId
	}
	h.backlog = append(h.backlog, backlogEntry{item: data, barrier: true, epoch: b.CheckpointId})
	h.maybeCompleteLocked(ctx)
}

// abortLocked retires an incomplete epoch: its held rows become ordinary
// rows and its queued barriers are dropped because that checkpoint can
// never complete once preempted.
func (h *BarrierAligner) abortLocked(epoch int64) {
	if epoch == 0 || h.armed[epoch] {
		return
	}
	kept := h.backlog[:0]
	for _, e := range h.backlog {
		if e.barrier && e.epoch == epoch {
			continue
		}
		if !e.barrier && !e.marker && e.heldFor == epoch {
			e.heldFor = 0
		}
		kept = append(kept, e)
	}
	h.backlog = kept
	delete(h.delivered, epoch)
}

// maybeCompleteLocked positions the trigger once every input has delivered
// the current epoch. When no ordinary row precedes it, the trigger fires
// immediately, preserving the previous trigger-then-replay behavior;
// otherwise a marker orders it after the preceding rows.
func (h *BarrierAligner) maybeCompleteLocked(ctx api.StreamContext) {
	if h.armed[h.curEpoch] || len(h.delivered[h.curEpoch]) != h.inputCount {
		return
	}
	logger := ctx.GetLogger()
	logger.Debugf("Received all barriers, triggering checkpoint %d", h.curEpoch)
	// Consume the epoch barriers: their bookkeeping is done.
	kept := h.backlog[:0]
	for _, e := range h.backlog {
		if e.barrier && e.epoch == h.curEpoch {
			continue
		}
		kept = append(kept, e)
	}
	h.backlog = kept
	pre := 0
	for _, e := range h.backlog {
		if e.barrier || e.marker {
			continue
		}
		if e.heldFor == 0 || e.heldFor <= h.firedEpoch {
			pre++
		}
	}
	if pre == 0 {
		h.fireLocked(h.curEpoch, ctx)
		return
	}
	h.armed[h.curEpoch] = true
	// Stable partition: ordinary rows, then the marker, then everything else.
	partitioned := make([]backlogEntry, 0, len(h.backlog)+1)
	var rest []backlogEntry
	for _, e := range h.backlog {
		if !e.barrier && !e.marker && (e.heldFor == 0 || e.heldFor <= h.firedEpoch) {
			partitioned = append(partitioned, e)
		} else {
			rest = append(rest, e)
		}
	}
	partitioned = append(partitioned, backlogEntry{
		item:   &BufferOrEvent{Data: triggerMarker{checkpointID: h.curEpoch}},
		marker: true,
		epoch:  h.curEpoch,
	})
	h.backlog = append(partitioned, rest...)
}

// serveBarrier applies a due barrier's alignment bookkeeping. Arrivals were
// already recorded at enqueue time, so serving only advances epochs and
// checks completion.
func (h *BarrierAligner) serveBarrier(d *Barrier) {
	if d.CheckpointId <= h.firedEpoch || d.CheckpointId < h.curEpoch {
		return
	}
	if d.CheckpointId > h.curEpoch {
		h.abortLocked(h.curEpoch)
		h.curEpoch = d.CheckpointId
	}
}

// fireLocked runs the checkpoint trigger and releases everything held for
// that epoch, even when the trigger reports an error: the checkpoint is
// dead but the stream must continue, matching the previous failure behavior
// of replaying the buffer and carrying on.
func (h *BarrierAligner) fireLocked(checkpointID int64, ctx api.StreamContext) {
	if err := h.responder.TriggerCheckpoint(checkpointID); err != nil {
		ctx.GetLogger().Errorf("trigger checkpoint for %s err: %s", h.responder.GetName(), err)
	}
	delete(h.armed, checkpointID)
	if checkpointID > h.firedEpoch {
		h.firedEpoch = checkpointID
	}
	for i := range h.backlog {
		if !h.backlog[i].barrier && !h.backlog[i].marker && h.backlog[i].heldFor <= checkpointID {
			h.backlog[i].heldFor = 0
		}
	}
	for epoch := range h.delivered {
		if epoch <= checkpointID {
			delete(h.delivered, epoch)
		}
	}
}

// NextDue returns the next backlog item the operator loop must handle
// before pulling fresh input, or false when nothing is due. Head order is
// strict: a row held for an unfired trigger stalls later items until its
// marker fires, which the partition guarantees is positioned ahead of it.
func (h *BarrierAligner) NextDue() (*BufferOrEvent, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.backlog) == 0 {
		return nil, false
	}
	head := h.backlog[0]
	if !head.barrier && !head.marker && head.heldFor > h.firedEpoch {
		return nil, false
	}
	h.backlog = h.backlog[1:]
	h.inflight[head.item] = true
	return head.item, true
}
