package checkpoints

import "github.com/emqx/kuiper/xstream/api"

type BarrierHandler interface {
	Process(data *BufferOrEvent, ctx api.StreamContext) bool //If data is barrier return true, else return false
	SetOutput(chan<- *BufferOrEvent)                         //It is using for block a channel
}

//For qos 1, simple track barriers
type BarrierTracker struct {
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
	d := data.Data
	if b, ok := d.(*Barrier); ok {
		h.processBarrier(b, ctx)
		return true
	}
	return false
}

func (h *BarrierTracker) SetOutput(output chan<- *BufferOrEvent) {
	//do nothing, does not need it
}

func (h *BarrierTracker) processBarrier(b *Barrier, _ api.StreamContext) {
	if h.inputCount == 1 {
		h.responder.TriggerCheckpoint(b.CheckpointId)
		return
	}
	if c, ok := h.pendingCheckpoints[b.CheckpointId]; ok {
		c += 1
		if c == h.inputCount {
			h.responder.TriggerCheckpoint(b.CheckpointId)
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

//For qos 2, block an input until all barriers are received
type BarrierAligner struct {
	responder           Responder
	inputCount          int
	currentCheckpointId int64
	output              chan<- *BufferOrEvent
	blockedChannels     map[string]bool
	buffer              []*BufferOrEvent
}

func NewBarrierAligner(responder Responder, inputCount int) *BarrierAligner {
	ba := &BarrierAligner{
		responder:       responder,
		inputCount:      inputCount,
		blockedChannels: make(map[string]bool),
	}
	return ba
}

func (h *BarrierAligner) Process(data *BufferOrEvent, ctx api.StreamContext) bool {
	switch d := data.Data.(type) {
	case *Barrier:
		h.processBarrier(d, ctx)
		return true
	default:
		//If blocking, save to buffer
		if h.inputCount > 1 && len(h.blockedChannels) > 0 {
			if _, ok := h.blockedChannels[data.Channel]; ok {
				h.buffer = append(h.buffer, data)
				return true
			}
		}
	}
	return false
}

func (h *BarrierAligner) processBarrier(b *Barrier, ctx api.StreamContext) {
	logger := ctx.GetLogger()
	logger.Debugf("Aligner process barrier %+v", b)
	if h.inputCount == 1 {
		if b.CheckpointId > h.currentCheckpointId {
			h.currentCheckpointId = b.CheckpointId
			h.responder.TriggerCheckpoint(b.CheckpointId)
		}
		return
	}
	if len(h.blockedChannels) > 0 {
		if b.CheckpointId == h.currentCheckpointId {
			h.onBarrier(b.OpId, ctx)
		} else if b.CheckpointId > h.currentCheckpointId {
			logger.Infof("Received checkpoint barrier for checkpoint %d before complete current checkpoint %d. Skipping current checkpoint.", b.CheckpointId, h.currentCheckpointId)
			//TODO Abort checkpoint

			h.releaseBlocksAndResetBarriers()
			h.beginNewAlignment(b, ctx)
		} else {
			return
		}
	} else if b.CheckpointId > h.currentCheckpointId {
		logger.Debugf("Aligner process new alignment", b)
		h.beginNewAlignment(b, ctx)
	} else {
		return
	}
	if len(h.blockedChannels) == h.inputCount {
		logger.Debugf("Received all barriers, triggering checkpoint %d", b.CheckpointId)
		h.releaseBlocksAndResetBarriers()
		h.responder.TriggerCheckpoint(b.CheckpointId)

		// clean up all the buffer
		var temp []*BufferOrEvent
		for _, d := range h.buffer {
			temp = append(temp, d)
		}
		go func() {
			for _, d := range temp {
				h.output <- d
			}
		}()
		h.buffer = make([]*BufferOrEvent, 0)
	}
}

func (h *BarrierAligner) onBarrier(name string, ctx api.StreamContext) {
	logger := ctx.GetLogger()
	if _, ok := h.blockedChannels[name]; !ok {
		h.blockedChannels[name] = true
		logger.Debugf("Received barrier from channel %s", name)
	}
}

func (h *BarrierAligner) SetOutput(output chan<- *BufferOrEvent) {
	h.output = output
}

func (h *BarrierAligner) releaseBlocksAndResetBarriers() {
	h.blockedChannels = make(map[string]bool)
}

func (h *BarrierAligner) beginNewAlignment(barrier *Barrier, ctx api.StreamContext) {
	logger := ctx.GetLogger()
	h.currentCheckpointId = barrier.CheckpointId
	h.onBarrier(barrier.OpId, ctx)
	logger.Debugf("Starting stream alignment for checkpoint %d", barrier.CheckpointId)
}
