// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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

	"github.com/benbjohnson/clock"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/infra"
)

type pendingCheckpoint struct {
	checkpointId   int64
	isDiscarded    bool
	notYetAckTasks map[string]bool
}

func newPendingCheckpoint(checkpointId int64, tasksToWaitFor []Responder) *pendingCheckpoint {
	pc := &pendingCheckpoint{checkpointId: checkpointId}
	nyat := make(map[string]bool)
	for _, r := range tasksToWaitFor {
		nyat[r.GetName()] = true
	}
	pc.notYetAckTasks = nyat
	return pc
}

func (c *pendingCheckpoint) ack(opId string) bool {
	if c.isDiscarded {
		return false
	}
	delete(c.notYetAckTasks, opId)
	// TODO serialize state
	return true
}

func (c *pendingCheckpoint) isFullyAck() bool {
	return len(c.notYetAckTasks) == 0
}

func (c *pendingCheckpoint) finalize() *completedCheckpoint {
	ccp := &completedCheckpoint{checkpointId: c.checkpointId}
	return ccp
}

func (c *pendingCheckpoint) dispose(_ bool) {
	c.isDiscarded = true
}

type completedCheckpoint struct {
	checkpointId int64
}

type checkpointStore struct {
	maxNum      int
	checkpoints []*completedCheckpoint
}

func (s *checkpointStore) add(c *completedCheckpoint) {
	s.checkpoints = append(s.checkpoints, c)
	if len(s.checkpoints) > s.maxNum {
		s.checkpoints = s.checkpoints[1:]
	}
}

func (s *checkpointStore) getLatest() *completedCheckpoint {
	if len(s.checkpoints) > 0 {
		return s.checkpoints[len(s.checkpoints)-1]
	}
	return nil
}

type Coordinator struct {
	tasksToTrigger          []Responder
	tasksToWaitFor          []Responder
	sinkTasks               []SinkTask
	pendingCheckpoints      *sync.Map
	completedCheckpoints    *checkpointStore
	ruleId                  string
	baseInterval            int
	cleanThreshold          int
	advanceToEndOfEventTime bool
	ticker                  *clock.Ticker // For processing time only
	signal                  chan *Signal
	store                   api.Store
	ctx                     api.StreamContext
	activated               bool
}

func NewCoordinator(ruleId string, sources []StreamTask, operators []NonSourceTask, sinks []SinkTask, qos api.Qos, store api.Store, interval int, ctx api.StreamContext) *Coordinator {
	logger := ctx.GetLogger()
	logger.Infof("create new coordinator for rule %s", ruleId)
	signal := make(chan *Signal, 1024)
	var allResponders, sourceResponders []Responder
	for _, r := range sources {
		r.SetQos(qos)
		re := NewResponderExecutor(signal, r)
		allResponders = append(allResponders, re)
		sourceResponders = append(sourceResponders, re)
	}
	for _, r := range operators {
		r.SetQos(qos)
		re := NewResponderExecutor(signal, r)
		handler := createBarrierHandler(re, r.GetInputCount(), qos)
		r.SetBarrierHandler(handler)
		allResponders = append(allResponders, re)
	}
	for _, r := range sinks {
		r.SetQos(qos)
		re := NewResponderExecutor(signal, r)
		handler := NewBarrierTracker(re, r.GetInputCount())
		r.SetBarrierHandler(handler)
		allResponders = append(allResponders, re)
	}
	// 5 minutes by default
	if interval <= 0 {
		interval = 300000
	}
	return &Coordinator{
		tasksToTrigger:     sourceResponders,
		tasksToWaitFor:     allResponders,
		sinkTasks:          sinks,
		pendingCheckpoints: new(sync.Map),
		completedCheckpoints: &checkpointStore{
			maxNum: 3,
		},
		ruleId:         ruleId,
		signal:         signal,
		baseInterval:   interval,
		store:          store,
		ctx:            ctx,
		cleanThreshold: 100,
	}
}

func createBarrierHandler(re Responder, inputCount int, qos api.Qos) BarrierHandler {
	if qos == api.AtLeastOnce {
		return NewBarrierTracker(re, inputCount)
	} else if qos == api.ExactlyOnce {
		return NewBarrierAligner(re, inputCount)
	} else {
		return nil
	}
}

func (c *Coordinator) Activate() error {
	logger := c.ctx.GetLogger()
	logger.Infof("Start checkpoint coordinator for rule %s at %d", c.ruleId, conf.GetNowInMilli())
	if c.ticker != nil {
		c.ticker.Stop()
	}
	c.ticker = conf.GetTicker(int64(c.baseInterval))
	tc := c.ticker.C
	go func() {
		err := infra.SafeRun(func() error {
			c.activated = true
			toBeClean := 0
			for {
				select {
				case n := <-tc:
					// trigger checkpoint
					// TODO pose max attempt and min pause check for consequent pendingCheckpoints

					// TODO Check if all tasks are running

					// Create a pending checkpoint
					checkpointId := cast.TimeToUnixMilli(n)
					checkpoint := newPendingCheckpoint(checkpointId, c.tasksToWaitFor)
					logger.Debugf("Create checkpoint %d", checkpointId)
					c.pendingCheckpoints.Store(checkpointId, checkpoint)
					// Let the sources send out a barrier
					for _, r := range c.tasksToTrigger {
						go func(t Responder) {
							if err := t.TriggerCheckpoint(checkpointId); err != nil {
								logger.Infof("Fail to trigger checkpoint for source %s with error %v, cancel it", t.GetName(), err)
								c.cancel(checkpointId)
							}
						}(r)
					}
					toBeClean++
					if toBeClean >= c.cleanThreshold {
						c.store.Clean()
						toBeClean = 0
					}
				case s := <-c.signal:
					switch s.Message {
					case STOP:
						logger.Debug("Stop checkpoint scheduler")
						if c.ticker != nil {
							c.ticker.Stop()
						}
						return nil
					case ACK:
						logger.Debugf("Receive ack from %s for checkpoint %d", s.OpId, s.CheckpointId)
						if cp, ok := c.pendingCheckpoints.Load(s.CheckpointId); ok {
							checkpoint := cp.(*pendingCheckpoint)
							checkpoint.ack(s.OpId)
							if checkpoint.isFullyAck() {
								c.complete(s.CheckpointId)
							}
						} else {
							logger.Debugf("Receive ack from %s for non existing checkpoint %d", s.OpId, s.CheckpointId)
						}
					case DEC:
						logger.Debugf("Receive dec from %s for checkpoint %d, cancel it", s.OpId, s.CheckpointId)
						c.cancel(s.CheckpointId)
					}
				case <-c.ctx.Done():
					logger.Infoln("Cancelling coordinator....")
					if c.ticker != nil {
						c.ticker.Stop()
						logger.Infoln("Stop coordinator ticker")
					}
					return nil
				}
			}
		})
		logger.Error(err)
	}()
	return nil
}

func (c *Coordinator) Deactivate() error {
	if c.ticker != nil {
		c.ticker.Stop()
	}
	c.signal <- &Signal{Message: STOP}
	return nil
}

func (c *Coordinator) cancel(checkpointId int64) {
	logger := c.ctx.GetLogger()
	if checkpoint, ok := c.pendingCheckpoints.Load(checkpointId); ok {
		c.pendingCheckpoints.Delete(checkpointId)
		checkpoint.(*pendingCheckpoint).dispose(true)
	} else {
		logger.Debugf("Cancel for non existing checkpoint %d. Just ignored", checkpointId)
	}
}

func (c *Coordinator) complete(checkpointId int64) {
	logger := c.ctx.GetLogger()

	if ccp, ok := c.pendingCheckpoints.Load(checkpointId); ok {
		err := c.store.SaveCheckpoint(checkpointId)
		if err != nil {
			logger.Infof("Cannot save checkpoint %d due to storage error: %v", checkpointId, err)
			// TODO handle checkpoint error
			return
		}
		c.completedCheckpoints.add(ccp.(*pendingCheckpoint).finalize())
		c.pendingCheckpoints.Delete(checkpointId)
		// Drop the previous pendingCheckpoints
		c.pendingCheckpoints.Range(func(a1 interface{}, a2 interface{}) bool {
			cid := a1.(int64)
			cp := a2.(*pendingCheckpoint)
			if cid < checkpointId {
				// TODO revisit how to abort a checkpoint, discard callback
				cp.isDiscarded = true
				c.pendingCheckpoints.Delete(cid)
			}
			return true
		})
		logger.Debugf("Totally complete checkpoint %d", checkpointId)
	} else {
		logger.Infof("Cannot find checkpoint %d to complete", checkpointId)
	}
}

// For testing
func (c *Coordinator) GetCompleteCount() int {
	return len(c.completedCheckpoints.checkpoints)
}

func (c *Coordinator) GetLatest() int64 {
	return c.completedCheckpoints.getLatest().checkpointId
}

func (c *Coordinator) IsActivated() bool {
	return c.activated
}
