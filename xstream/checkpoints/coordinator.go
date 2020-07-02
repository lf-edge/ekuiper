package checkpoints

import (
	"github.com/benbjohnson/clock"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
	"time"
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
	//TODO serialize state
	return true
}

func (c *pendingCheckpoint) isFullyAck() bool {
	return len(c.notYetAckTasks) == 0
}

func (c *pendingCheckpoint) finalize() *completedCheckpoint {
	ccp := &completedCheckpoint{checkpointId: c.checkpointId}
	return ccp
}

func (c *pendingCheckpoint) dispose(releaseState bool) {
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
	pendingCheckpoints      map[int64]*pendingCheckpoint
	completedCheckpoints    *checkpointStore
	ruleId                  string
	baseInterval            int
	timeout                 int
	advanceToEndOfEventTime bool
	ticker                  *clock.Ticker //For processing time only
	signal                  chan *Signal
	store                   Store
	ctx                     api.StreamContext
}

func NewCoordinator(ruleId string, sources []StreamTask, operators []NonSourceTask, sinks []NonSourceTask, qos xsql.Qos, store Store, interval int, ctx api.StreamContext) *Coordinator {
	signal := make(chan *Signal, 1024)
	var allResponders, sourceResponders []Responder
	for _, r := range sources {
		re := NewResponderExecutor(signal, r)
		allResponders = append(allResponders, re)
		sourceResponders = append(sourceResponders, re)
	}
	for _, r := range operators {
		re := NewResponderExecutor(signal, r)
		handler := createBarrierHandler(re, r.GetInputCount(), qos)
		r.InitCheckpoint(handler, qos)
		allResponders = append(allResponders, re)
	}
	for _, r := range sinks {
		re := NewResponderExecutor(signal, r)
		handler := NewBarrierTracker(re, r.GetInputCount())
		r.InitCheckpoint(handler, qos)
		allResponders = append(allResponders, re)
	}
	//5 minutes by default
	if interval <= 0 {
		interval = 5000
	}
	return &Coordinator{
		tasksToTrigger:     sourceResponders,
		tasksToWaitFor:     allResponders,
		pendingCheckpoints: make(map[int64]*pendingCheckpoint),
		completedCheckpoints: &checkpointStore{
			maxNum: 3,
		},
		ruleId:       ruleId,
		signal:       signal,
		baseInterval: interval,
		timeout:      200000,
		store:        store,
		ctx:          ctx,
	}
}

func createBarrierHandler(re Responder, inputCount int, qos xsql.Qos) BarrierHandler {
	if qos == xsql.AtLeastOnce {
		return NewBarrierTracker(re, inputCount)
	} else if qos == xsql.ExactlyOnce {
		return NewBarrierAligner(re, inputCount)
	} else {
		return nil
	}
}

func (c *Coordinator) Activate() error {
	logger := c.ctx.GetLogger()
	if c.ticker != nil {
		c.ticker.Stop()
	}
	c.ticker = common.GetTicker(c.baseInterval)
	tc := c.ticker.C
	go func() {
		for {
			select {
			case <-tc:
				//trigger checkpoint
				//TODO pose max attempt and min pause check for consequent pendingCheckpoints

				// TODO Check if all tasks are running

				//Create a pending checkpoint
				checkpointId := common.GetNowInMilli()
				checkpoint := newPendingCheckpoint(checkpointId, c.tasksToWaitFor)
				logger.Debugf("Create checkpoint %d", checkpointId)
				c.pendingCheckpoints[checkpointId] = checkpoint
				//Let the sources send out a barrier
				for _, r := range c.tasksToTrigger {
					go func() {
						if err := r.TriggerCheckpoint(checkpointId); err != nil {
							logger.Infof("Fail to trigger checkpoint for source %s with error %v", r.GetName(), err)
							c.cancel(checkpointId)
						} else {
							time.Sleep(time.Duration(c.timeout) * time.Microsecond)
							c.cancel(checkpointId)
						}
					}()
				}
			case s := <-c.signal:
				switch s.Message {
				case STOP:
					logger.Debug("Stop checkpoint scheduler")
					if c.ticker != nil {
						c.ticker.Stop()
					}
					return
				case ACK:
					logger.Debugf("Receive ack from %s for checkpoint %d", s.OpId, s.CheckpointId)
					if checkpoint, ok := c.pendingCheckpoints[s.CheckpointId]; ok {
						checkpoint.ack(s.OpId)
						if checkpoint.isFullyAck() {
							c.complete(s.CheckpointId)
						}
					} else {
						logger.Debugf("Receive ack from %s for non existing checkpoint %d", s.OpId, s.CheckpointId)
					}
				case DEC:
					logger.Debugf("Receive dec from %s for checkpoint %d", s.OpId, s.CheckpointId)
					c.cancel(s.CheckpointId)
				}
			case <-c.ctx.Done():
				logger.Infoln("Cancelling coordinator....")
				if c.ticker != nil {
					c.ticker.Stop()
				}
				return
			}
		}
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
	if checkpoint, ok := c.pendingCheckpoints[checkpointId]; ok {
		delete(c.pendingCheckpoints, checkpointId)
		checkpoint.dispose(true)
	} else {
		logger.Debugf("Cancel for non existing checkpoint %d. Just ignored", checkpointId)
	}
}

func (c *Coordinator) complete(checkpointId int64) {
	logger := c.ctx.GetLogger()

	if ccp, ok := c.pendingCheckpoints[checkpointId]; ok {
		err := c.store.SaveCheckpoint(checkpointId)
		if err != nil {
			logger.Infof("Cannot save checkpoint %d due to storage error: %v", checkpointId, err)
			//TODO handle checkpoint error
			return
		}
		c.completedCheckpoints.add(ccp.finalize())
		delete(c.pendingCheckpoints, checkpointId)
		//Drop the previous pendingCheckpoints
		for cid, cp := range c.pendingCheckpoints {
			if cid < checkpointId {
				//TODO revisit how to abort a checkpoint, discard callback
				cp.isDiscarded = true
				delete(c.pendingCheckpoints, cid)
			}
		}
		logger.Debugf("Totally complete checkpoint %d", checkpointId)
	} else {
		logger.Infof("Cannot find checkpoint %d to complete", checkpointId)
	}
}

//For testing
func (c *Coordinator) GetCompleteCount() int {
	return len(c.completedCheckpoints.checkpoints)
}

func (c *Coordinator) GetLatest() int64 {
	return c.completedCheckpoints.getLatest().checkpointId
}
