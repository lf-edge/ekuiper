package checkpoint

import "fmt"

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
	//create
	barrier := &Barrier{
		CheckpointId: checkpointId,
		OpId:         name,
	}
	//broadcast barrier
	re.task.Broadcast(barrier)
	//Save key state to the global state
	err := sctx.Snapshot()
	if err != nil {
		return err
	}
	go func() {
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
	}()
	return nil
}
