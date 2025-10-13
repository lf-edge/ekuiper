package machine

import (
	"fmt"
	"sync"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type ActionSignal int

const (
	ActionSignalStart ActionSignal = iota
	ActionSignalStop
	ActionSignalScheduledStart
	ActionSignalScheduledStop
)

type RunState int

const (
	Stopped RunState = iota
	Starting
	Running
	Stopping
	ScheduledStop
	StoppedByErr
)

var StateName = map[RunState]string{
	Stopped:       "stopped", // normal stop and schedule terminated are here
	Starting:      "starting",
	Running:       "running",
	Stopping:      "stopping",
	ScheduledStop: "stopped: waiting for next schedule.",
	StoppedByErr:  "stopped by error",
}

type StateMachine struct {
	sync.RWMutex
	currentState RunState
	actionQ      []ActionSignal
	// Metric RunState
	lastStartTimestamp int64
	lastStopTimestamp  int64
	lastWill           string
	logger             api.Logger
}

func NewStateMachine(logger api.Logger) StateMachine {
	return StateMachine{
		actionQ:      make([]ActionSignal, 0),
		currentState: Stopped,
		logger:       logger,
	}
}

func (s *StateMachine) TriggerAction(action ActionSignal) bool {
	s.Lock()
	defer s.Unlock()
	if len(s.actionQ) > 0 {
		if s.actionQ[len(s.actionQ)-1] == action {
			s.logger.Infof("ignore action %d because last action is the same", action)
			return true
		} else {
			s.actionQ = append(s.actionQ, action)
			s.logger.Infof("defer action %d to action queue", action)
			return true
		}
	}
	ss := s.currentState
	switch action {
	case ActionSignalStart:
		switch ss {
		case Starting, Running, ScheduledStop:
			// s.logger.Infof("ignore start action, because current RunState is %s", StateName[ss])
			return true
		case Stopping:
			s.actionQ = append(s.actionQ, ActionSignalStart)
			s.logger.Infof("defer start action to action queue because current RunState is stopping")
			return true
		case Stopped, StoppedByErr:
			s.currentState = Starting
			return false
		}
	case ActionSignalStop:
		switch ss {
		case Stopped, StoppedByErr:
			s.logger.Infof("ignore stop action, because current RunState is %s", StateName[ss])
			return true
		case Starting, Stopping:
			s.actionQ = append(s.actionQ, action)
			s.logger.Infof("defer stop action to action queue because current RunState is starting")
			return true
		case Running, ScheduledStop: // do stop
			s.currentState = Stopping
			return false
		}
	case ActionSignalScheduledStart:
		switch ss {
		case ScheduledStop, Stopped, StoppedByErr:
			s.currentState = Starting
			return false
		case Starting, Running:
			// s.logger.Infof("ignore schedule start action, because current RunState is %s", StateName[ss])
			return true
		case Stopping:
			s.actionQ = append(s.actionQ, action)
			s.logger.Infof("defer schedule start action to action queue because current RunState is stopping")
			return true
		}
	case ActionSignalScheduledStop:
		switch ss {
		case Running:
			s.currentState = Stopping
			return false
		case ScheduledStop, Stopped, StoppedByErr:
			s.logger.Infof("ignore schedule stop action, because current RunState is %s", StateName[ss])
			return true
		case Starting, Stopping:
			s.actionQ = append(s.actionQ, action)
			s.logger.Infof("defer schedule stop action to action queue because current RunState is %s", StateName[ss])
			return true
		}
	}
	return false
}

func (s *StateMachine) Transit(newState RunState, lastWill string) (chainAction bool) {
	s.Lock()
	defer s.Unlock()
	s.currentState = newState
	s.lastWill = lastWill
	switch newState {
	case Running:
		s.lastStartTimestamp = timex.GetNowInMilli()
		s.lastWill = ""
		chainAction = true
	case Stopped, StoppedByErr, ScheduledStop:
		s.lastStopTimestamp = timex.GetNowInMilli()
		chainAction = true
	default:
		// do nothing
	}
	s.logger.Info(infra.MsgWithStack(fmt.Sprintf("rule transit to state %s", StateName[s.currentState])))
	return
}

func (s *StateMachine) PopAction() ActionSignal {
	s.Lock()
	defer s.Unlock()
	var action ActionSignal = -1
	if len(s.actionQ) > 0 {
		action = s.actionQ[0]
		s.actionQ = s.actionQ[1:]
	}
	return action
}

func (s *StateMachine) LastWill() string {
	s.RLock()
	defer s.RUnlock()
	return s.lastWill
}

func (s *StateMachine) CurrentState() RunState {
	s.RLock()
	defer s.RUnlock()
	return s.currentState
}

func (s *StateMachine) LastStartTimestamp() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.lastStartTimestamp
}

func (s *StateMachine) CurrentStateName() string {
	s.RLock()
	defer s.RUnlock()
	return StateName[s.currentState]
}

func (s *StateMachine) LastStopTimestamp() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.lastStopTimestamp
}
