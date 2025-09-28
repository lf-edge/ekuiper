package rule

import (
	"fmt"

	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func (s *State) transit(newState RunState, err error) {
	chainAction := false
	defer func() {
		if chainAction {
			s.nextAction()
		}
	}()
	s.currentState = newState
	if err != nil {
		s.lastWill = err.Error()
	}
	switch newState {
	case Running:
		s.lastStartTimestamp = timex.GetNowInMilli()
		chainAction = true
	case Stopped, StoppedByErr, ScheduledStop:
		s.lastStopTimestamp = timex.GetNowInMilli()
		chainAction = true
	default:
		// do nothing
	}
	s.logger.Info(infra.MsgWithStack(fmt.Sprintf("rule %s transit to state %s", s.Rule.Id, StateName[s.currentState])))
}

func (s *State) triggerAction(action ActionSignal) bool {
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

func (s *State) nextAction() {
	var action ActionSignal = -1
	if len(s.actionQ) > 0 {
		action = s.actionQ[0]
		s.actionQ = s.actionQ[1:]
	}
	var err error
	switch action {
	case ActionSignalStart:
		err = s.Start()
	case ActionSignalStop:
		s.Stop()
	case ActionSignalScheduledStart:
		err = s.ScheduleStart()
	case ActionSignalScheduledStop:
		s.ScheduleStop()
	}
	if err != nil {
		s.logger.Error(err)
	}
}
