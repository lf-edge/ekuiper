package rule

import "github.com/lf-edge/ekuiper/v2/internal/topo/rule/machine"

func (s *State) transitState(newState machine.RunState, lastWill string) {
	chainAction := false
	defer func() {
		if chainAction {
			s.nextAction()
		}
	}()
	chainAction = s.sm.Transit(newState, lastWill)
}

func (s *State) nextAction() {
	action := s.sm.PopAction()
	var err error
	switch action {
	case machine.ActionSignalStart:
		err = s.Start()
	case machine.ActionSignalStop:
		s.Stop()
	case machine.ActionSignalScheduledStart:
		err = s.ScheduleStart()
	case machine.ActionSignalScheduledStop:
		s.ScheduleStop()
	}
	if err != nil {
		s.logger.Error(err)
	}
}
