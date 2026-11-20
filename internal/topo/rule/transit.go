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
		err = s.doStart()
	case machine.ActionSignalStop:
		s.doStop(machine.Stopped, "canceled manually")
	case machine.ActionSignalScheduledStart:
		s.logger.Infof("schedule to run rule %s", s.Rule.Id)
		err = s.doStart()
	case machine.ActionSignalScheduledStop:
		s.doStop(machine.ScheduledStop, "canceled manually")
	}
	if err != nil {
		s.logger.Error(err)
	}
}
