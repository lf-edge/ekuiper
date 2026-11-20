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
	for {
		var err error
		switch s.sm.PopAction() {
		case machine.ActionSignalStart:
			if !s.Rule.Triggered && s.Rule.IsScheduleRule() {
				continue
			}
			err = s.doStart()
		case machine.ActionSignalStop:
			s.doStop(machine.Stopped, "canceled manually")
		case machine.ActionSignalScheduledStart:
			if !s.Rule.Triggered {
				continue
			}
			s.logger.Infof("schedule to run rule %s", s.Rule.Id)
			err = s.doStart()
		case machine.ActionSignalScheduledStop:
			if !s.Rule.Triggered {
				continue
			}
			s.doStop(machine.ScheduledStop, "canceled manually")
		}
		if err != nil {
			s.logger.Error(err)
		}
		return
	}
}
