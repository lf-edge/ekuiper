package rule

import "github.com/lf-edge/ekuiper/v2/internal/topo/rule/machine"

// transitState updates the state machine and returns whether another action should be chained.
// The caller is responsible for processing the chain action if needed.
// This design avoids recursive defer patterns that could cause deep call stacks.
func (s *State) transitState(newState machine.RunState, lastWill string) {
	chainAction := s.sm.Transit(newState, lastWill)
	if chainAction {
		s.nextAction()
	}
}

// nextAction executes queued actions from the state machine iteratively.
// Uses a loop to process all queued actions without recursive defer.
//
// Design notes:
//   - The state machine (StateMachine) serializes actions: only one action runs at a time
//   - TriggerAction() queues new actions if one is already in progress
//   - This function is "lock-free" from the state machine's perspective (no blocking on action queue)
//   - However, ruleLock is still needed to protect shared fields (s.topology, etc.)
//     from concurrent reads by query methods like GetStatusMessage(), GetTopoGraph()
func (s *State) nextAction() {
	// Iteratively process all queued actions
	for {
		action := s.sm.PopAction()
		if action < 0 {
			return // No more actions to execute
		}
		// Acquire ruleLock to protect shared fields from concurrent reader access.
		// Query methods (GetStatusMessage, GetTopoGraph, etc.) hold this lock while reading.
		s.ruleLock.Lock()
		err := s.executeAction(action)
		s.ruleLock.Unlock()
		if err != nil {
			s.logger.Error(err)
		}
	}
}

// executeAction executes a single action. Must be called with ruleLock held.
func (s *State) executeAction(action machine.ActionSignal) error {
	switch action {
	case machine.ActionSignalStart:
		return s.doStart()
	case machine.ActionSignalStop:
		s.doStop(machine.Stopped, "canceled manually")
	case machine.ActionSignalScheduledStart:
		s.logger.Infof("schedule to run rule %s", s.Rule.Id)
		return s.doStart()
	case machine.ActionSignalScheduledStop:
		s.doStop(machine.ScheduledStop, "canceled manually")
	}
	return nil
}
