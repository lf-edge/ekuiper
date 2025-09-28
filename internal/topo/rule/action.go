package rule

import (
	"errors"
	"fmt"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo"
	"github.com/lf-edge/ekuiper/v2/internal/topo/planner"
	"github.com/lf-edge/ekuiper/v2/internal/topo/rule/machine"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
)

const EOFMessage = "done"

func (s *State) doValidateAndRun(newRule *def.Rule) (err error) {
	if newRule == nil {
		return errors.New("new rule is nil")
	}
	// Try plan with the new json. If err, revert to old rule
	oldRule := s.Rule
	s.Rule = newRule
	defer func() {
		if err != nil {
			s.Rule = oldRule
		}
	}()
	// validateRule only check plan is valid, topology shouldn't be changed before ruleState stop
	tp, err := s.validate()
	if err != nil {
		return err
	}
	// stop the old run
	if s.topology != nil {
		s.doStop(machine.Stopped, "stopped by update")
	}
	// start new rule
	if newRule.Triggered {
		s.topology = tp
		panicOrError := infra.SafeRun(func() error {
			// Start the rule which runs async
			return s.doStart()
		})
		if panicOrError != nil {
			s.logger.Errorf("Rule %s start failed: %s", s.Rule.Id, panicOrError)
		}
	} else {
		e := tp.Cancel()
		if e != nil {
			s.logger.Warnf("clean temp tp %s error: %v", tp.GetName(), err)
		}
	}
	return nil
}

// If validate error, the return tp is clean up and set to nil
func (s *State) validate() (tp *topo.Topo, err error) {
	// Do validation
	if s.topology != nil {
		s.logger.Warn("topology is already exist, should not happen")
	}
	defer func() { // clean topo if error happens
		if err != nil && tp != nil {
			e := tp.Cancel()
			if e != nil {
				s.logger.Warnf("clean invalid tp %s error: %v", tp.GetName(), err)
			}
			tp = nil
		}
	}()
	err = infra.SafeRun(func() error {
		tp, err = planner.Plan(s.Rule)
		return err
	})
	if err != nil {
		return tp, err
	}
	return tp, nil
}

// DoStart runs internally
func (s *State) doStart() error {
	// Start normally or start in schedule period Rule
	// doStart trigger the Rule run. If no trigger error, the Rule will run async and control the state by itself
	s.logger.Infof("start to run rule %s", s.Rule.Id)
	err := infra.SafeRun(func() error {
		if s.topology == nil {
			if tp, err := planner.Plan(s.Rule); err != nil {
				return err
			} else {
				s.topology = tp
				s.topoGraph = s.topology.GetTopo()
			}
		}
		go s.runTopo(s.topology)
		return nil
	})
	if err != nil {
		s.transitState(machine.StoppedByErr, err.Error())
		return err
	} else {
		s.transitState(machine.Running, "")
	}
	return nil
}

func (s *State) doStop(stateType machine.RunState, msg string) {
	s.logger.Infof("stopping rule %s", s.Rule.Id)
	if s.topology != nil {
		s.topoGraph = s.topology.GetTopo()
		keys, values := s.topology.GetMetrics()
		s.stoppedMetrics = []any{keys, values}
		err := s.topology.Cancel()
		if err == nil {
			s.topology.WaitClose()
		}
		s.topology = nil
	}
	s.transitState(stateType, msg)
}

// This is called async
func (s *State) runTopo(tp *topo.Topo) {
	s.logger.Infof("topo %d opens", tp.GetRunId())
	e := <-tp.Open()
	s.logger.Infof("topo %d stops", tp.GetRunId())
	lastWill := ""
	hasError := false
	if errorx.IsUnexpectedErr(e) { // Only restart Rule for errors
		tp.GetContext().SetError(e)
		hasError = true
		s.logger.Errorf("closing Rule for error: %v", e)
	} else {
		// exit normally
		lastWill = "canceled manually"
		if errorx.IsEOF(e) {
			lastWill = EOFMessage
			msg := e.Error()
			if len(msg) > 0 {
				lastWill = fmt.Sprintf("%s: %s", lastWill, msg)
			}
			s.updateTrigger(s.Rule.Id, false)
		}
	}
	// The run exit may be caused by user action or rule itself
	// Only do clean up when it is exit automatically
	if !tp.IsClosed() {
		tp.Cancel()
		s.cleanRule(hasError, lastWill)
	}
}

func (s *State) cleanRule(hasError bool, lastWill string) {
	s.ruleLock.Lock()
	defer s.ruleLock.Unlock()
	if s.topology != nil {
		s.topoGraph = s.topology.GetTopo()
		keys, values := s.topology.GetMetrics()
		s.stoppedMetrics = []any{keys, values}
	}
	if hasError {
		s.transitState(machine.StoppedByErr, lastWill)
	} else {
		s.transitState(machine.Stopped, lastWill)
	}
	s.topology = nil
}
