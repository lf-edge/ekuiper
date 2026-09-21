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
		// Discard the temp topo
		tp.Cancel()
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
			tp.Cancel()
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
// requires ruleLock held by caller
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
		go s.runTopo(s.topology, s.Rule.Id)
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
	// requires ruleLock held by caller
	s.logger.Infof("stopping rule %s", s.Rule.Id)
	if s.topology != nil {
		s.topoGraph = s.topology.GetTopo()
		keys, values := s.topology.GetMetrics()
		s.stoppedMetrics = []any{keys, values}
		err := s.topology.GracefulStop(0)
		if err != nil {
			s.logger.Errorf("graceful stop error, just cancel forcely: %v", err)
		}
		s.topology = nil
	}
	s.transitState(stateType, msg)
}

// This is called async without holding ruleLock.
// It must not access s.topology directly; ownership is decided in cleanRule under lock.
func (s *State) runTopo(tp *topo.Topo, ruleId string) {
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
			s.updateTrigger(ruleId, false)
		}
	}
	// The run exit may be caused by user action or rule itself. Ownership
	// and stopping are serialized in cleanRule under ruleLock (see below).
	s.cleanRule(tp, hasError, lastWill)
}

func (s *State) cleanRule(tp *topo.Topo, hasError bool, lastWill string) {
	s.ruleLock.Lock()
	defer s.ruleLock.Unlock()
	if s.topology != tp {
		// s.topology may have been cleared by a concurrent doStop; never
		// dereference it here. A nil topology means an explicit stop already
		// owned the cleanup, which is the normal path, so stay quiet.
		if s.topology == nil {
			s.logger.Debug("topology already stopped, skip clean up")
			return
		}
		s.logger.Warnf("topology mismatch, skip clean up: %d vs %d", s.topology.GetRunId(), tp.GetRunId())
		return
	}
	// This is the only place an async run stops its topo. The ownership
	// check above and this stop run in the same critical section, so an
	// explicit doStop and a stale runTopo cannot interleave here: whoever
	// holds the lock owns the stop, the loser sees a changed topology and
	// returns above. This matters because Topo.doClose closes shared
	// MergeableTopo sources even when the topo is already closed, so a
	// stale stop could detach schema state of a replacement run.
	// Holding ruleLock across GracefulStop matches the existing doStop path
	// (StopWithLastWill holds the lock while stopping).
	_ = tp.GracefulStop(0)
	if s.topology != nil {
		s.topoGraph = s.topology.GetTopo()
		keys, values := s.topology.GetMetrics()
		s.stoppedMetrics = []any{keys, values}
	}
	s.topology = nil
	if hasError {
		s.transitState(machine.StoppedByErr, lastWill)
	} else {
		s.transitState(machine.Stopped, lastWill)
	}
}
