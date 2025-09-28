package rule

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
	"time"

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
		s.stopOld()
		s.topology = nil
	}
	// start new rule
	if newRule.Triggered {
		s.topology = tp
		go func() {
			panicOrError := infra.SafeRun(func() error {
				// Start the rule which runs async
				return s.Start()
			})
			if panicOrError != nil {
				s.logger.Errorf("Rule %s start failed: %s", s.Rule.Id, panicOrError)
			}
		}()
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

// doStart/doStop actions are run in sync!!
// 1. create topo if not exists
// 2. run topo async
func (s *State) doStart() error {
	err := infra.SafeRun(func() error {
		if s.topology == nil {
			if tp, err := planner.Plan(s.Rule); err != nil {
				return err
			} else {
				s.topology = tp
				s.topoGraph = s.topology.GetTopo()
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		s.cancelRetry = cancel
		go s.runTopo(ctx, s.topology, s.Rule.Options.RestartStrategy)
		return nil
	})
	return err
}

func (s *State) doStop() error {
	if s.cancelRetry != nil {
		s.cancelRetry()
	}
	if s.topology != nil {
		e := s.topology.GetContext().Err()
		s.topoGraph = s.topology.GetTopo()
		keys, values := s.topology.GetMetrics()
		s.stoppedMetrics = []any{keys, values}
		err := s.topology.Cancel()
		if err == nil {
			s.topology.WaitClose()
		}
		s.topology = nil
		return e
	}
	return nil
}

func (s *State) stopOld() {
	done := s.sm.TriggerAction(machine.ActionSignalStop)
	if done {
		return
	}
	// do stop, stopping action and starting action are mutual exclusive. No concurrent problem here
	s.logger.Infof("stopping rule %s", s.Rule.Id)
	lastWill := "stopped by update"
	err := s.doStop()
	if err != nil {
		lastWill = fmt.Sprintf("stopped by update with error: %v", err)
	}
	// currentState may be accessed concurrently
	s.transitState(machine.Stopped, lastWill)
	return
}

// This is called async
func (s *State) runTopo(ctx context.Context, tp *topo.Topo, rs *def.RestartStrategy) {
	err := infra.SafeRun(func() error {
		count := 0
		d := time.Duration(rs.Delay)
		var er error
		ticker := time.NewTicker(d)
		defer ticker.Stop()
		for {
			select {
			case e := <-tp.Open():
				er = e
				if errorx.IsUnexpectedErr(er) { // Only restart Rule for errors
					tp.GetContext().SetError(er)
					s.logger.Errorf("closing Rule for error: %v", er)
					tp.Cancel()
					s.transitState(machine.Stopped, "retrying after error: "+er.Error())
				} else {
					// exit normally
					lastWill := "cancelled manually"
					if errorx.IsEOF(er) {
						lastWill = EOFMessage
						msg := er.Error()
						if len(msg) > 0 {
							lastWill = fmt.Sprintf("%s: %s", lastWill, msg)
						}
						s.updateTrigger(s.Rule.Id, false)
					}
					tp.Cancel()
					s.transitState(machine.Stopped, lastWill)
					return nil
				}
			}
			if count < rs.Attempts {
				if d > time.Duration(rs.MaxDelay) {
					d = time.Duration(rs.MaxDelay)
				}
				if rs.JitterFactor > 0 {
					d = time.Duration(math.Round(float64(d.Milliseconds())*((rand.Float64()*2-1)*rs.JitterFactor+1))) * time.Millisecond
					// make sure d is always in range
					for d <= 0 || d > time.Duration(rs.MaxDelay) {
						d = time.Duration(math.Round(float64(d.Milliseconds())*((rand.Float64()*2-1)*rs.JitterFactor+1))) * time.Millisecond
					}
					s.logger.Infof("Rule will restart with jitterred delay %d", d)
				} else {
					s.logger.Infof("Rule will restart with delay %d", d)
				}
				// retry after delay
				select {
				case <-ticker.C:
					break
				case <-ctx.Done():
					s.logger.Errorf("stop Rule retry as cancelled")
					return nil
				}
				count++
				if rs.Multiplier > 0 {
					d = time.Duration(rs.Delay) * time.Duration(math.Pow(rs.Multiplier, float64(count)))
				}
			} else {
				return er
			}
		}
	})
	s.cleanRule(err)
}

func (s *State) cleanRule(err error) {
	s.ruleLock.Lock()
	defer s.ruleLock.Unlock()
	if s.topology != nil {
		s.topoGraph = s.topology.GetTopo()
		keys, values := s.topology.GetMetrics()
		s.stoppedMetrics = []any{keys, values}
	}
	if err != nil { // Exit after retries
		s.logger.Error(err)
		s.transitState(machine.StoppedByErr, err.Error())
		s.topology = nil
		s.logger.Infof("%s exit by error set tp to nil", s.Rule.Id)
	} else if strings.HasPrefix(s.sm.LastWill(), EOFMessage) {
		// Two case when err is nil; 1. Manually stop 2.EOF
		// Only transit status when EOF. Don't do this for manual stop because the state already changed!
		s.transitState(machine.Stopped, "")
		s.topology = nil
		s.logger.Infof("%s exit eof set tp to nil", s.Rule.Id)
	}
}
