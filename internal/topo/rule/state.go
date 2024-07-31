// Copyright 2024 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rule

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo"
	"github.com/lf-edge/ekuiper/v2/internal/topo/planner"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type ActionSignal int

const (
	ActionSignalStart ActionSignal = iota
	ActionSignalStop
)

type RunState int

const (
	Stopped RunState = iota
	Starting
	Running
	Stopping
	ScheduledStop
)

var stateName = map[RunState]string{
	Stopped:       "Stopped: ", // normal stop and schedule terminated are here
	Starting:      "Starting",
	Running:       "Running",
	Stopping:      "Stopping",
	ScheduledStop: "Stopped: waiting for next schedule.",
}

// State control the rule RunState
// Created when loading from DB or creating. Deleted when rule deleting
// May be accessed by multiple go routines, receiving concurrent request to change the RunState
type State struct {
	sync.RWMutex
	id string
	// Nearly constant, only change when update the rule
	// It is used to construct topo
	rule   *def.Rule
	logger api.Logger
	// concurrent running states
	currentState RunState
	stateInfo    string
	actionQ      []ActionSignal
	// Sync RunState, do not call in concurrent go routines
	topology    *topo.Topo
	cancelRetry context.CancelFunc
	// temporary storage for topo graph to make sure even rule close, the graph is still available
	topoGraph *def.PrintableTopo
	// Metric RunState
	lastStartTimestamp int64
	lastStopTimestamp  int64
}

func NewState(rule *def.Rule) *State {
	contextLogger := conf.Log.WithField("rule", rule.Id)
	return &State{
		rule:         rule,
		actionQ:      make([]ActionSignal, 0),
		currentState: Stopped,
		logger:       contextLogger,
	}
}

// GetStatus return the current RunState of the rule
// No set is provided, RunState are changed according to the action (start, stop)
func (s *State) GetStatus() (st string) {
	s.RLock()
	defer s.RUnlock()
	switch s.currentState {
	case Stopped:
		if s.topology == nil {
			st = "Stopped: not trigger yet."
		} else {
			c := (*s.topology).GetContext()
			if c != nil {
				e := c.Err()
				switch e {
				case nil:
					st = "Stopped: unknown reason."
				case context.Canceled:
					st = "Stopped: cancel manually."
				case context.DeadlineExceeded:
					st = "Stopped: deadline exceed."
				default:
					st = fmt.Sprintf("Stopped: %v.", e)
				}
			} else {
				st = "Stopped: no context."
			}
		}
	case Running:
		if s.stateInfo != "" {
			st = fmt.Sprintf("%s: %s.", s.currentState, st)
		}
	default:
		st = stateName[s.currentState]
	}
	return
}

func (s *State) Start() (newState RunState, err error) {
	defer s.nextAction()
	s.logger.Debug("start RunState")
	ns, done := s.checkStartState()
	if done {
		return ns, nil
	}
	// do start
	err = s.doStart()
	s.Lock()
	// handle restart strategy
	if err != nil {
		s.currentState = Stopped
	} else {
		s.currentState = Running
	}
	s.Unlock()
	return s.currentState, err
}

func (s *State) checkStartState() (newState RunState, exit bool) {
	s.Lock()
	defer s.Unlock()
	if len(s.actionQ) > 0 {
		if s.actionQ[len(s.actionQ)-1] == ActionSignalStart {
			s.logger.Infof("ignore start action because last action is also start")
			return s.currentState, true
		} else {
			s.actionQ = append(s.actionQ, ActionSignalStart)
			s.logger.Infof("defer start action to action queue")
			return s.currentState, true
		}
	}
	ss := s.currentState
	switch ss {
	case Starting, Running:
		s.logger.Infof("ignore start action, because current RunState is %s", ss)
		return ss, true
	case Stopping:
		s.actionQ = append(s.actionQ, ActionSignalStart)
		s.logger.Infof("defer start action to action queue because current RunState is stopping")
		return ss, true
	case Stopped:
		s.currentState = Starting
		return s.currentState, false
	}
	return
}

// Stop is external stop command, do not call internally
func (s *State) Stop() RunState {
	s.logger.Debug("stop RunState")
	ns, done := s.checkStopState()
	if done {
		return ns
	}
	// do stop, stopping action and starting action are mutual exclusive. No concurrent problem here
	s.doStop()
	// currentState may be accessed concurrently
	s.Lock()
	s.currentState = Stopped
	s.Unlock()
	s.nextAction()
	return Stopped
}

func (s *State) checkStopState() (newState RunState, exit bool) {
	s.Lock()
	defer s.Unlock()
	// If has action queue, ignore or append
	if len(s.actionQ) > 0 {
		if s.actionQ[len(s.actionQ)-1] == ActionSignalStop {
			s.logger.Infof("ignore stop action because last action is also stop")
			return s.currentState, true
		} else {
			s.actionQ = append(s.actionQ, ActionSignalStop)
			s.logger.Infof("defer stop action to action queue")
			return s.currentState, true
		}
	}
	// If no action queue, check current RunState
	ss := s.currentState
	switch ss {
	case Stopping, Stopped:
		s.logger.Infof("ignore stop action, because current RunState is %s", ss)
		return ss, true
	case Starting:
		s.actionQ = append(s.actionQ, ActionSignalStop)
		s.logger.Infof("defer stop action to action queue because current RunState is starting")
		return ss, true
	case Running: // do stop
		s.currentState = Stopping
		return s.currentState, false
	}
	return
}

func (s *State) nextAction() {
	var action ActionSignal = -1
	s.Lock()
	if len(s.actionQ) > 0 {
		action = s.actionQ[0]
		s.actionQ = s.actionQ[1:]
	}
	s.Unlock()
	switch action {
	case ActionSignalStart:
		s.Start()
	case ActionSignalStop:
		s.Stop()
	}
}

// Start/Stop actions are run syncly!!
func (s *State) doStart() error {
	err := infra.SafeRun(func() error {
		if tp, err := planner.Plan(s.rule); err != nil {
			return err
		} else {
			s.topology = tp
			ctx, cancel := context.WithCancel(context.Background())
			s.cancelRetry = cancel
			s.lastStartTimestamp = timex.GetNowInMilli()
			go s.runTopo(ctx, tp, s.rule.Options.RestartStrategy)
		}
		return nil
	})
	return err
}

func (s *State) doStop() {
	if s.cancelRetry != nil {
		s.cancelRetry()
	}
	if s.topology != nil {
		s.topology.Cancel()
		// de-reference old topology in order to release data memory
		s.topology = s.topology.NewTopoWithSucceededCtx()
	}
	s.lastStopTimestamp = timex.GetNowInMilli()
}

// This is called asyncly
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
				if er != nil { // Only restart rule for errors
					tp.GetContext().SetError(er)
					s.logger.Errorf("closing rule for error: %v", er)
					tp.Cancel()
				} else { // exit normally
					return nil
				}
				// Although it is stopped, it is still retrying, so the status is still RUNNING
				s.stateInfo = "retrying after error: " + er.Error()
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
					s.logger.Errorf("stop rule retry as cancelled")
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
	if err != nil { // Exit after retries
		s.logger.Error(err)
		if s.topology != nil {
			s.topoGraph = s.topology.GetTopo()
		}
	}
	s.Lock()
	s.currentState = Stopped
	s.stateInfo = ""
	s.Unlock()
}

// Other APIs

func (s *State) GetTopoGraph() *def.PrintableTopo {
	s.RLock()
	defer s.RUnlock()
	if s.topoGraph != nil {
		return s.topoGraph
	} else if s.topology != nil {
		return s.topology.GetTopo()
	} else {
		return nil
	}
}
