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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/schedule"
	"github.com/lf-edge/ekuiper/v2/internal/topo"
	"github.com/lf-edge/ekuiper/v2/internal/topo/planner"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
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

// State control the Rule RunState
// Created when loading from DB or creating. Deleted when Rule deleting
// May be accessed by multiple go routines, receiving concurrent request to change the RunState
type State struct {
	sync.RWMutex
	// Nearly constant, only change when update the Rule
	// It is used to construct topo
	Rule   *def.Rule
	logger api.Logger
	// concurrent running states
	currentState RunState
	actionQ      []ActionSignal
	// Sync RunState, do not call in concurrent go routines
	topology    *topo.Topo
	cancelRetry context.CancelFunc
	// temporary storage for topo graph to make sure even Rule close, the graph is still available
	topoGraph *def.PrintableTopo
	// Metric RunState
	lastStartTimestamp int64
	lastStopTimestamp  int64
	lastWill           string
	stoppedMetrics     []any
}

// NewState provision a state instance only.
// Do not plan or run as before. If the Rule is not triggered, do not plan or run.
// When called by recover Rule, expect
func NewState(rule *def.Rule) *State {
	contextLogger := conf.Log.WithField("Rule", rule.Id)
	return &State{
		Rule:         rule,
		actionQ:      make([]ActionSignal, 0),
		currentState: Stopped,
		logger:       contextLogger,
	}
}

func (s *State) WithTopo(topo *topo.Topo) *State {
	s.topology = topo
	return s
}

// Validate is the second level validation
// It tries to plan and return any errors
// Only run when creating the Rule
func (s *State) Validate() error {
	s.Lock()
	defer s.Unlock()
	err := infra.SafeRun(func() error {
		if tp, err := planner.Plan(s.Rule); err != nil {
			return err
		} else {
			s.topology = tp
		}
		return nil
	})
	return err
}

func (s *State) transit(newState RunState, err error) {
	s.Lock()
	defer s.Unlock()
	s.currentState = newState
	if err != nil {
		s.lastWill = err.Error()
	}
	switch newState {
	case Running:
		s.lastStartTimestamp = timex.GetNowInMilli()
	case Stopped, StoppedByErr, ScheduledStop:
		s.lastStopTimestamp = timex.GetNowInMilli()
	default:
		// do nothing
	}
	s.logger.Infof("rule %s transit to state %s", s.Rule.Id, StateName[s.currentState])
}

func (s *State) GetState() RunState {
	s.RLock()
	defer s.RUnlock()
	return s.currentState
}

// GetStatusMessage return the current RunState of the Rule
// No set is provided, RunState are changed according to the action (start, stop)
func (s *State) GetStatusMessage() string {
	s.RLock()
	defer s.RUnlock()
	var result strings.Builder
	result.WriteString("{")
	// Compose status line
	result.WriteString(`"status": "`)
	result.WriteString(StateName[s.currentState])
	result.WriteString(`",`)
	result.WriteString(`"message": "`)
	result.WriteString(strings.ReplaceAll(s.lastWill, `"`, `\"`))
	result.WriteString(`",`)
	// Compose run timing metrics
	result.WriteString(`"lastStartTimestamp": `)
	result.WriteString(strconv.FormatInt(s.lastStartTimestamp, 10))
	result.WriteString(`,`)
	result.WriteString(`"lastStopTimestamp": `)
	result.WriteString(strconv.FormatInt(s.lastStopTimestamp, 10))
	result.WriteString(`,`)
	nextStartTimestamp := s.Rule.GetNextScheduleStartTime()
	result.WriteString(`"nextStartTimestamp": `)
	result.WriteString(strconv.FormatInt(nextStartTimestamp, 10))
	result.WriteString(`,`)
	// Compose metrics
	var (
		keys   []string
		values []any
	)
	if s.topology != nil {
		keys, values = s.topology.GetMetrics()
	} else if len(s.stoppedMetrics) == 2 {
		keys = s.stoppedMetrics[0].([]string)
		values = s.stoppedMetrics[1].([]any)
	}
	if len(keys) > 0 {
		for i, key := range keys {
			result.WriteString(`"`)
			result.WriteString(key)
			result.WriteString(`":`)
			value := values[i]
			v, _ := cast.ToString(value, cast.CONVERT_ALL)
			switch value.(type) {
			case string:
				result.WriteString(`"`)
				result.WriteString(strings.ReplaceAll(v, `"`, `\"`))
				result.WriteString(`"`)
			default:
				result.WriteString(v)
			}
			result.WriteString(`,`)
		}
	}
	stStr := result.String()
	stStr = stStr[:len(stStr)-1] + "}"
	dst := &bytes.Buffer{}
	var status string
	if err := json.Indent(dst, cast.StringToBytes(stStr), "", "  "); err != nil {
		status = stStr
	} else {
		status = dst.String()
	}
	return status
}

func (s *State) GetStatusMap() map[string]any {
	s.RLock()
	defer s.RUnlock()
	result := make(map[string]any, 20)
	result["status"] = StateName[s.currentState]
	result["message"] = s.lastWill
	result["lastStartTimestamp"] = s.lastStartTimestamp
	result["lastStopTimestamp"] = s.lastStopTimestamp
	nextStartTimestamp := s.Rule.GetNextScheduleStartTime()
	result["nextStartTimestamp"] = nextStartTimestamp
	// Compose metrics
	var (
		keys   []string
		values []any
	)
	if s.topology != nil {
		keys, values = s.topology.GetMetrics()
	} else if len(s.stoppedMetrics) == 2 {
		keys = s.stoppedMetrics[0].([]string)
		values = s.stoppedMetrics[1].([]any)
	}
	if len(keys) > 0 {
		for i, key := range keys {
			result[key] = values[i]
		}
	}
	return result
}

// Start run start or add the start action to queue
// By check state, it assures only one Start function is running at any time. (thread safe)
// regSchedule: whether need to handle scheduler. If call externally, set it to true
func (s *State) Start() error {
	defer s.nextAction()
	s.logger.Debug("start RunState")
	done := s.triggerAction(ActionSignalStart)
	if done {
		return nil
	}
	// delegate to rule patrol checker
	if s.Rule.IsScheduleRule() {
		s.transit(ScheduledStop, nil)
		return nil
	}
	// Start normally or start in schedule period Rule
	// doStart trigger the Rule run. If no trigger error, the Rule will run async and control the state by itself
	s.logger.Infof("start to run rule %s", s.Rule.Id)
	err := s.doStart()
	if err != nil {
		s.transit(StoppedByErr, err)
		return err
	} else {
		s.transit(Running, nil)
	}
	return nil
}

func (s *State) ScheduleStart() error {
	defer s.nextAction()
	s.logger.Debug("scheduled start RunState")
	done := s.triggerAction(ActionSignalScheduledStart)
	if done {
		return nil
	}
	// doStart trigger the Rule run. If no trigger error, the Rule will run async and control the state by itself
	s.logger.Infof("schedule to run rule %s", s.Rule.Id)
	err := s.doStart()
	if err != nil {
		s.transit(StoppedByErr, err)
		return err
	} else {
		s.transit(Running, nil)
	}
	return nil
}

func (s *State) triggerAction(action ActionSignal) bool {
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

// Stop run stop action or add the stop action to queue
// regSchedule: whether need to handle scheduler. If call externally, set it to true
func (s *State) Stop() {
	defer s.nextAction()
	s.logger.Debug("stop RunState")
	done := s.triggerAction(ActionSignalStop)
	if done {
		return
	}
	// do stop, stopping action and starting action are mutual exclusive. No concurrent problem here
	s.logger.Infof("stopping rule %s", s.Rule.Id)
	err := s.doStop()
	if err == nil {
		err = errors.New("canceled manually")
	}
	// currentState may be accessed concurrently
	s.transit(Stopped, err)
	return
}

func (s *State) ScheduleStop() {
	defer s.nextAction()
	s.logger.Debug("scheduled stop RunState")
	done := s.triggerAction(ActionSignalScheduledStop)
	if done {
		return
	}
	// do stop, stopping action and starting action are mutual exclusive. No concurrent problem here
	s.logger.Infof("schedule to stop rule %s", s.Rule.Id)
	err := s.doStop()
	// currentState may be accessed concurrently
	if schedule.IsAfterTimeRanges(timex.GetNow(), s.Rule.Options.CronDatetimeRange) {
		s.transit(ScheduledStop, errors.New("schedule terminated"))
	} else {
		s.transit(ScheduledStop, err)
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
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		s.cancelRetry = cancel
		s.lastStartTimestamp = timex.GetNowInMilli()
		s.lastWill = ""
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
		s.topology.Cancel()
		s.topology.WaitClose()
		s.topology = nil
		return e
	}
	return nil
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
				if er != nil && !errorx.IsEOF(er) { // Only restart Rule for errors
					tp.GetContext().SetError(er)
					s.logger.Errorf("closing Rule for error: %v", er)
					tp.Cancel()
				} else { // exit normally
					if errorx.IsEOF(er) {
						s.lastWill = "done"
					}
					tp.Cancel()
					return nil
				}
				// Although it is stopped, it is still retrying, so the status is still RUNNING
				s.lastWill = "retrying after error: " + er.Error()
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
	if err != nil { // Exit after retries
		s.logger.Error(err)
		if s.topology != nil {
			s.topoGraph = s.topology.GetTopo()
			keys, values := s.topology.GetMetrics()
			s.stoppedMetrics = []any{keys, values}
			s.topology = nil
		}
		s.transit(StoppedByErr, err)
	} else {
		s.transit(Stopped, err)
	}
}

// Other APIs

func (s *State) GetTopoGraph() *def.PrintableTopo {
	s.RLock()
	defer s.RUnlock()
	if s.topology != nil {
		return s.topology.GetTopo()
	} else {
		return s.topoGraph
	}
}

func (s *State) Delete() (err error) {
	defer func() {
		if err != nil {
			if _, ok := err.(errorx.ErrorWithCode); !ok {
				err = errorx.NewWithCode(errorx.RuleErr, err.Error())
			}
		}
	}()
	s.Lock()
	defer s.Unlock()
	if s.topology != nil {
		s.topology.RemoveMetrics()
		s.topology.Cancel()
		s.topology.WaitClose()
	}
	return nil
}

func (s *State) GetMetrics() ([]string, []any) {
	s.RLock()
	defer s.RUnlock()
	if s.topology != nil {
		return s.topology.GetMetrics()
	}
	return nil, nil
}

func (s *State) GetStreams() []string {
	s.RLock()
	defer s.RUnlock()
	if s.topology != nil {
		return s.topology.GetStreams()
	}
	return nil
}

func (s *State) GetLastWill() string {
	s.RLock()
	defer s.RUnlock()
	return s.lastWill
}

func (s *State) ResetStreamOffset(name string, input map[string]any) error {
	s.RLock()
	defer s.RUnlock()
	if s.topology != nil {
		return s.topology.ResetStreamOffset(name, input)
	}
	return fmt.Errorf("topo is not initialized, check rule status")
}
