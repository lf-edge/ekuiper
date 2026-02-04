// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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
	"strconv"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/schedule"
	"github.com/lf-edge/ekuiper/v2/internal/topo"
	kctx "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/planner"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/syncx"
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
	syncx.RWMutex
	// Nearly constant, only change when update the Rule
	// It is used to construct topo
	Rule          *def.Rule
	logger        api.Logger
	updateTrigger func(string, bool)
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
func NewState(rule *def.Rule, updateTriggerFunc func(string, bool)) *State {
	contextLogger := conf.Log.WithField("Rule", rule.Id)
	return &State{
		Rule:          rule,
		actionQ:       make([]ActionSignal, 0),
		currentState:  Stopped,
		logger:        contextLogger,
		updateTrigger: updateTriggerFunc,
	}
}

func (s *State) WithTopo(topo *topo.Topo) *State {
	s.Lock()
	defer s.Unlock()
	s.topology = topo
	if topo != nil {
		s.topoGraph = s.topology.GetTopo()
	}
	return s
}

func (s *State) GetRule() *def.Rule {
	s.RLock()
	defer s.RUnlock()
	return s.Rule
}

func (s *State) SetRule(r *def.Rule) {
	s.Lock()
	defer s.Unlock()
	s.Rule = r
}

func (s *State) HasTopo() bool {
	s.RLock()
	defer s.RUnlock()
	return s.topology != nil
}

// Validate tries to plan and return the planned topo and any errors
// Need to cancel the topo if it is of no use because the input/output channels are set
// Otherwise, the shared source may send to these channels and hang
func (s *State) Validate() (*topo.Topo, error) {
	s.Lock()
	defer s.Unlock()
	var (
		tp  *topo.Topo
		err error
	)
	err = infra.SafeRun(func() error {
		tp, err = planner.Plan(s.Rule)
		return err
	})
	if err != nil {
		return nil, err
	}
	return tp, err
}

func (s *State) transit(newState RunState, err error) {
	chainAction := false
	s.Lock()
	defer func() {
		s.Unlock()
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

func (s *State) GetState() RunState {
	s.RLock()
	defer s.RUnlock()
	return s.currentState
}

func (s *State) GetStartTimestamp() time.Time {
	s.RLock()
	defer s.RUnlock()
	return time.UnixMilli(s.lastStartTimestamp)
}

func (s *State) GetSchema() map[string]*ast.JsonStreamField {
	s.RLock()
	defer s.RUnlock()
	if s.topology != nil {
		return s.topology.GetSinkSchema()
	}
	return nil
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
	result.WriteString(`"message": `)
	result.WriteString(fmt.Sprintf("%q", s.lastWill))
	result.WriteString(`,`)
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
				result.WriteString(fmt.Sprintf("%q", v))
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
	r := s.GetRule()
	s.logger.Infof("start to run rule %s", r.Id)
	err := s.doStart(r)
	if err != nil {
		s.transit(StoppedByErr, err)
		return err
	}
	// State will transition to Running in runTopo after Open() completes
	return nil
}

func (s *State) ScheduleStart() error {
	s.logger.Debug("scheduled start RunState")
	done := s.triggerAction(ActionSignalScheduledStart)
	if done {
		return nil
	}
	// doStart trigger the Rule run. If no trigger error, the Rule will run async and control the state by itself
	r := s.GetRule()
	s.logger.Infof("schedule to run rule %s", r.Id)
	err := s.doStart(r)
	if err != nil {
		s.transit(StoppedByErr, err)
		return err
	}
	// State will transition to Running in runTopo after Open() completes
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
	s.StopWithLastWillAndSig("canceled manually", 0)
}

func (s *State) StopWithLastWillAndSig(msg string, sig int) {
	s.logger.Debug("stop RunState")
	done := s.triggerAction(ActionSignalStop)
	if done {
		return
	}
	// do stop, stopping action and starting action are mutual exclusive. No concurrent problem here
	r := s.GetRule()
	s.logger.Infof("stopping rule %s", r.Id)
	err := s.doStopWithSig(r, sig)
	if err == nil {
		err = errors.New(msg)
	}
	// currentState may be accessed concurrently
	s.transit(Stopped, err)
	return
}

func (s *State) ScheduleStop() {
	s.logger.Debug("scheduled stop RunState")
	done := s.triggerAction(ActionSignalScheduledStop)
	if done {
		return
	}
	// do stop, stopping action and starting action are mutual exclusive. No concurrent problem here
	r := s.GetRule()
	s.logger.Infof("schedule to stop rule %s", r.Id)
	err := s.doStop(r)
	// currentState may be accessed concurrently
	if schedule.IsAfterTimeRanges(timex.GetNow(), r.Options.CronDatetimeRange) {
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
func (s *State) doStart(r *def.Rule) error {
	err := infra.SafeRun(func() error {
		s.Lock()
		tp := s.topology
		s.Unlock()
		if tp == nil {
			if tp, err := planner.Plan(r); err != nil {
				return err
			} else {
				s.Lock()
				// Double check
				if s.topology == nil {
					s.topology = tp
					s.topoGraph = s.topology.GetTopo()
				}
				s.Unlock()
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		s.Lock()
		s.cancelRetry = cancel
		s.lastStartTimestamp = timex.GetNowInMilli()
		s.lastWill = ""
		s.Unlock()
		go s.runTopo(ctx, s.topology, r)
		return nil
	})
	return err
}

func (s *State) doStop(r *def.Rule) error {
	return s.doStopWithSig(r, 0)
}

func (s *State) doStopWithSig(r *def.Rule, sig int) error {
	if s.cancelRetry != nil {
		s.cancelRetry()
	}
	s.Lock()
	tp := s.topology
	s.Unlock()
	if tp != nil {
		e := tp.GetContext().Err()
		s.Lock()
		s.topoGraph = tp.GetTopo()
		keys, values := tp.GetMetrics()
		s.stoppedMetrics = []any{keys, values}
		s.Unlock()
		err := tp.CancelWithSig(sig)
		if err == nil {
			tp.WaitClose()
		}
		s.Lock()
		if s.topology == tp {
			s.topology = nil
		}
		s.Unlock()
		return e
	}
	return nil
}

const EOFMessage = "done"

// This is called async
func (s *State) runTopo(ctx context.Context, tp *topo.Topo, r *def.Rule) {
	var err error
	err = infra.SafeRun(func() error {
		drain := tp.Open()
		s.transit(Running, nil)
		select {
		case e := <-drain:
			if errorx.IsUnexpectedErr(e) {
				tp.GetContext().SetError(e)
				s.logger.Errorf("closing Rule for error: %v", e)
				tp.Cancel()
				return e
			} else { // exit normally
				if errorx.IsEOF(e) {
					s.Lock()
					s.lastWill = EOFMessage
					msg := e.Error()
					if len(msg) > 0 {
						s.lastWill = fmt.Sprintf("%s: %s", s.lastWill, msg)
					}
					s.Unlock()
					s.updateTrigger(r.Id, false)
				}
				tp.Cancel()
				return nil
			}
		case <-ctx.Done():
			s.logger.Infof("rule %s context done: %v", r.Id, ctx.Err())
			return nil // Manual stop, not an error
		}
	})

	s.Lock()
	if s.topology != nil && s.topology == tp {
		s.topoGraph = s.topology.GetTopo()
		keys, values := s.topology.GetMetrics()
		s.stoppedMetrics = []any{keys, values}
	}
	s.Unlock()

	if err != nil { // Exit after retries
		s.logger.Error(err)
		s.transit(StoppedByErr, err)
		s.Lock()
		if s.topology == tp {
			s.topology = nil
		}
		s.Unlock()
		s.logger.Infof("%s exit by error set tp to nil", r.Id)
	} else {
		s.RLock()
		lw := s.lastWill
		s.RUnlock()
		if strings.HasPrefix(lw, EOFMessage) {
			// Two case when err is nil; 1. Manually stop 2.EOF
			// Only transit status when EOF. Don't do this for manual stop because the state already changed!
			s.transit(Stopped, nil)
			s.Lock()
			if s.topology == tp {
				s.topology = nil
			}
			s.Unlock()
			s.logger.Infof("%s exit eof set tp to nil", r.Id)
		}
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

func (s *State) SetIsTraceEnabled(isEnabled bool, stra kctx.TraceStrategy) error {
	s.Lock()
	defer s.Unlock()
	if s.topology != nil {
		s.topology.EnableTracer(isEnabled, stra)
		return nil
	}
	return fmt.Errorf("rule %s set trace failed due to rule didn't started", s.Rule.Name)
}

func (s *State) IsTraceEnabled() bool {
	s.Lock()
	defer s.Unlock()
	if s.topology != nil {
		return s.topology.IsTraceEnabled()
	}
	return false
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
