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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo"
	kctx "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/rule/machine"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/syncx"
)

// State control the Rule RunState
// Created when loading from DB or creating. Deleted when Rule deleting
// May be accessed by multiple go routines, receiving concurrent request to change the RunState
type State struct {
	// A singleton for state, create at new and never change
	ruleLock syncx.RWMutex
	// Nearly constant, only change when update the Rule
	// It is used to construct topo
	Rule          *def.Rule
	logger        api.Logger
	updateTrigger func(string, bool)
	// The physical rule instance for each **run**. control the lifecycle in State.
	topology *topo.Topo
	// temporary storage for topo graph to make sure even Rule close, the graph is still available
	topoGraph *def.PrintableTopo
	// Metric RunState
	stoppedMetrics []any
	// State machine
	sm machine.StateMachine
}

// NewState provision a state instance only.
// Do not plan or run as before. If the Rule is not triggered, do not plan or run.
// When called by recover Rule, expect
func NewState(rule *def.Rule, updateTriggerFunc func(string, bool)) *State {
	contextLogger := conf.Log.WithField("Rule", rule.Id)
	return &State{
		Rule:          rule,
		sm:            machine.NewStateMachine(contextLogger),
		logger:        contextLogger,
		updateTrigger: updateTriggerFunc,
	}
}

// ValidateAndRun tries to set up the rule in an atomic way
// It is the only way to update the state rule.
// 1. validate the new rule
// 2. set the state rule property and create the new topo
// 3. stop and clean the old topo if any
// 4. run the new topo
// Notice that, the return err is VALIDATION error only. Run error is async and checked from rule status
// Topo side effect: 1.This function will create and store a new topo if no validation error
// 2. If there is validation error, this function will destroy the new topo
func (s *State) ValidateAndRun(newRule *def.Rule) error {
	s.ruleLock.Lock()
	defer s.ruleLock.Unlock()
	return s.doValidateAndRun(newRule)
}

func (s *State) Bootstrap() error {
	s.ruleLock.Lock()
	defer s.ruleLock.Unlock()
	s.Rule.Triggered = true
	return s.doValidateAndRun(s.Rule)
}

// Start run start or add the start action to queue
// By check state, it assures only one Start function is running at any time. (thread safe)
// regSchedule: whether need to handle scheduler. If call externally, set it to true
func (s *State) Start() error {
	done := s.sm.TriggerAction(machine.ActionSignalStart)
	if done {
		return nil
	}
	s.ruleLock.Lock()
	defer s.ruleLock.Unlock()
	// delegate to rule patrol checker
	if s.Rule.IsScheduleRule() {
		s.transitState(machine.ScheduledStop, "")
		return nil
	}
	return s.doStart()
}

func (s *State) ScheduleStart() error {
	done := s.sm.TriggerAction(machine.ActionSignalScheduledStart)
	if done {
		return nil
	}
	s.ruleLock.Lock()
	defer s.ruleLock.Unlock()
	// doStart trigger the Rule run. If no trigger error, the Rule will run async and control the state by itself
	s.logger.Infof("schedule to run rule %s", s.Rule.Id)
	return s.doStart()
}

// Stop run stop action or add the stop action to queue
// regSchedule: whether need to handle scheduler. If call externally, set it to true
func (s *State) Stop() {
	s.StopWithLastWill("canceled manually")
}

func (s *State) ScheduleStop() {
	s.logger.Debug("scheduled stop RunState")
	done := s.sm.TriggerAction(machine.ActionSignalScheduledStop)
	if done {
		return
	}
	s.ruleLock.Lock()
	defer s.ruleLock.Unlock()
	// do stop, stopping action and starting action are mutual exclusive. No concurrent problem here
	s.logger.Infof("schedule to stop rule %s", s.Rule.Id)
	s.doStop(machine.ScheduledStop, "schedule terminated")
}

func (s *State) StopWithLastWill(msg string) {
	done := s.sm.TriggerAction(machine.ActionSignalStop)
	if done {
		return
	}
	s.ruleLock.Lock()
	defer s.ruleLock.Unlock()
	s.doStop(machine.Stopped, msg)
}

func (s *State) Delete() {
	s.ruleLock.Lock()
	defer s.ruleLock.Unlock()
	if s.topology != nil {
		s.topology.Cancel()
		s.topology.RemoveMetrics()
		s.topology = nil
	}
}

func (s *State) GetState() machine.RunState {
	return s.sm.CurrentState()
}

func (s *State) GetStartTimestamp() time.Time {
	return time.UnixMilli(s.sm.LastStartTimestamp())
}

func (s *State) GetSchema() (map[string]*ast.JsonStreamField, error) {
	s.ruleLock.RLock()
	defer s.ruleLock.RUnlock()
	if s.topology != nil {
		return s.topology.GetSinkSchema(), nil
	}
	return nil, errorx.New(fmt.Sprintf("Fail to get rule %s's topo, make sure the rule has been started before", s.Rule.Id))
}

// GetStatusMessage return the current RunState of the Rule
// No set is provided, RunState are changed according to the action (start, stop)
func (s *State) GetStatusMessage() string {
	s.ruleLock.RLock()
	defer s.ruleLock.RUnlock()
	var result strings.Builder
	result.WriteString("{")
	// Compose status line
	result.WriteString(`"status": "`)
	result.WriteString(s.sm.CurrentStateName())
	result.WriteString(`",`)
	result.WriteString(`"message": `)
	result.WriteString(fmt.Sprintf("%q", s.sm.LastWill()))
	result.WriteString(`,`)
	// Compose run timing metrics
	result.WriteString(`"lastStartTimestamp": `)
	result.WriteString(strconv.FormatInt(s.sm.LastStartTimestamp(), 10))
	result.WriteString(`,`)
	result.WriteString(`"lastStopTimestamp": `)
	result.WriteString(strconv.FormatInt(s.sm.LastStopTimestamp(), 10))
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
	s.ruleLock.RLock()
	defer s.ruleLock.RUnlock()
	result := make(map[string]any, 20)
	result["status"] = s.sm.CurrentStateName()
	result["message"] = s.sm.LastWill()
	result["lastStartTimestamp"] = s.sm.LastStartTimestamp()
	result["lastStopTimestamp"] = s.sm.LastStopTimestamp()
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

func (s *State) GetTopoGraph() *def.PrintableTopo {
	s.ruleLock.RLock()
	defer s.ruleLock.RUnlock()
	if s.topology != nil {
		return s.topology.GetTopo()
	} else {
		return s.topoGraph
	}
}

func (s *State) SetIsTraceEnabled(isEnabled bool, stra kctx.TraceStrategy) error {
	s.ruleLock.Lock()
	defer s.ruleLock.Unlock()
	if s.topology != nil {
		s.topology.EnableTracer(isEnabled, stra)
		return nil
	}
	return fmt.Errorf("rule %s set trace failed due to rule didn't started", s.Rule.Name)
}

func (s *State) IsTraceEnabled() bool {
	s.ruleLock.RLock()
	defer s.ruleLock.RUnlock()
	if s.topology != nil {
		return s.topology.IsTraceEnabled()
	}
	return false
}

func (s *State) GetMetrics() ([]string, []any) {
	s.ruleLock.RLock()
	defer s.ruleLock.RUnlock()
	if s.topology != nil {
		return s.topology.GetMetrics()
	}
	return nil, nil
}

func (s *State) GetStreams() []string {
	s.ruleLock.RLock()
	defer s.ruleLock.RUnlock()
	if s.topology != nil {
		return s.topology.GetStreams()
	}
	return nil
}

func (s *State) GetLastWill() string {
	return s.sm.LastWill()
}

func (s *State) ResetStreamOffset(name string, input map[string]any) error {
	s.ruleLock.RLock()
	defer s.ruleLock.RUnlock()
	if s.topology != nil {
		return s.topology.ResetStreamOffset(name, input)
	}
	return fmt.Errorf("topo is not initialized, check rule status")
}
