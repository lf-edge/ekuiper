// Copyright 2022 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo"
	"github.com/lf-edge/ekuiper/internal/topo/planner"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/infra"
	"math"
	"math/rand"
	"sync"
	"time"
)

type ActionSignal int

const (
	ActionSignalStart ActionSignal = iota
	ActionSignalStop
)

/*********
 *  RuleState is created for each rule. Each ruleState runs two loops:
 *  1. action event loop to accept commands, such as start, stop, getStatus, delete
 *  2. topo running loop
 *  Both loops need to access the status, so lock is needed
 */

type RuleState struct {
	// Constant, never change. Channel to send signals to manage connection retry. When deleting the rule, close it.
	RuleId   string
	ActionCh chan ActionSignal
	// Nearly constant, only change when update the rule
	Rule *api.Rule
	// States, create through rule in each rule start
	Topology *topo.Topo
	// 0 stop, 1 start, -1 delete, changed in actions
	triggered int
	// temporary storage for topo graph to make sure even rule close, the graph is still available
	topoGraph *api.PrintableTopo
	sync.RWMutex
}

// NewRuleState Create and initialize a rule state.
// Errors are possible during plan the topo.
// If error happens return immediately without add it to the registry
func NewRuleState(rule *api.Rule) (*RuleState, error) {
	rs := &RuleState{
		RuleId:   rule.Id,
		Rule:     rule,
		ActionCh: make(chan ActionSignal),
	}
	if tp, err := planner.Plan(rule); err != nil {
		return rs, err
	} else {
		rs.Topology = tp
		rs.Run()
		return rs, nil
	}
}

// UpdateTopo update the rule and the topology AND restart the topology
// Do not need to call restart after update
func (rs *RuleState) UpdateTopo(rule *api.Rule) error {
	if tp, err := planner.Plan(rule); err != nil {
		return err
	} else {
		rs.Lock()
		defer rs.Unlock()
		// Update rule
		rs.Rule = rule
		rs.topoGraph = nil
		// Stop the old topo
		if rs.triggered == 1 {
			rs.Topology.Cancel()
			rs.ActionCh <- ActionSignalStop
			// wait a little to make sure the old topo is stopped
			time.Sleep(1 * time.Millisecond)
		}
		// Update the topo and start
		rs.Topology = tp
		rs.triggered = 1
		rs.ActionCh <- ActionSignalStart
		return nil
	}
}

// Run start to run the two loops, do not access any changeable states
func (rs *RuleState) Run() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	// action loop, once start never end until the rule is deleted
	go func() {
		conf.Log.Infof("Start rulestate %s", rs.RuleId)
		for {
			s, opened := <-rs.ActionCh
			if !opened {
				conf.Log.Infof("Stop rulestate %s", rs.RuleId)
				if cancel != nil {
					cancel()
				}
				return
			}
			switch s {
			case ActionSignalStart:
				if ctx != nil {
					conf.Log.Warnf("rule %s is already started", rs.RuleId)
				} else {
					ctx, cancel = context.WithCancel(context.Background())
					go rs.runTopo(ctx)
				}
			case ActionSignalStop:
				// Stop the running loop
				if cancel != nil {
					cancel()
					ctx = nil
					cancel = nil
				} else {
					conf.Log.Warnf("rule %s is already stopped", rs.RuleId)
				}
			}
		}
	}()
}

func (rs *RuleState) runTopo(ctx context.Context) {
	// Load the changeable states once
	rs.Lock()
	tp := rs.Topology
	option := rs.Rule.Options.Restart
	rs.Unlock()
	if tp == nil {
		conf.Log.Warnf("rule %s is not initialized or just stopped", rs.RuleId)
		return
	}
	err := infra.SafeRun(func() error {
		count := 0
		d := option.Delay
		var (
			er error
		)
		for {
			select {
			case e := <-tp.Open():
				er = e
				if er != nil { // Only restart rule for errors
					tp.GetContext().SetError(er)
					conf.Log.Errorf("closing rule %s for error: %v", rs.RuleId, er)
					tp.Cancel()
				} else { // exit normally
					return nil
				}
			}
			if count < option.Attempts {
				if d > option.MaxDelay {
					d = option.MaxDelay
				}
				if option.JitterFactor > 0 {
					d = int(math.Round(float64(d) * ((rand.Float64()*2-1)*0.1 + 1)))
					conf.Log.Infof("Rule %s will restart with jitterred delay %d", rs.RuleId, d)
				} else {
					conf.Log.Infof("Rule %s will restart with delay %d", rs.RuleId, d)
				}
				// retry after delay
				select {
				case <-time.Tick(time.Duration(d) * time.Millisecond):
					break
				case <-ctx.Done():
					conf.Log.Errorf("stop rule %s retry as cancelled", rs.RuleId)
					return nil
				}
				count++
				if option.Multiplier > 0 {
					d = option.Delay * int(math.Pow(option.Multiplier, float64(count)))
				}
			} else {
				return er
			}
		}
	})
	if err != nil { // Exit after retries
		rs.Lock()
		// The only change the state by error
		rs.triggered = 0
		if rs.Topology != nil {
			rs.topoGraph = rs.Topology.GetTopo()
		}
		rs.Topology = nil
		rs.Unlock()
	}
}

// The action functions are state machine.

func (rs *RuleState) Start() error {
	rs.Lock()
	defer rs.Unlock()
	if rs.triggered == -1 {
		return fmt.Errorf("rule %s is already deleted", rs.RuleId)
	}
	if rs.Topology == nil {
		if tp, err := planner.Plan(rs.Rule); err != nil {
			return err
		} else {
			rs.Topology = tp
		}
	} // else start after create
	rs.triggered = 1
	rs.ActionCh <- ActionSignalStart
	return nil
}

// Stop remove the Topology
func (rs *RuleState) Stop() error {
	rs.Lock()
	defer rs.Unlock()
	if rs.triggered == -1 {
		return fmt.Errorf("rule %s is already deleted", rs.RuleId)
	}
	rs.triggered = 0
	if rs.Topology != nil {
		rs.Topology.Cancel()
		rs.Topology = nil
	}
	rs.ActionCh <- ActionSignalStop
	return nil
}

func (rs *RuleState) Close() error {
	rs.Lock()
	defer rs.Unlock()
	if rs.triggered == 1 && rs.Topology != nil {
		rs.Topology.Cancel()
	}
	rs.triggered = -1
	close(rs.ActionCh)
	return nil
}

func (rs *RuleState) GetState() (string, error) {
	rs.RLock()
	defer rs.RUnlock()
	result := ""
	if rs.Topology == nil {
		result = "Stopped: canceled manually or by error."
	} else {
		c := (*rs.Topology).GetContext()
		if c != nil {
			err := c.Err()
			switch err {
			case nil:
				result = "Running"
			case context.Canceled:
				result = "Stopped: canceled by error."
			case context.DeadlineExceeded:
				result = "Stopped: deadline exceed."
			default:
				result = fmt.Sprintf("Stopped: %v.", err)
			}
		} else {
			result = "Stopped: no context found."
		}
	}
	return result, nil
}

func (rs *RuleState) GetTopoGraph() *api.PrintableTopo {
	rs.RLock()
	defer rs.RUnlock()
	if rs.topoGraph != nil {
		return rs.topoGraph
	} else if rs.Topology != nil {
		return rs.Topology.GetTopo()
	} else {
		return nil
	}
}
