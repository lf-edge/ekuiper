// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo"
	"github.com/lf-edge/ekuiper/internal/topo/planner"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/infra"
	"github.com/lf-edge/ekuiper/pkg/schedule"
)

const (
	RuleStarted    = "Running"
	RuleStopped    = "Stopped: canceled manually."
	RuleTerminated = "Stopped: schedule terminated."
	RuleWait       = "Stopped: waiting for next schedule."
)

type ActionSignal int

const (
	ActionSignalStart ActionSignal = iota
	ActionSignalStop
)

type cronInterface interface {
	Start()
	AddFunc(spec string, cmd func()) (cron.EntryID, error)
	Remove(id cron.EntryID)
}

var backgroundCron cronInterface

func init() {
	if !conf.IsTesting {
		backgroundCron = cron.New()
	} else {
		backgroundCron = &MockCron{}
	}
	backgroundCron.Start()
}

type cronStateCtx struct {
	cancel  context.CancelFunc
	entryID cron.EntryID
	// isInSchedule indicates the current rule is in scheduled in backgroundCron
	isInSchedule   bool
	startFailedCnt int

	// only used for test
	cron     string
	duration string
}

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
	// 0 stop, 1 running, -1 delete, 2 internal stop, changed in actions
	triggered int
	// temporary storage for topo graph to make sure even rule close, the graph is still available
	topoGraph *api.PrintableTopo
	sync.RWMutex
	cronState cronStateCtx

	lastStartTimestamp int64
	lastStopTimestamp  int64
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
	rs.run()
	if tp, err := planner.Plan(rule); err != nil {
		return rs, err
	} else {
		rs.Topology = tp
		return rs, nil
	}
}

// UpdateTopo update the rule and the topology AND restart the topology
// Do not need to call restart after update
func (rs *RuleState) UpdateTopo(rule *api.Rule) error {
	t, err := planner.Plan(rule)
	if err != nil {
		return err
	}
	if err := rs.Stop(); err != nil {
		return err
	}
	time.Sleep(1 * time.Millisecond)
	rs.Rule = rule
	rs.Topology = t
	// If not triggered, just ignore start the rule
	if rule.Triggered {
		if err := rs.Start(); err != nil {
			return err
		}
	}
	return nil
}

// only used for unit test
var ignoreSignal = false

// Run start to run the two loops, do not access any changeable states
func (rs *RuleState) run() {
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

			if ignoreSignal {
				continue
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
		var er error
		ticker := time.NewTicker(time.Duration(d) * time.Millisecond)
		defer ticker.Stop()
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
					d = int(math.Round(float64(d) * ((rand.Float64()*2-1)*option.JitterFactor + 1)))
					// make sure d is always in range
					for d <= 0 || d > option.MaxDelay {
						d = int(math.Round(float64(d) * ((rand.Float64()*2-1)*option.JitterFactor + 1)))
					}
					conf.Log.Infof("Rule %s will restart with jitterred delay %d", rs.RuleId, d)
				} else {
					conf.Log.Infof("Rule %s will restart with delay %d", rs.RuleId, d)
				}
				// retry after delay
				select {
				case <-ticker.C:
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
		if rs.triggered != -1 {
			rs.triggered = 0
			if rs.Topology != nil {
				rs.topoGraph = rs.Topology.GetTopo()
			}
			rs.ActionCh <- ActionSignalStop
		}

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
	if rs.Rule.IsLongRunningScheduleRule() {
		isIn, err := schedule.IsInScheduleRanges(conf.GetNow(), rs.Rule.Options.CronDatetimeRange)
		if err != nil {
			return err
		}
		// When rule is created, we need to check its schedule range before start it.
		if !isIn {
			rs.triggered = 2
			return nil
		}
	}
	if rs.Rule.IsScheduleRule() {
		return rs.startScheduleRule()
	}
	return rs.start()
}

// startScheduleRule will register the job in the backgroundCron to run.
// Job will do following 2 things:
// 1. start the rule in cron if else the job is already stopped
// 2. after the rule started, start an extract goroutine to stop the rule after specific duration
func (rs *RuleState) startScheduleRule() error {
	if rs.cronState.isInSchedule {
		return fmt.Errorf("rule %s is already in schedule", rs.RuleId)
	}
	d, err := time.ParseDuration(rs.Rule.Options.Duration)
	if err != nil {
		return err
	}
	var cronCtx context.Context
	cronCtx, rs.cronState.cancel = context.WithCancel(context.Background())
	now := conf.GetNow()
	isInRunningSchedule, remainedDuration, err := rs.isInRunningSchedule(now, d)
	if err != nil {
		return err
	}
	if isInRunningSchedule {
		if err := rs.runScheduleRule(); err != nil {
			return err
		}
		rs.stopAfterDuration(remainedDuration, cronCtx)
	}
	entryID, err := backgroundCron.AddFunc(rs.Rule.Options.Cron, func() {
		var started bool
		var err error
		if started, err = func() (bool, error) {
			switch backgroundCron.(type) {
			case *MockCron:
				// skip mutex if this is a unit test
			default:
				rs.Lock()
				defer rs.Unlock()
			}
			now := conf.GetNow()
			allowed, err := rs.isInAllowedTimeRange(now)
			if err != nil {
				return false, err
			}
			if !allowed {
				return false, nil
			}

			rs.cronState.cron = rs.Rule.Options.Cron
			rs.cronState.duration = rs.Rule.Options.Duration
			return true, rs.start()
		}(); err != nil {
			rs.Lock()
			rs.cronState.startFailedCnt++
			rs.Unlock()
			conf.Log.Errorf(err.Error())
			return
		}
		if started {
			rs.stopAfterDuration(d, cronCtx)
		}
	})
	if err != nil {
		return err
	}
	rs.cronState.isInSchedule = true
	rs.cronState.entryID = entryID
	return nil
}

func (rs *RuleState) runScheduleRule() error {
	rs.cronState.cron = rs.Rule.Options.Cron
	rs.cronState.duration = rs.Rule.Options.Duration
	err := rs.start()
	if err != nil {
		return err
	}
	return nil
}

// stopAfterDuration only for schedule rule
func (rs *RuleState) stopAfterDuration(d time.Duration, cronCtx context.Context) {
	after := time.After(d)
	go func(ctx context.Context) {
		select {
		case <-after:
			rs.Lock()
			defer rs.Unlock()
			if err := rs.internalStop(); err != nil {
				conf.Log.Errorf("close rule %s failed, err: %v", rs.RuleId, err)
			}
			return
		case <-cronCtx.Done():
			return
		}
	}(cronCtx)
}

func (rs *RuleState) start() error {
	if rs.triggered != 1 {
		// If the rule has been stopped due to error, the topology is not nil
		if rs.Topology != nil {
			if rs.Topology.HasOpen() {
				rs.Topology.Cancel()
				if tp, err := planner.Plan(rs.Rule); err != nil {
					return err
				} else {
					rs.Topology = tp
				}
			}
		}
		rs.triggered = 1
	}
	if rs.Rule.IsScheduleRule() || rs.Rule.IsLongRunningScheduleRule() {
		conf.Log.Debugf("rule %v started", rs.RuleId)
	}
	rs.lastStartTimestamp = time.Now().UnixMilli()
	rs.ActionCh <- ActionSignalStart
	return nil
}

// Stop remove the Topology
func (rs *RuleState) Stop() error {
	rs.Lock()
	defer rs.Unlock()
	if rs.Rule.IsScheduleRule() || rs.Rule.IsLongRunningScheduleRule() {
		conf.Log.Debugf("rule %v manual stopped", rs.RuleId)
	}
	rs.stopScheduleRule()
	return rs.stop()
}

func (rs *RuleState) stopScheduleRule() {
	if rs.Rule.IsScheduleRule() && rs.cronState.isInSchedule {
		rs.cronState.isInSchedule = false
		if rs.cronState.cancel != nil {
			rs.cronState.cancel()
		}
		rs.cronState.startFailedCnt = 0
		backgroundCron.Remove(rs.cronState.entryID)
	}
}

func (rs *RuleState) stop() error {
	if rs.triggered == -1 {
		return fmt.Errorf("rule %s is already deleted", rs.RuleId)
	}
	rs.triggered = 0
	if rs.Topology != nil {
		rs.Topology.Cancel()
		// de-reference old Topology in order to release data memory
		rs.Topology = rs.Topology.NewTopoWithSucceededCtx()
	}
	rs.lastStopTimestamp = time.Now().UnixMilli()
	rs.ActionCh <- ActionSignalStop
	return nil
}

func (rs *RuleState) InternalStop() error {
	rs.Lock()
	defer rs.Unlock()
	if !rs.Rule.IsLongRunningScheduleRule() {
		err := fmt.Errorf("rule %v isn't allowed to execute Internal stop as it's not long running schedule rule", rs.RuleId)
		conf.Log.Errorf(err.Error())
		return err
	} else {
		conf.Log.Debugf("rule %v internal stopped", rs.RuleId)
	}
	return rs.internalStop()
}

func (rs *RuleState) internalStop() error {
	if rs.triggered == -1 {
		return fmt.Errorf("rule %s is already deleted", rs.RuleId)
	}
	rs.triggered = 2
	if rs.Topology != nil {
		rs.Topology.Cancel()
		// de-reference old Topology in order to release data memory
		rs.Topology = rs.Topology.NewTopoWithSucceededCtx()
	}
	rs.ActionCh <- ActionSignalStop
	return nil
}

func (rs *RuleState) Close() error {
	rs.Lock()
	defer rs.Unlock()
	if rs.Topology != nil {
		rs.Topology.RemoveMetrics()
	}
	if rs.triggered == 1 && rs.Topology != nil {
		rs.Topology.Cancel()
	}
	rs.triggered = -1
	rs.stopScheduleRule()
	close(rs.ActionCh)
	return nil
}

func (rs *RuleState) GetState() (string, error) {
	rs.RLock()
	defer rs.RUnlock()
	result := ""
	if rs.Topology == nil {
		result = "Stopped: fail to create the topo."
	} else {
		c := (*rs.Topology).GetContext()
		if c != nil {
			err := c.Err()
			switch err {
			case nil:
				result = RuleStarted
			case context.Canceled:
				result = rs.getStoppedRuleState()
			case context.DeadlineExceeded:
				result = "Stopped: deadline exceed."
			default:
				result = fmt.Sprintf("Stopped: %v.", err)
			}
		} else {
			result = rs.getStoppedRuleState()
		}
	}
	if rs.Rule.IsScheduleRule() && rs.cronState.startFailedCnt > 0 {
		result = result + fmt.Sprintf(" Start failed count: %v.", rs.cronState.startFailedCnt)
	}
	return result, nil
}

func (rs *RuleState) getStoppedRuleState() (result string) {
	if schedule.IsAfterTimeRanges(conf.GetNow(), rs.Rule.Options.CronDatetimeRange) {
		result = RuleTerminated
	} else if rs.cronState.isInSchedule {
		result = RuleWait
	} else if rs.triggered == 0 || rs.triggered == -1 {
		result = RuleStopped
	} else if rs.triggered == 2 {
		result = RuleWait
	}
	return result
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

func (rs *RuleState) isInRunningSchedule(now time.Time, d time.Duration) (bool, time.Duration, error) {
	allowed, err := rs.isInAllowedTimeRange(now)
	if err != nil {
		return false, 0, err
	}
	if !allowed {
		return false, 0, nil
	}
	cronExpr := rs.Rule.Options.Cron
	if strings.HasPrefix(cronExpr, "mock") {
		return false, 0, nil
	}
	return schedule.IsInRunningSchedule(cronExpr, now, d)
}

func (rs *RuleState) isInAllowedTimeRange(now time.Time) (bool, error) {
	return schedule.IsInScheduleRanges(now, rs.Rule.Options.CronDatetimeRange)
}

func (rs *RuleState) GetNextScheduleStartTime() int64 {
	if rs.Rule.IsScheduleRule() && len(rs.Rule.Options.Cron) > 0 {
		isIn, err := schedule.IsInScheduleRanges(time.Now(), rs.Rule.Options.CronDatetimeRange)
		if err == nil && isIn {
			s, err := cron.ParseStandard(rs.Rule.Options.Cron)
			if err == nil {
				return s.Next(time.Now()).UnixMilli()
			}
		}
	}
	return 0
}

func (rs *RuleState) GetScheduleTimestamp() (int64, int64, int64) {
	nextStartTimestamp := rs.GetNextScheduleStartTime()
	rs.Lock()
	defer rs.Unlock()
	return rs.lastStartTimestamp, rs.lastStopTimestamp, nextStartTimestamp
}
