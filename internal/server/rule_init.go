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

package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Rookiecom/cpuprofile"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/schedule"
	"github.com/lf-edge/ekuiper/v2/internal/topo/rule/machine"
	"github.com/lf-edge/ekuiper/v2/metrics"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func initRuleset() {
	// init data firstly, so that same version can take precedence
	loc, _ := conf.GetDataLoc()
	_ = initFromLoc(loc)
	loc, _ = conf.GetConfLoc()
	_ = initFromLoc(loc)
}

func initFromLoc(loc string) error {
	initFile := filepath.Join(loc, "init.json")
	fileInfo, err := os.Stat(initFile)
	if err != nil {
		conf.Log.Infof("init rules file %s does not exist", initFile)
		return nil
	}
	updateTime := fileInfo.ModTime().UnixMilli()
	lastUpdate := findInitializedTime(loc)
	conf.Log.Infof("found init.json with update time %d and last init time %d", updateTime, lastUpdate)
	// Only leave one initialized file each time. Due to the time shift in some system, compare time is not a good idea
	if updateTime != lastUpdate {
		defer func() {
			// delete all signal files
			ff, err := os.ReadDir(loc)
			if err == nil {
				prefix := "initialized"
				for _, entry := range ff {
					if !entry.IsDir() && strings.HasPrefix(entry.Name(), prefix) {
						path := filepath.Join(loc, entry.Name())
						err = os.Remove(path)
						if err != nil {
							conf.Log.Warnf("remove file %s failed", path)
						}
					}
				}
			}
			// create the unique file
			_, err = os.Create(filepath.Join(loc, fmt.Sprintf("initialized%d", updateTime)))
			if err != nil {
				conf.Log.Warn("create new initialized file failed")
			}
		}()
		content, err := os.ReadFile(filepath.Join(loc, "init.json"))
		if err != nil {
			conf.Log.Errorf("fail to read init file: %v", err)
			return nil
		}
		conf.Log.Infof("start to initialize ruleset")
		_, counts, err := rulesetProcessor.Import(content)
		if err != nil {
			conf.Log.Errorf("fail to import ruleset: %v", err)
			return nil
		}
		conf.Log.Infof("initialzie %d streams, %d tables and %d rules", counts[0], counts[1], counts[2])
	}
	return nil
}

// findInitializedTime finds one files starting with "initialized" and returns
// the int64 suffix value according to the rules:
// - No matching files: -1
// - Matching file with no numeric suffix: 0
// - Otherwise, the int64 suffix value
func findInitializedTime(root string) int64 {
	prefix := "initialized"
	// Walk through the directory tree
	var result int64 = -1 // Default: no files found
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Check if it's a file and starts with "initialized"
		if !info.IsDir() && strings.HasPrefix(info.Name(), prefix) {
			// Extract the suffix after "initialized"
			suffix := strings.TrimPrefix(info.Name(), prefix)
			if suffix == "" {
				result = 0 // No suffix, return 0
			} else {
				// Try to parse the suffix as an int64
				if num, err := strconv.ParseInt(suffix, 10, 64); err == nil {
					result = num // Valid suffix, return it
				} else {
					result = 0 // Invalid suffix treated as no suffix
				}
			}
			return filepath.SkipDir // Stop walking after first match
		}
		return nil
	})
	if err != nil {
		conf.Log.Errorf("Error walking directory: %v\n", err)
		return -1
	}
	return result
}

func resetAllRules() error {
	for _, name := range registry.keys() {
		err := registry.DeleteRule(name)
		if err != nil {
			logger.Warnf("delete rule: %s with error %v", name, err)
			continue
		}
	}
	return nil
}

func resetAllStreams() error {
	allStreams, err := streamProcessor.GetAll()
	if err != nil {
		return err
	}
	Streams := allStreams["streams"]
	Tables := allStreams["tables"]

	for name := range Streams {
		_, err2 := streamProcessor.DropStream(name, ast.TypeStream)
		if err2 != nil {
			logger.Warnf("streamProcessor DropStream %s error: %v", name, err2)
			continue
		}
	}
	for name := range Tables {
		_, err2 := streamProcessor.DropStream(name, ast.TypeTable)
		if err2 != nil {
			logger.Warnf("streamProcessor DropTable %s error: %v", name, err2)
			continue
		}
	}
	return nil
}

func runScheduleRuleCheckerByInterval(d time.Duration, ctx context.Context) {
	conf.Log.Infof("start patroling schedule rule state")
	ticker := time.NewTicker(d)
	defer func() {
		ticker.Stop()
		conf.Log.Infof("exit partoling schedule rule state")
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rs, err := getAllRulesWithState()
			if err != nil {
				conf.Log.Errorf("get all rules with stated failed, err:%v", err)
				continue
			}
			now := timex.GetNow()
			handleAllRuleStatusMetrics(rs)
			handleAllScheduleRuleState(now, rs)
		}
	}
}

func runScheduleRuleChecker(ctx context.Context) {
	runScheduleRuleCheckerByInterval(time.Duration(conf.Config.Basic.RulePatrolInterval), ctx)
}

type RuleStatusMetricsValue int

const (
	RuleStoppedByError RuleStatusMetricsValue = -1
	RuleStopped        RuleStatusMetricsValue = 0
	RuleRunning        RuleStatusMetricsValue = 1
)

func handleAllRuleStatusMetrics(rs []ruleWrapper) {
	if conf.Config != nil && conf.Config.Basic.Prometheus {
		var runningCount int
		var stopCount int
		var v RuleStatusMetricsValue
		for _, r := range rs {
			id := r.rule.Id
			switch r.state {
			case machine.Running:
				runningCount++
				v = RuleRunning
			case machine.StoppedByErr:
				stopCount++
				v = RuleStoppedByError
			default:
				stopCount++
				v = RuleStopped
			}
			metrics.SetRuleStatus(id, int(v))
		}
		metrics.SetRuleStatusCountGauge(true, runningCount)
		metrics.SetRuleStatusCountGauge(false, stopCount)
	}
}

func handleAllScheduleRuleState(now time.Time, rs []ruleWrapper) {
	for _, r := range rs {
		if r.rule.IsScheduleRule() || r.rule.IsDurationRule() {
			if err := handleScheduleRuleState(now, r); err != nil {
				conf.Log.Errorf("handle schedule rule %v state failed, err:%v", r.rule.Id, err)
			}
		}
	}
}

func handleScheduleRuleState(now time.Time, rw ruleWrapper) error {
	scheduleActionSignal := handleScheduleRule(now, rw)
	conf.Log.Debugf("rule %v, sginal: %v", rw.rule.Id, scheduleActionSignal)
	switch scheduleActionSignal {
	case scheduleRuleActionStart:
		return registry.scheduledStart(rw.rule.Id)
	case scheduleRuleActionStop:
		return registry.scheduledStop(rw.rule.Id)
	case doStop:
		return registry.stopAtExit(rw.rule.Id, "duration terminated")
	default:
		// do nothing
	}
	return nil
}

type scheduleRuleAction int

const (
	scheduleRuleActionDoNothing scheduleRuleAction = iota
	scheduleRuleActionStart
	scheduleRuleActionStop
	doStop
)

func handleScheduleRule(now time.Time, rw ruleWrapper) scheduleRuleAction {
	options := rw.rule.Options
	if options == nil {
		return scheduleRuleActionDoNothing
	}
	isInRange, err := schedule.IsInScheduleRanges(now, options.CronDatetimeRange)
	if err != nil {
		conf.Log.Errorf("check rule %v schedule failed, err:%v", rw.rule.Id, err)
		return scheduleRuleActionDoNothing
	}
	if !isInRange {
		return scheduleRuleActionStop
	}
	if options.Cron == "" && options.Duration == "" {
		return scheduleRuleActionStart
	}
	return scheduleCronRuleAction(now, rw)
}

func scheduleCronRuleAction(now time.Time, rw ruleWrapper) scheduleRuleAction {
	options := rw.rule.Options
	if options == nil {
		return scheduleRuleActionDoNothing
	}
	if len(options.Duration) > 0 {
		d, err := time.ParseDuration(options.Duration)
		if err != nil {
			conf.Log.Errorf("check rule %v schedule failed, err:%v", rw.rule.Id, err)
			return scheduleRuleActionDoNothing
		}
		if len(options.Cron) > 0 {
			isin, _, err := schedule.IsInRunningSchedule(options.Cron, now, d)
			if err != nil {
				conf.Log.Errorf("check rule %v schedule failed, err:%v", rw.rule.Id, err)
				return scheduleRuleActionDoNothing
			}
			if isin {
				return scheduleRuleActionStart
			} else {
				return scheduleRuleActionStop
			}

		} else {
			if rw.state == machine.Running && !rw.startTime.IsZero() && now.Sub(rw.startTime) >= d {
				return doStop
			}
		}
	}
	return scheduleRuleActionDoNothing
}

type Profiler interface {
	StartCPUProfiler(context.Context, time.Duration) error
	EnableWindowAggregator(int)
	GetWindowData() cpuprofile.DataSetAggregateMap
	RegisterTag(string, chan *cpuprofile.DataSetAggregate)
}

type ekuiperProfile struct{}

func (e *ekuiperProfile) StartCPUProfiler(ctx context.Context, t time.Duration) error {
	return cpuprofile.StartCPUProfiler(ctx, t)
}

func (e *ekuiperProfile) EnableWindowAggregator(window int) {
	cpuprofile.EnableWindowAggregator(window)
}

func (e *ekuiperProfile) GetWindowData() cpuprofile.DataSetAggregateMap {
	return cpuprofile.GetWindowData()
}

func (e *ekuiperProfile) RegisterTag(tag string, receiveChan chan *cpuprofile.DataSetAggregate) {
	cpuprofile.RegisterTag(tag, receiveChan)
}

func StartCPUProfiling(ctx context.Context, cpuProfile Profiler, interval time.Duration) error {
	recvCh := make(chan *cpuprofile.DataSetAggregate)
	cpuProfile.RegisterTag("rule", recvCh)
	if err := cpuProfile.StartCPUProfiler(ctx, interval); err != nil {
		return err
	}
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case dataset := <-recvCh:
				if dataset == nil {
					return
				}
				for ruleID, cpuTimeMs := range dataset.Stats {
					metrics.AddRuleCPUTime(ruleID, float64(cpuTimeMs)/1000)
				}
			}
		}
	}(ctx)

	return nil
}

func waitAllRuleStop() {
	for _, r := range registry.keys() {
		err := registry.stopAtExit(r, "")
		if err != nil {
			logger.Warnf("stop rule %s failed, err:%v", r, err)
		}
	}
}
