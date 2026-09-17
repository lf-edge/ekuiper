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
	"github.com/cenkalti/backoff/v4"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/schedule"
	"github.com/lf-edge/ekuiper/v2/internal/processor"
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
	return initFromLocWith(loc, rulesetProcessor.ImportForInit, func() backoff.BackOff {
		return backoff.WithMaxRetries(backoff.NewConstantBackOff(100*time.Millisecond), 2)
	})
}

func initFromLocWith(loc string, importRuleset func([]byte) (processor.InitImportResult, error), newBackoff func() backoff.BackOff) error {
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
		var content []byte
		err = backoff.Retry(func() error {
			content, err = os.ReadFile(initFile)
			return err
		}, newBackoff())
		if err != nil {
			conf.Log.Errorf("fail to read init file: %v", err)
			return nil
		}
		conf.Log.Infof("start to initialize ruleset")
		result, err := importRuleset(content)
		if err != nil {
			conf.Log.Errorf("fail to import ruleset: %v", err)
		} else {
			for _, object := range result.Objects {
				switch object.Outcome {
				case processor.InitSkipped:
					conf.Log.Infof("Skip %s %s: existing version is newer or equal", object.Kind, object.Name)
				case processor.InitTerminalError, processor.InitRetryableError:
					conf.Log.Warnf("Fail to import %s %s after %d attempt(s) (retryable=%t): %v", object.Kind, object.Name, object.Attempts, object.Outcome == processor.InitRetryableError, object.Err)
				}
			}
			conf.Log.Infof("initialize %d streams, %d tables and %d rules", result.Counts.Streams, result.Counts.Tables, result.Counts.Rules)
			if result.HasRetryableFailure() {
				conf.Log.Warn("init.json has retryable failures; initialized marker will not be updated")
				return nil
			}
		}
		if err := writeInitialized(loc, updateTime); err != nil {
			conf.Log.Warnf("create new initialized file failed: %v", err)
		}
	}
	return nil
}

func writeInitialized(loc string, updateTime int64) error {
	return writeInitializedWithReadDir(loc, updateTime, os.ReadDir)
}

func writeInitializedWithReadDir(loc string, updateTime int64, readDir func(string) ([]os.DirEntry, error)) error {
	name := fmt.Sprintf("initialized%d", updateTime)
	path := filepath.Join(loc, name)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	created := err == nil
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
		info, statErr := os.Stat(path)
		if statErr != nil {
			return statErr
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("initialized marker %s is not a regular file", path)
		}
	} else if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return err
	}
	entries, err := readDir(loc)
	if err != nil {
		if created {
			_ = os.Remove(path)
		}
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == name || !strings.HasPrefix(entry.Name(), "initialized") {
			continue
		}
		path := filepath.Join(loc, entry.Name())
		if err := os.Remove(path); err != nil {
			conf.Log.Warnf("remove file %s failed: %v", path, err)
		}
	}
	return nil
}

// findInitializedTime finds one files starting with "initialized" and returns
// the int64 suffix value according to the rules:
// - No matching files: -1
// - Matching file with no numeric suffix: 0
// - Otherwise, the int64 suffix value
func findInitializedTime(root string) int64 {
	entries, err := os.ReadDir(root)
	if err != nil {
		conf.Log.Errorf("Error reading initialized directory: %v", err)
		return -1
	}
	var marker string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "initialized") {
			continue
		}
		if marker != "" {
			conf.Log.Warn("multiple initialized markers found; initialization will run again")
			return -1
		}
		marker = entry.Name()
	}
	if marker == "" {
		return -1
	}
	suffix := strings.TrimPrefix(marker, "initialized")
	if suffix == "" {
		return 0
	}
	result, err := strconv.ParseInt(suffix, 10, 64)
	if err != nil {
		return 0
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
		// handle auto restart rules
		if r.rule.Options.RestartStrategy != nil && r.rule.Options.RestartStrategy.Attempts > 0 {
			if r.state == machine.StoppedByErr {
				reply := registry.RecoverRule(r.rule)
				conf.Log.Infof("restart exit rule %v with reply: %s", r.rule.Id, reply)
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
	// Enable window aggregator to allow GetWindowData() to work without panic.
	// Window size of 5 means aggregating data over 5 profiling intervals.
	cpuProfile.EnableWindowAggregator(5)
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
