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

package server

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/Rookiecom/cpuprofile"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/schedule"
	"github.com/lf-edge/ekuiper/v2/internal/server/promMetrics"
	"github.com/lf-edge/ekuiper/v2/internal/topo/rule"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func initRuleset() error {
	loc, err := conf.GetDataLoc()
	if err != nil {
		return err
	}
	signalFile := filepath.Join(loc, "initialized")
	if _, err := os.Stat(signalFile); errors.Is(err, os.ErrNotExist) {
		defer os.Create(signalFile)
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

func resetAllRules() error {
	rules, err := ruleProcessor.GetAllRules()
	if err != nil {
		return err
	}
	for _, name := range rules {
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
			case rule.Running:
				runningCount++
				v = RuleRunning
			case rule.StoppedByErr:
				stopCount++
				v = RuleStoppedByError
			default:
				stopCount++
				v = RuleStopped
			}
			promMetrics.SetRuleStatus(id, int(v))
		}
		promMetrics.SetRuleStatusCountGauge(true, runningCount)
		promMetrics.SetRuleStatusCountGauge(false, stopCount)
	}
}

func handleAllScheduleRuleState(now time.Time, rs []ruleWrapper) {
	for _, r := range rs {
		if !r.rule.IsScheduleRule() {
			continue
		}
		if err := handleScheduleRuleState(now, r.rule); err != nil {
			conf.Log.Errorf("handle schedule rule %v state failed, err:%v", r.rule.Id, err)
		}
	}
}

func handleScheduleRuleState(now time.Time, r *def.Rule) error {
	scheduleActionSignal := handleScheduleRule(now, r)
	conf.Log.Debugf("rule %v, sginal: %v", r.Id, scheduleActionSignal)
	switch scheduleActionSignal {
	case scheduleRuleActionStart:
		return registry.scheduledStart(r.Id)
	case scheduleRuleActionStop:
		return registry.scheduledStop(r.Id)
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
)

func handleScheduleRule(now time.Time, r *def.Rule) scheduleRuleAction {
	options := r.Options
	if options == nil {
		return scheduleRuleActionDoNothing
	}
	isInRange, err := schedule.IsInScheduleRanges(now, options.CronDatetimeRange)
	if err != nil {
		conf.Log.Errorf("check rule %v schedule failed, err:%v", r.Id, err)
		return scheduleRuleActionDoNothing
	}
	if !isInRange {
		return scheduleRuleActionStop
	}
	if options.Cron == "" && options.Duration == "" {
		return scheduleRuleActionStart
	}
	isInCron, err := scheduleCronRule(now, options)
	if err != nil {
		conf.Log.Errorf("check rule %v schedule failed, err:%v", r.Id, err)
		return scheduleRuleActionDoNothing
	}
	if isInCron {
		return scheduleRuleActionStart
	}
	return scheduleRuleActionStop
}

func scheduleCronRule(now time.Time, options *def.RuleOption) (bool, error) {
	if len(options.Cron) > 0 && len(options.Duration) > 0 {
		d, err := time.ParseDuration(options.Duration)
		if err != nil {
			return false, err
		}
		isin, _, err := schedule.IsInRunningSchedule(options.Cron, now, d)
		return isin, err
	}
	return false, nil
}

func startCPUProfiling(ctx context.Context) error {
	if err := cpuprofile.StartProfilerAndAggregater(ctx, time.Duration(1000)*time.Millisecond); err != nil {
		return err
	}
	receiveChan := make(chan *cpuprofile.DataSetAggregate, 1024)
	cpuprofile.RegisterTag("rule", receiveChan)
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case data := <-receiveChan:
				// TODO: support query in future
				conf.Log.Debugf("cpu profile data: %v", data)
			}
		}
	}(ctx)
	return nil
}

func waitAllRuleStop() {
	rules, _ := ruleProcessor.GetAllRules()
	for _, r := range rules {
		err := registry.stopAtExit(r)
		if err != nil {
			logger.Warnf("stop rule %s failed, err:%v", r, err)
		}
	}
}
