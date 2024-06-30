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

package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/Rookiecom/cpuprofile"
	"github.com/sirupsen/logrus"
	"go.uber.org/automaxprocs/maxprocs"

	"github.com/lf-edge/ekuiper/v2/internal/binder/function"
	"github.com/lf-edge/ekuiper/v2/internal/binder/io"
	"github.com/lf-edge/ekuiper/v2/internal/binder/meta"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/keyedstate"
	meta2 "github.com/lf-edge/ekuiper/v2/internal/meta"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/schedule"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store/definition"
	"github.com/lf-edge/ekuiper/v2/internal/processor"
	"github.com/lf-edge/ekuiper/v2/internal/server/bump"
	"github.com/lf-edge/ekuiper/v2/internal/server/promMetrics"
	"github.com/lf-edge/ekuiper/v2/internal/topo/rule"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

var (
	logger                 = conf.Log
	startTimeStamp         int64
	version                = ""
	sysMetrics             *Metrics
	ruleProcessor          *processor.RuleProcessor
	streamProcessor        *processor.StreamProcessor
	rulesetProcessor       *processor.RulesetProcessor
	ruleMigrationProcessor *RuleMigrationProcessor
	stopSignal             chan struct{}
)

// newNetListener allows EdgeX Foundry, protected by OpenZiti to override and obtain a transport
// protected by OpenZiti's zero trust connectivity. See client_edgex.go where this function is
// set in an init() call
var newNetListener = newTcpListener

func newTcpListener(addr string, logger *logrus.Logger) (net.Listener, error) {
	logger.Info("using ListenMode 'http'")
	return net.Listen("tcp", addr)
}

func stopEKuiper() {
	stopSignal <- struct{}{}
}

// Create path if mount an empty dir. For edgeX, all the folders must be created priorly
func createPaths() {
	dataDir, err := conf.GetDataLoc()
	if err != nil {
		panic(err)
	}
	dirs := []string{"uploads", "sources", "sinks", "functions", "services", "services/schemas", "connections"}

	for _, v := range dirs {
		// Create dir if not exist
		realDir := filepath.Join(dataDir, v)
		if _, err := os.Stat(realDir); os.IsNotExist(err) {
			if err := os.MkdirAll(realDir, os.ModePerm); err != nil {
				fmt.Printf("Failed to create dir %s: %v", realDir, err)
			}
		}
	}

	files := []string{"connections/connection.yaml"}
	for _, v := range files {
		// Create dir if not exist
		realFile := filepath.Join(dataDir, v)
		if _, err := os.Stat(realFile); os.IsNotExist(err) {
			if _, err := os.Create(realFile); err != nil {
				fmt.Printf("Failed to create file %s: %v", realFile, err)
			}
		}
	}
}

func getStoreConfigByKuiperConfig(c *conf.KuiperConf) (*store.StoreConf, error) {
	dataDir, err := conf.GetDataLoc()
	if err != nil {
		return nil, err
	}
	sc := &store.StoreConf{
		Type:         c.Store.Type,
		ExtStateType: c.Store.ExtStateType,
		RedisConfig: definition.RedisConfig{
			Host:     c.Store.Redis.Host,
			Port:     c.Store.Redis.Port,
			Password: c.Store.Redis.Password,
			Timeout:  time.Duration(c.Store.Redis.Timeout),
		},
		SqliteConfig: definition.SqliteConfig{
			Path: dataDir,
			Name: c.Store.Sqlite.Name,
		},
		FdbConfig: definition.FdbConfig{
			Path: c.Store.Fdb.Path,
		},
	}
	return sc, nil
}

func StartUp(Version string) {
	version = Version
	startTimeStamp = time.Now().Unix()
	createPaths()
	conf.InitConf()

	serverCtx, serverCancel := context.WithCancel(context.Background())
	if conf.Config.Basic.EnableResourceProfiling {
		startCPUProfiling(serverCtx)
	}

	undo, _ := maxprocs.Set(maxprocs.Logger(conf.Log.Infof))
	defer undo()

	sc, err := getStoreConfigByKuiperConfig(conf.Config)
	if err != nil {
		panic(err)
	}
	err = store.SetupWithConfig(sc)
	if err != nil {
		panic(err)
	}
	if err := bump.InitBumpManager(); err != nil {
		panic(err)
	}
	dataDir, _ := conf.GetDataLoc()
	if err := bump.BumpToCurrentVersion(dataDir); err != nil {
		panic(err)
	}

	keyedstate.InitKeyedStateKV()

	meta2.InitYamlConfigManager()
	ruleProcessor = processor.NewRuleProcessor()
	streamProcessor = processor.NewStreamProcessor()
	rulesetProcessor = processor.NewRulesetProcessor(ruleProcessor, streamProcessor)
	ruleMigrationProcessor = NewRuleMigrationProcessor(ruleProcessor, streamProcessor)
	sysMetrics = NewMetrics()

	// register all extensions
	for k, v := range components {
		logger.Infof("register component %s", k)
		v.register()
	}

	// Bind the source, function, sink
	sort.Sort(entries)
	err = function.Initialize(entries)
	if err != nil {
		panic(err)
	}
	err = io.Initialize(entries)
	if err != nil {
		panic(err)
	}
	connection.InitConnectionManager()
	if err := connection.ReloadConnection(); err != nil {
		conf.Log.Warn(err)
	}
	meta.Bind()
	initRuleset()

	registry = &RuleRegistry{internal: make(map[string]*rule.RuleState)}
	// Start lookup tables
	streamProcessor.RecoverLookupTable()
	// Start rules
	if rules, err := ruleProcessor.GetAllRules(); err != nil {
		logger.Infof("Start rules error: %s", err)
	} else {
		logger.Info("Starting rules")
		var reply string
		for _, name := range rules {
			rule, err := ruleProcessor.GetRuleById(name)
			if err != nil {
				logger.Error(err)
				continue
			}
			// err = server.StartRule(rule, &reply)
			reply = recoverRule(rule)
			if 0 != len(reply) {
				logger.Info(reply)
			}
		}
	}
	go runScheduleRuleChecker(serverCtx)

	// Start rest service
	srvRest := createRestServer(conf.Config.Basic.RestIp, conf.Config.Basic.RestPort, conf.Config.Basic.Authentication)
	go func() {
		var err error
		ln, listenErr := newNetListener(srvRest.Addr, logger)
		if listenErr != nil {
			panic(listenErr)
		}
		if conf.Config.Basic.RestTls == nil {
			err = srvRest.Serve(ln)
		} else {
			err = srvRest.ServeTLS(ln, conf.Config.Basic.RestTls.Certfile, conf.Config.Basic.RestTls.Keyfile)
		}
		if err != nil && err != http.ErrServerClosed {
			logger.Fatal("Error serving rest service: ", err)
		}
	}()

	// Start extend services
	for k, v := range servers {
		logger.Infof("start service %s", k)
		v.serve()
	}

	if conf.Config.Basic.Prometheus {
		promMetrics.RegisterMetrics()
	}
	// Register conf managers
	InitConfManagers()

	// Startup message
	restHttpType := "http"
	if conf.Config.Basic.RestTls != nil {
		restHttpType = "https"
	}
	stopSignal = make(chan struct{})
	msg := fmt.Sprintf("Serving kuiper (version - %s) on port %d, and restful api on %s://%s.", Version, conf.Config.Basic.Port, restHttpType, cast.JoinHostPortInt(conf.Config.Basic.RestIp, conf.Config.Basic.RestPort))
	logger.Info(msg)
	fmt.Println(msg)

	// Stop the services
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, syscall.SIGTERM)
	select {
	case <-sigint:
		conf.Log.Info("eKuiper stopped by SIGTERM")
	case <-stopSignal:
		// sleep 1 sec in order to let stop request got response
		time.Sleep(time.Second)
		conf.Log.Info("eKuiper stopped by Stop request")
	}
	serverCancel()
	// wait rule checker exit
	time.Sleep(10 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(conf.Config.Basic.GracefulShutdownTimeout))
	defer cancel()
	wg := sync.WaitGroup{}
	// wait all service stop
	wg.Add(2)
	go func() {
		conf.Log.Info("start to stop all rules")
		if err := waitAllRuleStop(ctx); err != nil {
			conf.Log.Warnf(err.Error())
		}
		wg.Done()
	}()
	go func() {
		conf.Log.Info("start to stop rest server")
		if err = srvRest.Shutdown(ctx); err != nil {
			logger.Errorf("rest server shutdown error: %v", err)
		}
		logger.Info("rest server successfully shutdown.")
		wg.Done()
	}()
	wg.Wait()

	// close extend services
	for k, v := range servers {
		logger.Infof("start to close service %s", k)
		v.close()
		logger.Infof("close service %s successfully", k)
	}

	os.Exit(0)
}

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
		_ = deleteRule(name)
		_, err := ruleProcessor.ExecDrop(name)
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
			case rule.RuleStarted:
				runningCount++
				v = RuleRunning
			case rule.RuleStopped, rule.RuleTerminated, rule.RuleWait:
				stopCount++
				v = RuleStopped
			default:
				stopCount++
				v = RuleStoppedByError
			}
			promMetrics.SetRuleStatus(id, int(v))
		}
		promMetrics.SetRuleStatusCountGauge(true, runningCount)
		promMetrics.SetRuleStatusCountGauge(false, stopCount)
	}
}

func handleAllScheduleRuleState(now time.Time, rs []ruleWrapper) {
	for _, r := range rs {
		if err := handleScheduleRuleState(now, r.rule, r.state); err != nil {
			conf.Log.Errorf("handle schedule rule %v state failed, err:%v", r.rule.Id, err)
		}
	}
}

func handleScheduleRuleState(now time.Time, r *def.Rule, state string) error {
	scheduleActionSignal := handleScheduleRule(now, r, state)
	conf.Log.Debugf("rule %v origin state: %v, sginal: %v", r.Id, state, scheduleActionSignal)
	switch scheduleActionSignal {
	case scheduleRuleActionStart:
		return startRuleInternal(r.Id)
	case scheduleRuleActionStop:
		stopRuleInternal(r.Id)
	}
	return nil
}

type scheduleRuleAction int

const (
	scheduleRuleActionDoNothing scheduleRuleAction = iota
	scheduleRuleActionStart
	scheduleRuleActionStop
)

func handleScheduleRule(now time.Time, r *def.Rule, state string) scheduleRuleAction {
	options := r.Options
	if options != nil && options.Cron == "" && options.Duration == "" && len(options.CronDatetimeRange) > 0 {
		isInRange, err := schedule.IsInScheduleRanges(now, options.CronDatetimeRange)
		if err != nil {
			conf.Log.Errorf("check rule %v schedule failed, err:%v", r.Id, err)
			return scheduleRuleActionDoNothing
		}
		if isInRange && state == rule.RuleWait && r.Triggered {
			return scheduleRuleActionStart
		} else if !isInRange && state == rule.RuleStarted && r.Triggered {
			return scheduleRuleActionStop
		}
	}
	return scheduleRuleActionDoNothing
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

func waitAllRuleStop(ctx context.Context) error {
	rules, _ := ruleProcessor.GetAllRules()
	for _, r := range rules {
		stopRuleWhenServerStop(r)
	}
	wg := &sync.WaitGroup{}
	m := &sync.Map{}
	for _, r := range rules {
		rs, ok := registry.Load(r)
		if ok {
			m.Store(r, struct{}{})
			wg.Add(1)
			go func() {
				defer func() {
					m.Delete(r)
					wg.Done()
				}()
				rs.Topology.WaitClose()
			}()
		}
	}
	wgCh := make(chan struct{})
	go func() {
		wg.Wait()
		wgCh <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		timeoutRules := make([]string, 0)
		m.Range(func(key, value any) bool {
			timeoutRules = append(timeoutRules, key.(string))
			return true
		})
		return fmt.Errorf("stop rules timeout, remain::%s", timeoutRules)
	case <-wgCh:
		return nil
	}
}
