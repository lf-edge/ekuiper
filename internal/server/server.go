// Copyright 2022-2025 EMQ Technologies Co., Ltd.
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
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"go.uber.org/automaxprocs/maxprocs"

	"github.com/lf-edge/ekuiper/v2/internal/binder/function"
	"github.com/lf-edge/ekuiper/v2/internal/binder/io"
	"github.com/lf-edge/ekuiper/v2/internal/binder/meta"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/io/http/httpserver"
	"github.com/lf-edge/ekuiper/v2/internal/keyedstate"
	meta2 "github.com/lf-edge/ekuiper/v2/internal/meta"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/async"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/sig"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store/definition"
	"github.com/lf-edge/ekuiper/v2/internal/plugin/portable/runtime"
	"github.com/lf-edge/ekuiper/v2/internal/processor"
	"github.com/lf-edge/ekuiper/v2/internal/server/bump"
	"github.com/lf-edge/ekuiper/v2/internal/topo/rule"
	"github.com/lf-edge/ekuiper/v2/metrics"
	"github.com/lf-edge/ekuiper/v2/modules/encryptor"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/cert"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
	"github.com/lf-edge/ekuiper/v2/pkg/tracer"
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
	cpuProfiler            = &ekuiperProfile{}
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

func getStoreConfigByKuiperConfig(c *model.KuiperConf) (*store.StoreConf, error) {
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
	conf.SetupEnv()
	conf.InitConf()
	if modules.ConfHook != nil {
		modules.ConfHook(conf.Config)
	}
	if conf.Config.Security != nil {
		if conf.Config.Security.Encryption != nil {
			encryptor.InitConf(conf.Config.Security.Encryption, conf.Config.AesKey)
		}
		if conf.Config.Security.Tls != nil {
			cert.InitConf(conf.Config.Security.Tls)
		}
	}
	// Print inited modules
	for n := range modules.Sources {
		conf.Log.Infof("register source %s", n)
	}
	for n := range modules.Sinks {
		conf.Log.Infof("register sink %s", n)
	}
	for n := range modules.LookupSources {
		conf.Log.Infof("register lookup source %s", n)
	}
	for n := range modules.Converters {
		conf.Log.Infof("register format %s", n)
	}

	serverCtx, serverCancel := context.WithCancel(context.Background())
	if conf.Config.Basic.ResourceProfileConfig.Enable {
		err := StartCPUProfiling(serverCtx, cpuProfiler, conf.Config.Basic.ResourceProfileConfig.Interval)
		conf.Log.Warn(err)
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
	if err := tracer.InitTracer(); err != nil {
		conf.Log.Warn(err)
	} else {
		conf.Log.Infof("tracer init successfully")
	}

	keyedstate.InitKeyedStateKV()

	meta2.InitYamlConfigManager()
	httpserver.InitGlobalServerManager(conf.Config.Source.HttpServerIp, conf.Config.Source.HttpServerPort, conf.Config.Source.HttpServerTls)
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
	meta.Bind()
	sig.InitMQTTControl()
	connection.InitConnectionManager(serverCtx)
	if err := connection.ReloadNamedConnection(); err != nil {
		conf.Log.Warn(err)
	}
	initRuleset()

	registry = &RuleRegistry{internal: make(map[string]*rule.State)}
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
			reply = registry.RecoverRule(rule)
			if 0 != len(reply) {
				logger.Info(reply)
			}
		}
	}
	go runScheduleRuleChecker(serverCtx)
	metrics.InitMetricsDumpJob(serverCtx)
	async.InitManager()

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
	case ss := <-sigint:
		conf.Log.Infof("eKuiper stopped by %v", ss)
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
		waitAllRuleStop()
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
	// kill all plugin process
	runtime.GetPluginInsManager().KillAll()

	// close extend services
	for k, v := range servers {
		logger.Infof("start to close service %s", k)
		v.close()
		logger.Infof("close service %s successfully", k)
	}

	os.Exit(0)
}
