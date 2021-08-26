// Copyright 2021 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/store"
	"github.com/lf-edge/ekuiper/internal/plugin"
	"github.com/lf-edge/ekuiper/internal/processor"
	"github.com/lf-edge/ekuiper/internal/service"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"context"
	"fmt"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	logger          = conf.Log
	startTimeStamp  int64
	version         = ""
	ruleProcessor   *processor.RuleProcessor
	streamProcessor *processor.StreamProcessor
	pluginManager   *plugin.Manager
	serviceManager  *service.Manager
)

func StartUp(Version, LoadFileType string) {
	version = Version
	conf.LoadFileType = LoadFileType
	startTimeStamp = time.Now().Unix()
	conf.InitConf()

	err := store.SetupWithKuiperConfig(conf.Config)
	if err != nil {
		panic(err)
	}

	ruleProcessor = processor.NewRuleProcessor()
	streamProcessor = processor.NewStreamProcessor()
	pluginManager, err = plugin.NewPluginManager()
	if err != nil {
		panic(err)
	}
	serviceManager, err = service.GetServiceManager()
	if err != nil {
		panic(err)
	}
	xsql.InitFuncRegisters(serviceManager, pluginManager)

	registry = &RuleRegistry{internal: make(map[string]*RuleState)}

	server := new(Server)
	//Start rules
	if rules, err := ruleProcessor.GetAllRules(); err != nil {
		logger.Infof("Start rules error: %s", err)
	} else {
		logger.Info("Starting rules")
		var reply string
		for _, rule := range rules {
			//err = server.StartRule(rule, &reply)
			reply = recoverRule(rule)
			if 0 != len(reply) {
				logger.Info(reply)
			}
		}
	}

	//Start prometheus service
	var srvPrometheus *http.Server = nil
	if conf.Config.Basic.Prometheus {
		portPrometheus := conf.Config.Basic.PrometheusPort
		if portPrometheus <= 0 {
			logger.Fatal("Miss configuration prometheusPort")
		}
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		srvPrometheus = &http.Server{
			Addr:         fmt.Sprintf("0.0.0.0:%d", portPrometheus),
			WriteTimeout: time.Second * 15,
			ReadTimeout:  time.Second * 15,
			IdleTimeout:  time.Second * 60,
			Handler:      mux,
		}
		go func() {
			if err := srvPrometheus.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Fatal("Listen prometheus error: ", err)
			}
		}()
		msg := fmt.Sprintf("Serving prometheus metrics on port http://localhost:%d/metrics", portPrometheus)
		logger.Infof(msg)
		fmt.Println(msg)
	}

	//Start rest service
	srvRest := createRestServer(conf.Config.Basic.RestIp, conf.Config.Basic.RestPort)
	go func() {
		var err error
		if conf.Config.Basic.RestTls == nil {
			err = srvRest.ListenAndServe()
		} else {
			err = srvRest.ListenAndServeTLS(conf.Config.Basic.RestTls.Certfile, conf.Config.Basic.RestTls.Keyfile)
		}
		if err != nil && err != http.ErrServerClosed {
			logger.Fatal("Error serving rest service: ", err)
		}
	}()

	// Start rpc service
	portRpc := conf.Config.Basic.Port
	ipRpc := conf.Config.Basic.Ip
	rpcSrv := rpc.NewServer()
	err = rpcSrv.Register(server)
	if err != nil {
		logger.Fatal("Format of service Server isn'restHttpType correct. ", err)
	}
	srvRpc := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", ipRpc, portRpc),
		WriteTimeout: time.Second * 15,
		ReadTimeout:  time.Second * 15,
		IdleTimeout:  time.Second * 60,
		Handler:      rpcSrv,
	}
	go func() {
		if err = srvRpc.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("Error serving rpc service:", err)
		}
	}()

	//Startup message
	restHttpType := "http"
	if conf.Config.Basic.RestTls != nil {
		restHttpType = "https"
	}
	msg := fmt.Sprintf("Serving kuiper (version - %s) on port %d, and restful api on %s://%s:%d. \n", Version, conf.Config.Basic.Port, restHttpType, conf.Config.Basic.RestIp, conf.Config.Basic.RestPort)
	logger.Info(msg)
	fmt.Printf(msg)

	//Stop the services
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, syscall.SIGTERM)
	<-sigint

	if err = srvRpc.Shutdown(context.TODO()); err != nil {
		logger.Errorf("rpc server shutdown error: %v", err)
	}
	logger.Info("rpc server shutdown.")

	if err = srvRest.Shutdown(context.TODO()); err != nil {
		logger.Errorf("rest server shutdown error: %v", err)
	}
	logger.Info("rest server successfully shutdown.")

	if srvPrometheus != nil {
		if err = srvPrometheus.Shutdown(context.TODO()); err != nil {
			logger.Errorf("prometheus server shutdown error: %v", err)
		}
		logger.Info("prometheus server successfully shutdown.")
	}

	os.Exit(0)
}
