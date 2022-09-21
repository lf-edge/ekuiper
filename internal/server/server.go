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

package server

import (
	"context"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/binder/function"
	"github.com/lf-edge/ekuiper/internal/binder/io"
	"github.com/lf-edge/ekuiper/internal/binder/meta"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/store"
	"github.com/lf-edge/ekuiper/internal/processor"
	"github.com/lf-edge/ekuiper/internal/topo/connection/factory"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"
)

var (
	logger          = conf.Log
	startTimeStamp  int64
	version         = ""
	ruleProcessor   *processor.RuleProcessor
	streamProcessor *processor.StreamProcessor
)

func StartUp(Version, LoadFileType string) {
	version = Version
	conf.LoadFileType = LoadFileType
	startTimeStamp = time.Now().Unix()
	conf.InitConf()
	factory.InitClientsFactory()

	err := store.SetupWithKuiperConfig(conf.Config)
	if err != nil {
		panic(err)
	}

	ruleProcessor = processor.NewRuleProcessor()
	streamProcessor = processor.NewStreamProcessor()

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

	registry = &RuleRegistry{internal: make(map[string]*RuleState)}
	//Start lookup tables
	streamProcessor.RecoverLookupTable()
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

	//Start rest service
	srvRest := createRestServer(conf.Config.Basic.RestIp, conf.Config.Basic.RestPort, conf.Config.Basic.Authentication)
	go func() {
		var err error
		if conf.Config.Basic.RestTls == nil {
			err = srvRest.ListenAndServe()
		} else {
			err = srvRest.ListenAndServeTLS(conf.Config.Basic.RestTls.Certfile, conf.Config.Basic.RestTls.Keyfile)
		}
		if err != nil && err != http.ErrServerClosed {
			logger.Errorf("Error serving rest service: %s", err)
		}
	}()

	// Start extend services
	for k, v := range servers {
		logger.Infof("start service %s", k)
		v.serve()
	}

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

	if err = srvRest.Shutdown(context.TODO()); err != nil {
		logger.Errorf("rest server shutdown error: %v", err)
	}
	logger.Info("rest server successfully shutdown.")

	// close extend services
	for k, v := range servers {
		logger.Infof("close service %s", k)
		v.close()
	}

	os.Exit(0)
}
