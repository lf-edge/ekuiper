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

//go:build prometheus || !core
// +build prometheus !core

package server

import (
	"context"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"time"
)

func init() {
	p := promeComp{}
	servers["prometheus"] = p
	components["prometheus"] = p
}

type promeComp struct {
	s *http.Server
}

func (p promeComp) register() {
	// Do nothing
}

func (p promeComp) rest(r *mux.Router) {
	portPrometheus := conf.Config.Basic.PrometheusPort
	portRest := conf.Config.Basic.RestPort
	if portPrometheus == portRest {
		r.Handle("/metrics", promhttp.Handler())
		msg := fmt.Sprintf("Register prometheus metrics to http://localhost:%d/metrics", portPrometheus)
		logger.Infof(msg)
		fmt.Println(msg)
	}
}

func (p promeComp) serve() {
	if conf.Config.Basic.Prometheus {
		//Start prometheus service
		portPrometheus := conf.Config.Basic.PrometheusPort
		if portPrometheus <= 0 {
			logger.Fatal("Miss configuration prometheusPort")
		}
		portRest := conf.Config.Basic.RestPort
		if portPrometheus != portRest {
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.Handler())
			srvPrometheus := &http.Server{
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
			p.s = srvPrometheus
			msg := fmt.Sprintf("Serving prometheus metrics on port http://localhost:%d/metrics", portPrometheus)
			logger.Infof(msg)
			fmt.Println(msg)
		}
	}
}

func (p promeComp) close() {
	if p.s != nil {
		if err := p.s.Shutdown(context.TODO()); err != nil {
			logger.Errorf("prometheus server shutdown error: %v", err)
		}
		logger.Info("prometheus server successfully shutdown.")
	}
}
