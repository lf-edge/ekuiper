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

//go:build !no_pprof

package server

import (
	"net/http"
	_ "net/http/pprof"

	"github.com/Rookiecom/cpuprofile"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

func init() {
	servers["pprof"] = pprofComp{}
}

type pprofComp struct {
	s *http.Server
}

func (p pprofComp) serve() {
	if conf.Config.Basic.Pprof {
		go func() {
			addr := cast.JoinHostPortInt(conf.Config.Basic.PprofIp, conf.Config.Basic.PprofPort)
			if conf.Config.Basic.EnableResourceProfiling {
				cpuprofile.WebProfile(addr)
				return
			}
			conf.Log.Infof("Run pprof in %s", addr)
			if err := http.ListenAndServe(addr, nil); err != nil {
				conf.Log.Errorf("pprof start error: %s", err)
			}
		}()
	}
}

func (p pprofComp) close() {
	// do nothing
}
