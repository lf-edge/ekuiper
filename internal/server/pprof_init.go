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

//go:build pprof
// +build pprof

package server

import (
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
)

func init() {
	servers["pprof"] = pprofComp{}
}

type pprofComp struct {
	s *http.Server
}

func (p pprofComp) serve() {
	if err := http.ListenAndServe(":6060", nil); err != nil {
		log.Fatal(err)
	}
	os.Exit(0)
}

func (p pprofComp) close() {
	// do nothing
}
