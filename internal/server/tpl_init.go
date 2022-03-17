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

//go:build template || !core
// +build template !core

package server

import (
	"github.com/gorilla/mux"
	"github.com/lf-edge/ekuiper/internal/topo/transform"
)

func init() {
	components["template"] = tplComp{}
}

type tplComp struct{}

func (t tplComp) register() {
	transform.RegisterAdditionalFuncs()
}

func (t tplComp) rest(_ *mux.Router) {
	// do nothing
}
