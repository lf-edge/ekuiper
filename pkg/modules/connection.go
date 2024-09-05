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

package modules

import "github.com/lf-edge/ekuiper/contract/v2/api"

type ConnectionStatus struct {
	Status string `json:"status"`
	ErrMsg string `json:"errMsg,omitempty"`
}

type Connection interface {
	Provision(ctx api.StreamContext, conId string, props map[string]any) error
	Dial(ctx api.StreamContext) error
	GetId(ctx api.StreamContext) string
	Ping(ctx api.StreamContext) error
	api.Closable
}

type StatefulDialer interface {
	SetStatusChangeHandler(ctx api.StreamContext, handler api.StatusChangeHandler)
	Status(ctx api.StreamContext) ConnectionStatus
}

type ConnectionProvider func(ctx api.StreamContext) Connection

var ConnectionRegister map[string]ConnectionProvider

func init() {
	ConnectionRegister = map[string]ConnectionProvider{}
}

func RegisterConnection(name string, cp ConnectionProvider) {
	ConnectionRegister[name] = cp
}
