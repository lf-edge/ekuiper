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

type Connection interface {
	Ping(ctx api.StreamContext) error
	DetachSub(ctx api.StreamContext, props map[string]any)
	api.Closable
}

type ConnectionProvider func(ctx api.StreamContext, props map[string]any) (Connection, error)

var ConnectionRegister map[string]ConnectionProvider

func init() {
	ConnectionRegister = map[string]ConnectionProvider{}
}

func RegisterConnection(name string, cp ConnectionProvider) {
	ConnectionRegister[name] = cp
}
