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

import (
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/syncx"
)

type ConnectionStatus struct {
	Status string `json:"status"`
	ErrMsg string `json:"errMsg,omitempty"`
}

type Connection interface {
	// Provision performs synchronous static validation and constructs
	// the candidate object. It must not dial, retry, or depend on the
	// remote being reachable.
	//
	// Ownership: a nil return transfers the candidate to the Pool.
	// The Pool may Close it at any time afterwards — including when a
	// later creation step (e.g. persist) fails before any Dial, or when
	// the lifecycle stops before Dial completes. Close must therefore
	// stay safe on a provisioned-but-never-dialed object.
	// A non-nil return means Provision itself failed: the provider
	// owns cleanup of any partial state, the Pool closes nothing.
	Provision(ctx api.StreamContext, conId string, props map[string]any) error
	// Dial performs one bounded connection attempt. It must honor ctx
	// cancellation (return ctx.Err() promptly) or otherwise guarantee a
	// finite block: the Pool stops a lifecycle by canceling its scope
	// and waiting for the worker, so an uncancellable Dial hangs every
	// stop path (Detach/Drop/reset).
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

var (
	connectionRegisterMu syncx.RWMutex
	ConnectionRegister   map[string]ConnectionProvider
)

func init() {
	ConnectionRegister = map[string]ConnectionProvider{}
}

func RegisterConnection(name string, cp ConnectionProvider) {
	connectionRegisterMu.Lock()
	defer connectionRegisterMu.Unlock()
	ConnectionRegister[name] = cp
}

// GetConnectionProvider returns a connection provider by name in a thread-safe manner
func GetConnectionProvider(name string) (ConnectionProvider, bool) {
	connectionRegisterMu.RLock()
	defer connectionRegisterMu.RUnlock()
	cp, ok := ConnectionRegister[name]
	return cp, ok
}
