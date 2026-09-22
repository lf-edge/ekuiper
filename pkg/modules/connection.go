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
	// Provision validates/configures and constructs the candidate. It
	// must return without blocking for remote readiness, and it may
	// start provider-internal background activity.
	//
	// Ownership: a nil return transfers full ownership to the Pool. The
	// Pool may Close the candidate at any time afterwards — including
	// when a later creation step (e.g. persist) fails before any Dial,
	// or when the lifecycle stops before Dial completes. Close must
	// safely release all candidate resources and background activity,
	// even if Dial was never called or never succeeded.
	// A non-nil return means Provision itself failed: the provider
	// owns cleanup of any partial state, the Pool closes nothing.
	Provision(ctx api.StreamContext, conId string, props map[string]any) error
	// Dial establishes or waits for initial usability. It must honor
	// ctx cancellation promptly or otherwise have a finite bound: the
	// Pool stops a lifecycle by canceling its scope and waiting for the
	// worker, so an uncancellable Dial hangs every stop path
	// (Detach/Drop/reset). Returning nil means the candidate is
	// initially usable.
	Dial(ctx api.StreamContext) error
	GetId(ctx api.StreamContext) string
	// Ping is a single bounded health-check attempt: no retry, no
	// reconnect, no dial-on-empty. An absent handle (Dial never
	// succeeded, or Close already ran) reports an error; creating
	// the handle belongs to Dial/Reconnect, never to a status read.
	// The Pool calls Ping on a bounded attempt scope; providers must
	// honor its deadline rather than imposing their own unbounded
	// block. Self-recovering clients (StatefulDialer) may answer
	// from their local lifecycle flag instead of hitting the remote.
	Ping(ctx api.StreamContext) error
	api.Closable
}

// Attempt and cancellation contract (Pool side, applies to Dial, Ping
// and the A3 Recover):
//
//   - Every attempt runs under an explicitly bounded, server-owned
//     scope — never a rule/request lifetime, never unbounded. Dial
//     waits for initial usability under plain cancellation (no blanket
//     timeout); Ping/Recover additionally carry an attempt deadline.
//   - Caller cancellation surfaces as ctx.Err(); lifecycle termination
//     surfaces as the Pool's ErrConnectionClosed. An attempt must never
//     return (nil, nil): success and failure are distinguishable in
//     every path, so waiters and the recovery worker never observe a
//     vacuous outcome.

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
