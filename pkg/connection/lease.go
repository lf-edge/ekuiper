// Copyright 2026 EMQ Technologies Co., Ltd.
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

package connection

import (
	"sync/atomic"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

// ConnectionLease is the per-consumer handle on a shared logical
// connection. Each successful fetch mints exactly one Lease binding
// one attachment (owner Manager generation + connectionKey + refID +
// attachment token) to the Meta-shared internal handle, so a consumer
// can never release the wrong reference: Release only detaches when
// the owner generation is still current and the token still matches
// the recorded attachment. A Lease from an earlier manager generation
// (after re-init) or an earlier same-ref attachment is stale: its
// Release is a no-op. Business methods delegate to the shared handle;
// only Release is per-Lease state. A nil Lease is safe to use:
// business methods behave as if the connection were never acquired,
// and Release is a no-op.
//
// Attachment vs handle semantics (deliberate): only Release is
// per-attachment. Wait, WaitReady, Status and ReportSuspectedFailure
// address the shared logical connection, so they stay usable after
// Release and are intentionally not token-gated. A suspect is
// transport-health signal, not an ownership mutation — even a
// superseded holder that just failed real I/O reports a true signal,
// and the Pool worker verifies (Ping) before acting on it, so a
// spurious report costs at most one verification.
type ConnectionLease struct {
	cw       *connWrapper
	mgr      *Manager
	key      string
	refID    string
	token    uint64
	released atomic.Bool
}

func newLease(cw *connWrapper, mgr *Manager, key, refID string, token uint64) *ConnectionLease {
	return &ConnectionLease{cw: cw, mgr: mgr, key: key, refID: refID, token: token}
}

// Wait blocks until the logical connection is first usable.
// See the shared handle for the full contract.
func (l *ConnectionLease) Wait(ctx api.StreamContext) (modules.Connection, error) {
	if l == nil || l.cw == nil {
		return nil, ErrConnectionClosed
	}
	return l.cw.Wait(ctx)
}

// Status reports the last-known connection state. Pure read.
func (l *ConnectionLease) Status() (string, string) {
	if l == nil || l.cw == nil {
		return "", ""
	}
	return l.cw.Status()
}

// WaitReady blocks until the pooled connection is internally ready.
// See the shared handle for the full contract.
func (l *ConnectionLease) WaitReady(ctx api.StreamContext) error {
	if l == nil || l.cw == nil {
		return ErrConnectionClosed
	}
	return l.cw.WaitReady(ctx)
}

// ReportSuspectedFailure reports one failed business I/O against the
// pooled connection. See the shared handle for the full contract.
func (l *ConnectionLease) ReportSuspectedFailure() {
	if l == nil || l.cw == nil {
		return
	}
	l.cw.ReportSuspectedFailure()
}

// IsInitialized reports whether the initial connection worker has
// published its first result: false while the first Dial is still in
// flight (a lookup against it reports not-ready once instead of
// parking), true once a handle or an error is available.
func (l *ConnectionLease) IsInitialized() bool {
	if l == nil || l.cw == nil {
		return false
	}
	return l.cw.IsInitialized()
}

// ConnectionKey returns the pool key this Lease is attached to.
func (l *ConnectionLease) ConnectionKey() string {
	if l == nil {
		return ""
	}
	return l.key
}

// Release detaches this Lease's own attachment exactly once; repeated
// calls are a no-op returning nil. A Lease holding no reference
// (named creation, or nil) releases nothing. A stale Lease — owner
// generation replaced, or attachment superseded by a reattach —
// releases nothing: the current holder is untouched.
func (l *ConnectionLease) Release(ctx api.StreamContext) error {
	if l == nil {
		return nil
	}
	if l.released.Swap(true) {
		return nil
	}
	if l.refID == "" {
		return nil
	}
	return detachChecked(ctx, l.mgr, l.key, l.refID, l.token)
}
