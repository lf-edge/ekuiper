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

package connection

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/syncx"
)

// Connection pool manages all connections in the system. There are two kinds of connections:
// 1. Named connection: Long running connection. Users can create it standalone through dedicated API without rules.
// The connection will run through all the eKuiper server lifecycle. When restarting, it will be loaded and run as server init.
// 2. Anonymous connection: It is a subsidiary of rules. The rule source/sink defines connection and the connection will
// be fetched when rules start. If no rule has accessed it, it will be closed and dropped.

type Manager struct {
	syncx.RWMutex
	// Lock invariant (A2): the Manager lock covers connection
	// control-plane state: map/entry/ref operations and KV/store CRUD.
	//
	// KV/store latency is intentionally part of the Manager critical
	// section in exchange for atomic, simpler control-plane
	// transitions. A slow KV backend only stalls the control plane;
	// it never affects data-plane I/O, which stays outside the lock.
	//
	// Never under the lock:
	// provider/runtime I/O, consumer callbacks, or blocking waits.
	// Heavy phases run outside the lock; callbacks are delivered
	// after unlock via Meta.deliverInitial.
	//
	// connectionPool maps a connectionKey to its coordination entry.
	// Entries exist for creating, ready, and (from the A1b stop work)
	// removing Metas alike, so concurrent Fetch/Create always observe
	// one authoritative record per key.
	connectionPool map[string]*poolEntry
	// ctx is the server-scoped lifecycle root. Every Meta lifecycle
	// derives from it; it is canceled on re-init/shutdown only, never
	// by any rule, request, or first fetcher.
	ctx context.Context
	// cancel terminates the whole manager scope. Nil only before the
	// first InitConnectionManager call.
	cancel context.CancelFunc
}

// entryState is the lifecycle stage of a pooled key.
type entryState int

const (
	// entryCreating reserves the key while the heavy creation
	// transaction (provider/Provision/persist/Meta build) runs outside
	// the Manager lock. Exactly one creator owns it.
	entryCreating entryState = iota
	// entryReady holds a published Meta ready for attach.
	entryReady
	// entryRemoving marks a key owned by teardown. Callers apply
	// operation-specific policy: anonymous fetches wait for cleanup and
	// retry, RequireExisting fetches and named creates fail fast with
	// ErrConnectionRemoving.
	entryRemoving
)

type poolEntry struct {
	state entryState
	// ready is closed exactly once when this creation round concludes
	// (success or failure). It never signals connection readiness;
	// use connWrapper.Wait / Meta readiness for that.
	ready chan struct{}
	// meta is set on successful creation before ready is closed.
	meta *Meta
	// err carries the creation failure to all waiters. Mutually
	// exclusive with meta: exactly one of them is set at close.
	err error
	// removed is created when the entry flips to removing and closed
	// after cleanup deletes the entry. Fetch waiters use it to wait
	// out a teardown instead of failing, mirroring the old behavior
	// where they blocked on the Manager lock until Close finished.
	removed chan struct{}
}

// globalConnectionManager is a process-lifetime singleton. Its identity
// never changes: Init/reset retires all runtime state and installs a
// fresh lifecycle scope on the same object, but never replaces it.
// Test reset may retire all runtime state and install a fresh lifecycle
// scope, but never replaces the Manager object.
var globalConnectionManager = newManager(context.Background())

func newManager(ctx context.Context) *Manager {
	mctx, cancel := context.WithCancel(ctx)
	return &Manager{
		connectionPool: make(map[string]*poolEntry),
		ctx:            mctx,
		cancel:         cancel,
	}
}

func InitConnectionManager4Test() error {
	conf.IsTesting = true
	InitConnectionManager(context.Background())
	return nil
}

func InitConnectionManager(ctx context.Context) {
	// Bootstrap/reset only: must not race Fetch/Create/Update/Drop/
	// Detach/Reload. The previous runtime, if any, is retired
	// serially first (scope canceled, every published runtime
	// connection stopped and closed) on the same singleton object;
	// only then is a fresh lifecycle scope installed. Persistent
	// named records are left intact; the next bootstrap reloads them
	// via ReloadNamedConnection. This is generation replacement of
	// runtime state, not object replacement, and not process shutdown:
	// server exit keeps relying on the existing rule teardown path
	// and never calls into here.
	if ctx == nil {
		ctx = context.Background()
	}
	globalConnectionManager.reset(ctx)
	if conf.IsTesting {
		return
	}
	go PatrolConnectionStatusJob(ctx)
	go ConnectionHealthProbeJob(ctx)
}

// reset retires this Manager's runtime and installs a fresh lifecycle
// scope in place. The Manager object's identity is stable; only its
// runtime state turns over. Callers must serialize reset with all
// mutations.
func (m *Manager) reset(ctx context.Context) {
	m.stopAllRuntime()
	m.Lock()
	m.ctx, m.cancel = context.WithCancel(ctx)
	m.Unlock()
}

// stopAllRuntime synchronously retires every published runtime
// connection of this manager: cancel the scope, then Meta.stop each
// (which waits worker exit and Closes exactly once via stopOnce).
// Named and anonymous Metas are both stopped; no KV record is touched.
// Init/reset is contractually serialized with mutations, so no new
// Fetch/Create/Detach/Drop can interleave here and no waiter can be
// parked on these entries — the runtime removing/waiter protocol is
// deliberately not simulated on this path.
func (m *Manager) stopAllRuntime() {
	if m.cancel != nil {
		m.cancel()
	}
	m.Lock()
	var metas []*Meta
	for _, e := range m.connectionPool {
		if e.state == entryReady && e.meta != nil {
			metas = append(metas, e.meta)
		}
	}
	// Reset owns the whole map: drop every record now instead of
	// transitioning through removing.
	m.connectionPool = make(map[string]*poolEntry)
	m.Unlock()
	cleanupCtx := serverStreamContext(context.Background())
	for _, meta := range metas {
		meta.stop(cleanupCtx)
	}
}

const (
	DefaultInitialInterval = 100 * time.Millisecond
	DefaultMaxInterval     = 10 * time.Second
)

func PatrolConnectionStatusJob(ctx context.Context) {
	ticker := time.NewTicker(defaultConnectionMonitorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			patrolConnectionStatus()
		}
	}
}

func patrolConnectionStatus() {
	// Snapshot under the lock, work outside it (lock invariant):
	// status reads and metric writes never hold the Manager lock,
	// so a slow consumer (or a future blocking read) cannot stall
	// the Pool. Only named ready Metas are patrolled; creating
	// entries have no Meta yet and report nothing.
	type patrolTarget struct {
		name string
		meta *Meta
	}
	m := globalConnectionManager
	m.RLock()
	var targets []patrolTarget
	for connName, e := range m.connectionPool {
		if e.state != entryReady || e.meta == nil || !e.meta.Named {
			continue
		}
		targets = append(targets, patrolTarget{name: connName, meta: e.meta})
	}
	m.RUnlock()
	for _, t := range targets {
		// Numeric gauge reports the availability class, not the full
		// four-state model: connecting and recovering both mean
		// "not serving yet" (0). The string status stays four-state
		// on the API surface.
		status, _ := t.meta.GetStatus()
		switch status {
		case api.ConnectionConnected:
			ConnStatusGauge.WithLabelValues(t.name).Set(1)
		case api.ConnectionDisconnected:
			ConnStatusGauge.WithLabelValues(t.name).Set(-1)
		case api.ConnectionConnecting, ConnectionRecovering:
			ConnStatusGauge.WithLabelValues(t.name).Set(0)
		}
	}
}

func NewExponentialBackOffWithMaxElapsedTime(maxElapsedTime time.Duration) *backoff.ExponentialBackOff {
	return newExponentialBackOff(maxElapsedTime)
}

func newExponentialBackOff(maxElapsedTime time.Duration) *backoff.ExponentialBackOff {
	return backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(DefaultInitialInterval),
		backoff.WithMaxInterval(DefaultMaxInterval),
		backoff.WithMaxElapsedTime(maxElapsedTime),
	)
}

func GetAllConnectionsMeta(forceAll bool) []*Meta {
	m := globalConnectionManager
	m.RLock()
	defer m.RUnlock()
	metaList := make([]*Meta, 0)
	for _, e := range m.connectionPool {
		if e.state != entryReady || e.meta == nil {
			continue
		}
		if !e.meta.Named && !forceAll {
			continue
		}
		metaList = append(metaList, e.meta)
	}
	return metaList
}

func GetConnectionDetail(_ api.StreamContext, id string) (*Meta, error) {
	if id == "" {
		return nil, fmt.Errorf("connection id should be defined")
	}
	m := globalConnectionManager
	m.RLock()
	defer m.RUnlock()
	e, ok := m.connectionPool[id]
	if !ok || e.state != entryReady || e.meta == nil {
		return nil, fmt.Errorf("connection %s not existed", id)
	}
	return e.meta, nil
}
