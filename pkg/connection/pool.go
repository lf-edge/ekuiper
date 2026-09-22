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
	"sync/atomic"
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
	// Lock invariant (A2): the Manager lock is structural only. Under
	// it: map/entry-state/pointer operations and ref registration.
	// Never under it: provider I/O (Provision/Dial/Ping/Close),
	// KV/store I/O, consumer-callback invocation, or blocking waits.
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
	// use ConnWrapper.Wait / Meta readiness for that.
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

// globalConnectionManager is swapped wholesale on Init/reset. The
// pointer itself is synchronized so concurrent readers (e.g. a rule
// teardown racing a test reset) never trip the memory model; every
// mutation path still goes through the captured instance's own
// lock. This does NOT make concurrent Init safe: Init/reset stays
// contractually serialized with all mutations, it just fails
// observably instead of racing.
var globalConnectionManager atomic.Pointer[Manager]

func init() {
	ctx, cancel := context.WithCancel(context.Background())
	globalConnectionManager.Store(&Manager{
		connectionPool: make(map[string]*poolEntry),
		ctx:            ctx,
		cancel:         cancel,
	})
}

func InitConnectionManager4Test() error {
	conf.IsTesting = true
	InitConnectionManager(context.Background())
	return nil
}

func InitConnectionManager(ctx context.Context) {
	// Bootstrap/reset only: must not race Fetch/Create/Update/Drop/
	// Detach/Reload. The previous generation, if any, is retired
	// serially first (scope canceled, every published runtime
	// connection stopped and closed), and only then replaced.
	// Persistent named records are left intact; the next bootstrap
	// reloads them via ReloadNamedConnection. This is generation
	// replacement, not process shutdown: server exit keeps relying on
	// the existing rule teardown path and never calls into here.
	if ctx == nil {
		ctx = context.Background()
	}
	if prev := globalConnectionManager.Load(); prev != nil {
		prev.stopAllRuntime()
	}
	mctx, cancel := context.WithCancel(ctx)
	globalConnectionManager.Store(&Manager{
		connectionPool: make(map[string]*poolEntry),
		ctx:            mctx,
		cancel:         cancel,
	})
	if conf.IsTesting {
		return
	}
	go PatrolConnectionStatusJob(ctx)
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
	ticker := time.NewTicker(15 * time.Second)
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
	m := globalConnectionManager.Load()
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
		status, _ := t.meta.GetStatus()
		switch status {
		case api.ConnectionConnected:
			ConnStatusGauge.WithLabelValues(t.name).Set(1)
		case api.ConnectionDisconnected:
			ConnStatusGauge.WithLabelValues(t.name).Set(-1)
		case api.ConnectionConnecting:
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
	m := globalConnectionManager.Load()
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
	m := globalConnectionManager.Load()
	m.RLock()
	defer m.RUnlock()
	e, ok := m.connectionPool[id]
	if !ok || e.state != entryReady || e.meta == nil {
		return nil, fmt.Errorf("connection %s not existed", id)
	}
	return e.meta, nil
}
