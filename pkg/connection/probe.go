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
	"context"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

const (
	// defaultConnectionMonitorInterval paces both the status Patrol
	// and the health probe. One shared constant, deliberately not a
	// user configuration item in A2: a probe cadence worth
	// configuring only arrives with the A3 recovery worker, which
	// will unify monitoring configuration then.
	defaultConnectionMonitorInterval = 15 * time.Second
	// probePingTimeout bounds a single probe Ping. Providers must
	// answer health checks within it; a provider needing longer has
	// a contract problem (c4), not a slower probe.
	probePingTimeout = 5 * time.Second
)

// ConnectionHealthProbeJob periodically verifies the health of
// connected named connections. It is failure discovery only: a
// failed Ping flips connected to disconnected (opening a new
// readiness generation for WaitReady waiters) and hands the episode
// to the per-Meta recovery worker via nudgeRecovery. It never dials,
// never recovers, never reconnects — recovery belongs to the worker.
// It runs independent of the status Patrol: the Patrol stays a
// pure-read metrics job that a slow probe can never stall.
func ConnectionHealthProbeJob(ctx context.Context) {
	ticker := time.NewTicker(defaultConnectionMonitorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			probeConnections(probePingTimeout)
		}
	}
}

// probeTarget is one snapshot entry: the Meta plus its published
// handle, both captured under the Manager read lock so the handle
// pointer read is synchronized with its publish.
type probeTarget struct {
	meta *Meta
	cw   *ConnWrapper
}

func snapshotProbeTargets() []probeTarget {
	m := globalConnectionManager.Load()
	if m == nil {
		return nil
	}
	m.RLock()
	defer m.RUnlock()
	var targets []probeTarget
	for _, e := range m.connectionPool {
		// Parity with the Patrol surface: named ready Metas only.
		// Anonymous coverage arrives with the A3 recovery worker,
		// which owns every ready Meta.
		if e.state != entryReady || e.meta == nil || !e.meta.Named || e.meta.cw == nil {
			continue
		}
		targets = append(targets, probeTarget{meta: e.meta, cw: e.meta.cw})
	}
	return targets
}

// probeConnections runs one probe round with the given per-Ping
// timeout. Exported behavior lives here so tests drive it
// deterministically without waiting for the job tick.
func probeConnections(timeout time.Duration) {
	for _, t := range snapshotProbeTargets() {
		probeOne(t.meta, t.cw, timeout)
	}
}

func probeOne(meta *Meta, cw *ConnWrapper, timeout time.Duration) {
	// A dying Meta belongs to its stop path, not to the probe.
	if meta.lifecycleCtx.Err() != nil {
		return
	}
	// The probe only ever Pings a connected+ready Meta: every other
	// state already parks waiters on an open generation, and a
	// verifying episode belongs to the recovery worker, not to a
	// competing Ping verdict. The generation travels with the Ping:
	// the verdict below only lands if none of the three moved while
	// it was in flight.
	status, ready, generation := meta.snapshotProbe()
	if status != api.ConnectionConnected || !ready {
		return
	}
	conn := cw.peekConn()
	if conn == nil {
		return
	}
	// Self-recovering clients report their own runtime state through
	// status callbacks (c4 wiring); the probe must not second-guess
	// them with a competing Ping verdict.
	if _, isStateful := conn.(modules.StatefulDialer); isStateful {
		return
	}
	pingCtx, cancel := attemptStreamContext(meta.lifecycleCtx, timeout)
	err := conn.Ping(pingCtx)
	cancel()
	if err == nil {
		return
	}
	// Our own scope may have died during the Ping: that is lifecycle
	// termination, never a provider fault to record.
	if meta.lifecycleCtx.Err() != nil {
		return
	}
	// A verdict superseded mid-Ping (e.g. by a completed recovery) is
	// dropped: it must not overwrite fresh state. Only a real flip
	// hands the episode to the recovery worker.
	if meta.tryProbeDisconnect(generation, err.Error()) {
		meta.nudgeRecovery()
	}
}
