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
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

// probeConn is a controllable non-stateful provider: healthy Dial,
// scripted Ping.
type probeConn struct {
	id        string
	pingErr   error
	pingCalls *atomic.Int32
	dialCalls *atomic.Int32
	blockPing bool
}

func (p *probeConn) Provision(ctx api.StreamContext, conId string, props map[string]any) error {
	p.id = conId
	return nil
}

func (p *probeConn) Dial(ctx api.StreamContext) error {
	p.dialCalls.Add(1)
	return nil
}

func (p *probeConn) GetId(ctx api.StreamContext) string { return p.id }

func (p *probeConn) Ping(ctx api.StreamContext) error {
	p.pingCalls.Add(1)
	if p.blockPing {
		<-ctx.Done()
		return ctx.Err()
	}
	return p.pingErr
}

func (p *probeConn) Close(ctx api.StreamContext) error { return nil }

// statefulProbeConn reports its own state through the Pool callback;
// the probe must never second-guess it.
type statefulProbeConn struct {
	probeConn
	handler api.StatusChangeHandler
}

func (s *statefulProbeConn) SetStatusChangeHandler(ctx api.StreamContext, handler api.StatusChangeHandler) {
	s.handler = handler
}

func (s *statefulProbeConn) Status(ctx api.StreamContext) modules.ConnectionStatus {
	return modules.ConnectionStatus{}
}

var (
	probeFailPingCalls  atomic.Int32
	probeFailDialCalls  atomic.Int32
	probeOkPingCalls    atomic.Int32
	probeOkDialCalls    atomic.Int32
	probeBlockPingCalls atomic.Int32
	probeBlockDialCalls atomic.Int32
	probeStatePingCalls atomic.Int32
	probeStateDialCalls atomic.Int32
)

func registerProbeProviders() {
	modules.RegisterConnection("failping", func(ctx api.StreamContext) modules.Connection {
		return &probeConn{pingErr: errors.New("failping down"), pingCalls: &probeFailPingCalls, dialCalls: &probeFailDialCalls}
	})
	modules.RegisterConnection("okping", func(ctx api.StreamContext) modules.Connection {
		return &probeConn{pingCalls: &probeOkPingCalls, dialCalls: &probeOkDialCalls}
	})
	modules.RegisterConnection("blockping", func(ctx api.StreamContext) modules.Connection {
		return &probeConn{blockPing: true, pingCalls: &probeBlockPingCalls, dialCalls: &probeBlockDialCalls}
	})
	modules.RegisterConnection("statefulprobe", func(ctx api.StreamContext) modules.Connection {
		return &statefulProbeConn{probeConn: probeConn{pingCalls: &probeStatePingCalls, dialCalls: &probeStateDialCalls}}
	})
}

func probeTestCtx() api.StreamContext {
	return mockContext.NewMockContext("probe", "op1")
}

func requireConnected(t *testing.T, key string) {
	t.Helper()
	require.Eventually(t, func() bool {
		m := globalConnectionManager.Load()
		m.RLock()
		defer m.RUnlock()
		e, ok := m.connectionPool[key]
		if !ok || e.state != entryReady || e.meta == nil {
			return false
		}
		s, _ := e.meta.GetStatus()
		return s == api.ConnectionConnected
	}, 5*time.Second, 5*time.Millisecond)
}

func probeMeta(t *testing.T, key string) *Meta {
	t.Helper()
	m := globalConnectionManager.Load()
	m.RLock()
	defer m.RUnlock()
	e, ok := m.connectionPool[key]
	require.True(t, ok, "missing pool entry for %s", key)
	require.NotNil(t, e.meta)
	return e.meta
}

// TestProbeFlipsConnectedToDisconnected is the core probe contract:
// a failed Ping moves connected to disconnected with the Ping error,
// opening a new parked generation. Nothing else happens.
func TestProbeFlipsConnectedToDisconnected(t *testing.T) {
	registerProbeProviders()
	ctx := probeTestCtx()
	_, err := CreateNamedConnection(ctx, "probe-flip", "failping", nil)
	require.NoError(t, err)
	defer DropNameConnection(ctx, "probe-flip")

	requireConnected(t, "probe-flip")
	meta := probeMeta(t, "probe-flip")
	genBefore := meta.generation

	probeConnections(time.Second)

	s, e := meta.GetStatus()
	require.Equal(t, api.ConnectionDisconnected, s)
	require.Equal(t, "failping down", e)
	require.Greater(t, meta.generation, genBefore)
	require.False(t, isClosed(meta.readyCh))
}

// TestProbeLeavesHealthyConnected verifies a passing Ping changes
// nothing: same state, cleared error, same generation.
func TestProbeLeavesHealthyConnected(t *testing.T) {
	registerProbeProviders()
	ctx := probeTestCtx()
	_, err := CreateNamedConnection(ctx, "probe-ok", "okping", nil)
	require.NoError(t, err)
	defer DropNameConnection(ctx, "probe-ok")

	requireConnected(t, "probe-ok")
	meta := probeMeta(t, "probe-ok")
	genBefore := meta.generation
	callsBefore := probeOkPingCalls.Load()

	probeConnections(time.Second)

	s, e := meta.GetStatus()
	require.Equal(t, api.ConnectionConnected, s)
	require.Equal(t, "", e)
	require.Equal(t, genBefore, meta.generation)
	require.Greater(t, probeOkPingCalls.Load(), callsBefore)
}

// TestProbeSkipsNonConnected pins that only connected is probed: a
// disconnected Meta is never Pinged and never recovered by the probe.
func TestProbeSkipsNonConnected(t *testing.T) {
	registerProbeProviders()
	ctx := probeTestCtx()
	_, err := CreateNamedConnection(ctx, "probe-skip", "okping", nil)
	require.NoError(t, err)
	defer DropNameConnection(ctx, "probe-skip")

	requireConnected(t, "probe-skip")
	meta := probeMeta(t, "probe-skip")
	meta.NotifyStatus(api.ConnectionDisconnected, "boom")
	callsBefore := probeOkPingCalls.Load()
	dialBefore := probeOkDialCalls.Load()

	probeConnections(time.Second)

	s, e := meta.GetStatus()
	require.Equal(t, api.ConnectionDisconnected, s)
	require.Equal(t, "boom", e)
	require.Equal(t, callsBefore, probeOkPingCalls.Load())
	require.Equal(t, dialBefore, probeOkDialCalls.Load())
}

// TestProbeSkipsStateful proves the probe never second-guesses
// self-recovering clients: no Ping, no state change.
func TestProbeSkipsStateful(t *testing.T) {
	registerProbeProviders()
	ctx := probeTestCtx()
	_, err := CreateNamedConnection(ctx, "probe-stateful", "statefulprobe", nil)
	require.NoError(t, err)
	defer DropNameConnection(ctx, "probe-stateful")

	// Stateful providers never get the automatic connected report;
	// simulate their runtime callback instead.
	meta := probeMeta(t, "probe-stateful")
	meta.NotifyStatus(api.ConnectionConnected, "")
	callsBefore := probeStatePingCalls.Load()

	probeConnections(time.Second)

	s, _ := meta.GetStatus()
	require.Equal(t, api.ConnectionConnected, s)
	require.Equal(t, callsBefore, probeStatePingCalls.Load())
}

// TestProbeSkipsDyingLifecycle ensures a scope death during the round
// is never recorded as a provider fault.
func TestProbeSkipsDyingLifecycle(t *testing.T) {
	registerProbeProviders()
	ctx := probeTestCtx()
	_, err := CreateNamedConnection(ctx, "probe-dying", "okping", nil)
	require.NoError(t, err)
	defer DropNameConnection(ctx, "probe-dying")

	requireConnected(t, "probe-dying")
	meta := probeMeta(t, "probe-dying")
	meta.lifecycleCancel()
	callsBefore := probeOkPingCalls.Load()

	probeConnections(time.Second)
	require.Equal(t, callsBefore, probeOkPingCalls.Load())
}

// TestProbePingBounded proves one hanging Ping cannot stall the
// round: the attempt ctx fires and the round moves on.
func TestProbePingBounded(t *testing.T) {
	registerProbeProviders()
	ctx := probeTestCtx()
	_, err := CreateNamedConnection(ctx, "probe-block", "blockping", nil)
	require.NoError(t, err)
	defer DropNameConnection(ctx, "probe-block")

	requireConnected(t, "probe-block")
	meta := probeMeta(t, "probe-block")

	start := time.Now()
	probeConnections(100 * time.Millisecond)
	elapsed := time.Since(start)
	require.Less(t, elapsed, 5*time.Second)

	s, _ := meta.GetStatus()
	require.Equal(t, api.ConnectionDisconnected, s)
}
