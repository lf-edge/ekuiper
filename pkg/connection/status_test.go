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
	"sync"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

type statusProbeConnection struct {
	mockConnection
	started, release     chan struct{}
	startOnce, closeOnce sync.Once
}

func (c *statusProbeConnection) Ping(ctx api.StreamContext) error {
	c.startOnce.Do(func() { close(c.started) })
	select {
	case <-c.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *statusProbeConnection) Close(api.StreamContext) error {
	c.closeOnce.Do(func() { close(c.release) })
	return nil
}

func setupStatusProbe(t *testing.T) (api.StreamContext, *statusProbeConnection) {
	t.Helper()
	require.NoError(t, InitConnectionManager4Test())
	ctx := mockContext.NewMockContext(t.Name(), "probe")
	c := &statusProbeConnection{started: make(chan struct{}), release: make(chan struct{})}
	modules.RegisterConnection("status-probe-test", func(api.StreamContext) modules.Connection { return c })
	cw, err := CreateNamedConnection(ctx, "slow-probe", "status-probe-test", nil)
	require.NoError(t, err)
	_, err = cw.Wait(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close(ctx)) })
	return ctx, c
}

func TestPatrolDoesNotBlockOtherConnections(t *testing.T) {
	ctx, c := setupStatusProbe(t)
	patrolDone := make(chan struct{})
	go func() {
		defer close(patrolDone)
		patrolConnectionStatus()
	}()
	select {
	case <-c.started:
	case <-time.After(2 * time.Second):
		t.Fatal("patrol did not start the health check")
	}
	done := make(chan error, 1)
	go func() {
		key := extractRefId(ctx)
		if _, err := FetchConnection(ctx, key, "mock", nil, nil); err != nil {
			done <- err
			return
		}
		done <- DetachConnection(ctx, key)
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		_ = c.Close(ctx)
		<-patrolDone
		<-done
		t.Fatal("a slow health check blocked an unrelated connection")
	}
	require.NoError(t, DropNameConnection(ctx, "slow-probe"))
	select {
	case <-patrolDone:
	case <-time.After(2 * time.Second):
		t.Fatal("patrol did not finish after close")
	}
}

func TestAttachReportsFastFailure(t *testing.T) {
	ready := make(chan struct{})
	close(ready)
	meta := &Meta{ID: "fast-failure", cw: &ConnWrapper{initialized: true, conn: &mockErrConnection{}, readCh: ready}}
	meta.status.Store(api.ConnectionConnected)
	status, message := meta.GetStatus()
	require.Equal(t, api.ConnectionDisconnected, status)
	require.NotEmpty(t, message)
	var gotStatus, gotMessage string
	meta.AddRef("consumer", func(status, message string) {
		gotStatus, gotMessage = status, message
	})
	require.Equal(t, api.ConnectionDisconnected, gotStatus)
	require.NotEmpty(t, gotMessage)
}
