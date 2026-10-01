// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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

package nng

import (
	"sync"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.nanomsg.org/mangos/v3/protocol/pair"
	_ "go.nanomsg.org/mangos/v3/transport/ipc"

	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name  string
		props map[string]any
		err   string
	}{
		{
			name: "wrong format",
			props: map[string]any{
				"url": 345,
			},
			err: "1 error(s) decoding:\n\n* 'url' expected type 'string', got unconvertible type 'int', value: '345'",
		},
		{
			name:  "missing url",
			props: map[string]any{},
			err:   "url is required",
		},
		{
			name: "wrong url",
			props: map[string]any{
				"url": "file:////abc",
			},
			err: "only tcp and ipc scheme are supported",
		},
		{
			name: "wrong protocol",
			props: map[string]any{
				"url":      "tcp://127.0.0.1:444",
				"protocol": "pair1",
			},
			err: "unsupported protocol pair1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, e := ValidateConf(tt.props)
			assert.Error(t, e)
			assert.EqualError(t, e, tt.err)
		})
	}
}

func TestConStatus(t *testing.T) {
	// Synchronize only on the recovering callback we care about: the
	// production path stores the status before invoking the handler,
	// so observing Status() alone cannot prove the callback ran.
	// close(channel) is the sync point; the recorder never blocks,
	// needs no mutex, and ignores the initial connecting/connected
	// deliveries.
	recoveringCh := make(chan struct{})
	var recoveringOnce sync.Once
	scRecorder := func(status string, message string) {
		if status == connection.ConnectionRecovering {
			recoveringOnce.Do(func() {
				close(recoveringCh)
			})
		}
	}
	ctx := mockContext.NewMockContext("testConStatus", "test")
	c := CreateConnection(ctx).(*Sock)
	err := c.Provision(ctx, "testconstatus", map[string]any{
		"url": "ipc:///tmp/testconstatus.ipc",
	})
	require.NoError(t, err)
	st := c.Status(ctx)
	require.Equal(t, modules.ConnectionStatus{Status: api.ConnectionConnecting}, st)
	// Connect
	sock, err := pair.NewSocket()
	require.NoError(t, err)
	err = sock.Listen("ipc:///tmp/testconstatus.ipc")
	require.NoError(t, err)
	c.SetStatusChangeHandler(ctx, scRecorder)
	err = c.Dial(ctx)
	require.NoError(t, err)
	defer c.Close(ctx)
	retry := 10
	for i := 0; i < retry; i++ {
		time.Sleep(100 * time.Millisecond)
		if c.connected.Load() {
			break
		}
	}
	if c.connected.Load() {
		st = c.Status(ctx)
		assert.Equal(t, modules.ConnectionStatus{Status: api.ConnectionConnected}, st)
	} else {
		require.FailNow(t, "failed to connect")
	}
	// Disconnect
	err = sock.Close()
	require.NoError(t, err)
	for i := 0; i < retry; i++ {
		time.Sleep(100 * time.Millisecond)
		if !c.connected.Load() {
			break
		}
	}
	if !c.connected.Load() {
		st = c.Status(ctx)
		// Runtime detach after a completed attach reports recovering,
		// not disconnected: async redial is already underway and the
		// Pool never recovers NNG itself.
		assert.Equal(t, modules.ConnectionStatus{Status: connection.ConnectionRecovering}, st)
	} else {
		require.FailNow(t, "failed to connect")
	}
	// The external callback observed the same runtime transition.
	// Internal status and callback delivery are verified separately:
	// the Status() assertion above covers the former, this wait
	// covers the latter.
	select {
	case <-recoveringCh:
	case <-time.After(5 * time.Second):
		t.Fatal("recovering callback not observed after runtime detach")
	}
	// ReConnect
	sock, err = pair.NewSocket()
	require.NoError(t, err)
	err = sock.Listen("ipc:///tmp/testconstatus.ipc")
	require.NoError(t, err)
	retry = 10
	for i := 0; i < retry; i++ {
		time.Sleep(100 * time.Millisecond)
		if c.connected.Load() {
			break
		}
	}
	if c.connected.Load() {
		st = c.Status(ctx)
		assert.Equal(t, modules.ConnectionStatus{Status: api.ConnectionConnected}, st)
	} else {
		require.FailNow(t, "failed to connect")
	}
}

func TestSend(t *testing.T) {
	ctx := mockContext.NewMockContext("testConStatus", "test")
	c := CreateConnection(ctx).(*Sock)
	err := c.Provision(ctx, "testsend", map[string]any{
		"url": "ipc:///tmp/testsend.ipc",
	})
	require.NoError(t, err)
	// let the connection ready after 100 ms
	go func() {
		time.Sleep(100 * time.Millisecond)
		sock, err := pair.NewSocket()
		require.NoError(t, err)
		err = sock.Listen("ipc:///tmp/testsend.ipc")
		require.NoError(t, err)
	}()
	err = c.Dial(ctx)
	require.NoError(t, err)
	defer c.Close(ctx)
	err = c.Send(ctx, []byte("test"))
	require.NoError(t, err)
}

func TestDialRespondsToCancel(t *testing.T) {
	ctx := mockContext.NewMockContext("nngcancel", "op1")
	c := CreateConnection(ctx).(*Sock)
	require.NoError(t, c.Provision(ctx, "canceltest", map[string]any{
		"url":      "tcp://127.0.0.1:1",
		"protocol": "pair",
	}))
	cctx, cancel := ctx.WithCancel()
	done := make(chan error, 1)
	go func() {
		done <- c.Dial(cctx)
	}()
	// Give Dial a chance to block on first-connect; cancel is
	// effective regardless of the exact timing.
	time.Sleep(200 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, cctx.Err())
	case <-time.After(5 * time.Second):
		t.Fatal("Dial did not observe lifecycle cancellation")
	}
	require.NoError(t, c.Close(ctx))
}
