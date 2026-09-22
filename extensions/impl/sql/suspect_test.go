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

package sql

import (
	"fmt"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/require"

	client2 "github.com/lf-edge/ekuiper/v2/extensions/impl/sql/client"
	kctx "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

// suspectFakeConn is a healthy non-recoverable provider: no worker
// ever starts for it, so the gate only moves when the test moves it.
type suspectFakeConn struct {
	id string
}

func (f *suspectFakeConn) Provision(ctx api.StreamContext, conId string, props map[string]any) error {
	f.id = conId
	return nil
}

func (f *suspectFakeConn) Dial(ctx api.StreamContext) error { return nil }

func (f *suspectFakeConn) GetId(ctx api.StreamContext) string { return f.id }

func (f *suspectFakeConn) Ping(ctx api.StreamContext) error { return nil }

func (f *suspectFakeConn) Close(ctx api.StreamContext) error { return nil }

// TestReportTransportFailureRespectsCallerDeath pins the isolation
// rule: a dead caller never reports (gate untouched, no wakeup),
// a live caller closes the gate while the public status stays
// connected.
func TestReportTransportFailureRespectsCallerDeath(t *testing.T) {
	require.NoError(t, connection.InitConnectionManager4Test())
	modules.RegisterConnection("suspecttest", func(ctx api.StreamContext) modules.Connection {
		return &suspectFakeConn{}
	})
	ctx := mockContext.NewMockContext("suspect", "op1")
	cw, err := connection.CreateNamedConnection(ctx, "suspect-gate", "suspecttest", nil)
	require.NoError(t, err)
	defer connection.DropNameConnection(ctx, "suspect-gate")
	require.Eventually(t, func() bool {
		s, _ := cw.Status()
		return s == api.ConnectionConnected
	}, 5*time.Second, 5*time.Millisecond)

	// Dead caller: nothing recorded, gate stays open.
	dead, cancel := ctx.WithCancel()
	cancel()
	reportTransportFailure(dead, cw)
	require.NoError(t, cw.WaitReady(ctx))

	// Live caller: gate closes (waiters park), status stays connected.
	reportTransportFailure(ctx, cw)
	s, _ := cw.Status()
	require.Equal(t, api.ConnectionConnected, s)
	done := make(chan error, 1)
	go func() { done <- cw.WaitReady(ctx) }()
	select {
	case err := <-done:
		t.Fatalf("WaitReady released on a closed gate: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
}

// TestLookupCanceledCallerNeverReports pins the consumer side of the
// same rule: a lookup canceled before doing I/O surfaces the caller
// error without touching the shared gate.
func TestLookupCanceledCallerNeverReports(t *testing.T) {
	require.NoError(t, connection.InitConnectionManager4Test())
	base := connection.WithLookupRefID(kctx.Background(), "lookup:suspectcancel")
	ctx, cancel := base.WithCancel()
	defer cancel()

	dburl := fmt.Sprintf("sqlite://%s/lookup_suspect.db", t.TempDir())
	ls := &SqlLookupSource{}
	require.NoError(t, ls.Provision(ctx, map[string]any{
		"dburl":      dburl,
		"datasource": "t",
	}))
	require.NoError(t, ls.Connect(ctx, nil))
	defer ls.Close(ctx)

	// Prime the cached handle with a successful lookup (no failure,
	// no report): create the table through the pooled handle first.
	live := mockContext.NewMockContext("prime", "op1")
	c, err := ls.cw.Wait(ctx)
	require.NoError(t, err)
	require.NotNil(t, c)
	_, err = c.(*client2.SQLConnection).GetDB().Exec(`CREATE TABLE t (a BIGINT)`)
	require.NoError(t, err)
	_, err = ls.Lookup(live, []string{"a"}, []string{"a"}, []any{1})
	require.NoError(t, err)

	// Cancel, then look up: WaitReady fails on the dead caller before
	// any I/O runs, so no suspect is ever recorded and the gate stays
	// open for everyone else.
	cancel()
	_, err = ls.Lookup(ctx, []string{"a"}, []string{"a"}, []any{1})
	require.Error(t, err)
	require.NoError(t, ls.cw.WaitReady(live))
}
