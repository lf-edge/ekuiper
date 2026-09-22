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

	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

// waitForPoolAttach polls until the key is present in the pool (Fetch
// attached) or the timeout expires. It proves the Connect path reached
// attach without depending on the initial Dial outcome.
func waitForPoolAttach(t *testing.T, ctx api.StreamContext, key string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := connection.GetConnectionDetail(ctx, key); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for pool attach of %s", key)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestSourceConnectWaitFailureReleasesRef locks in the partial-failure
// contract: when Fetch attaches but the initial Wait fails, Connect must
// have saved conId/refID already so Close releases the reference instead
// of leaking it.
func TestSourceConnectWaitFailureReleasesRef(t *testing.T) {
	require.NoError(t, connection.InitConnectionManager4Test())
	rootCtx := mockContext.NewMockContext("partial_close", "op1")
	ctx, cancel := rootCtx.WithCancel()
	defer cancel()

	// Blackhole: attach succeeds, the initial Dial never completes.
	port := newBlackholeListener(t)
	dburl := fmt.Sprintf("mysql://root:@127.0.0.1:%d/test", port)
	s := &SQLSourceConnector{}
	require.NoError(t, s.Provision(ctx, map[string]any{
		"interval": "1s",
		"dburl":    dburl,
		"templateSqlQueryCfg": map[string]any{
			"templateSql": "select a,b from t",
		},
	}))
	connectErr := make(chan error, 1)
	go func() {
		connectErr <- s.Connect(ctx, nil)
	}()
	waitForPoolAttach(t, ctx, dburl)
	// Cancel while Wait is blocked: Connect must fail, but the saved
	// conId/refID must still release the attached ref.
	cancel()
	select {
	case err := <-connectErr:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Connect did not return after cancel")
	}
	require.NotEmpty(t, s.conId)
	require.NotEmpty(t, s.refID)

	require.NoError(t, s.Close(ctx))
	_, err := connection.GetConnectionDetail(ctx, dburl)
	require.Error(t, err, "anonymous zero-ref Meta must be removed after Close")
}

// TestSinkConnectWaitFailureReleasesRef is the sink-side equivalent: the
// saved cw/refID must detach the partially attached reference.
func TestSinkConnectWaitFailureReleasesRef(t *testing.T) {
	require.NoError(t, connection.InitConnectionManager4Test())
	rootCtx := mockContext.NewMockContext("partial_close_sink", "op1")
	ctx, cancel := rootCtx.WithCancel()
	defer cancel()

	port := newBlackholeListener(t)
	dburl := fmt.Sprintf("mysql://root:@127.0.0.1:%d/test", port)
	s := &SQLSinkConnector{}
	require.NoError(t, s.Provision(ctx, map[string]any{
		"dburl": dburl,
		"table": "t",
	}))
	connectErr := make(chan error, 1)
	go func() {
		connectErr <- s.Connect(ctx, nil)
	}()
	waitForPoolAttach(t, ctx, dburl)
	cancel()
	select {
	case err := <-connectErr:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Connect did not return after cancel")
	}
	require.NotNil(t, s.cw)
	require.NotEmpty(t, s.refID)

	require.NoError(t, s.Close(ctx))
	_, err := connection.GetConnectionDetail(ctx, dburl)
	require.Error(t, err, "anonymous zero-ref Meta must be removed after Close")
}
