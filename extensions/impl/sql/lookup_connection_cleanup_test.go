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

package sql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

// TestLookupCreateSemantics verifies the CREATE TABLE contract: static
// configuration errors fail immediately, while an unreachable database does
// not block table creation at all.
func TestLookupCreateSemantics(t *testing.T) {
	require.NoError(t, connection.InitConnectionManager4Test())
	rootCtx := mockContext.NewMockContext("lookup_create", "op1")
	ctx, cancel := rootCtx.WithCancel()
	defer cancel()

	// Malformed URLs and unsupported drivers are static errors: they are
	// detected without touching the remote database and fail Provision.
	for _, dburl := range []string{"123", "unknown-driver://database"} {
		ls := &SqlLookupSource{}
		require.Error(t, ls.Provision(ctx, map[string]any{
			"dburl":      dburl,
			"datasource": "t",
		}), "dburl %v must fail static validation", dburl)
	}

	// A valid URL to an unreachable database is a connection-phase issue:
	// Connect only attaches to the pool, so creation succeeds immediately.
	// The blackhole listener accepts TCP but never answers the handshake,
	// so the database stays unreachable without racing on a released port.
	port := newBlackholeListener(t)
	dburl := fmt.Sprintf("mysql://root:@127.0.0.1:%d/test", port)
	ls := &SqlLookupSource{}
	require.NoError(t, ls.Provision(ctx, map[string]any{
		"dburl":      dburl,
		"datasource": "t",
	}))
	started := time.Now()
	require.NoError(t, ls.Connect(ctx, nil))
	require.Less(t, time.Since(started), time.Second, "lookup Connect must not wait for the database")

	// The first business request reports the unavailable connection...
	_, err := ls.Lookup(ctx, []string{"a"}, []string{"a"}, []any{1})
	require.Error(t, err)

	require.NoError(t, ls.Close(ctx))
	_, err = connection.GetConnectionDetail(ctx, dburl)
	require.Error(t, err, "a closed lookup connection must not remain in the pool")
}

// TestLookupWaitCancellation verifies that the recovery wait is bound to
// the request context: a rule stop must unblock a pending Lookup even while
// the pool initial connection never succeeds.
func TestLookupWaitCancellation(t *testing.T) {
	require.NoError(t, connection.InitConnectionManager4Test())
	rootCtx := mockContext.NewMockContext("lookup_cancel", "op1")
	ctx, cancel := rootCtx.WithCancel()

	port := newBlackholeListener(t)
	dburl := fmt.Sprintf("mysql://root:@127.0.0.1:%d/test", port)
	ls := &SqlLookupSource{}
	require.NoError(t, ls.Provision(ctx, map[string]any{
		"dburl":      dburl,
		"datasource": "t",
	}))
	require.NoError(t, ls.Connect(ctx, nil))

	// First request reports once and marks the source as recovering.
	_, err := ls.Lookup(ctx, []string{"a"}, []string{"a"}, []any{1})
	require.Error(t, err)

	// The second request enters the recovery wait and must exit promptly
	// once the rule context is canceled.
	result := make(chan error, 1)
	go func() {
		_, err := ls.Lookup(ctx, []string{"a"}, []string{"a"}, []any{1})
		result <- err
	}()
	select {
	case <-result:
		t.Fatal("lookup must block while waiting for the database")
	case <-time.After(300 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("canceled lookup did not return in time")
	}

	require.NoError(t, ls.Close(ctx))
}
