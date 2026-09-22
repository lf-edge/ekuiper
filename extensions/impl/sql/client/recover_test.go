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

package client

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func recoverTestConn(t *testing.T, url string) (*SQLConnection, api.StreamContext) {
	t.Helper()
	ctx := mockContext.NewMockContext("recover", "op1")
	c := &SQLConnection{}
	require.NoError(t, c.Provision(ctx, "recover-test", map[string]any{"dburl": url}))
	return c, ctx
}

// TestRecoverInstallsVerifiedCandidate: Recover swaps in a fresh
// Ping-verified handle; the new handle serves immediately.
func TestRecoverInstallsVerifiedCandidate(t *testing.T) {
	url := "sqlite3://" + filepath.Join(t.TempDir(), "recover.db")
	c, ctx := recoverTestConn(t, url)
	require.NoError(t, c.Dial(ctx))
	old := c.GetDB()
	require.NotNil(t, old)

	require.NoError(t, c.Recover(ctx))
	current := c.GetDB()
	require.NotNil(t, current)
	require.NotSame(t, old, current)
	require.NoError(t, c.Ping(ctx))
	require.NoError(t, c.Close(ctx))
}

// TestRecoverRetiresOldHandleAsync: the replaced handle is closed
// detached from Recover (Recover never waits for it) and nothing
// outlives the logical Close.
func TestRecoverRetiresOldHandleAsync(t *testing.T) {
	url := "sqlite3://" + filepath.Join(t.TempDir(), "retire.db")
	c, ctx := recoverTestConn(t, url)
	require.NoError(t, c.Dial(ctx))
	old := c.GetDB()

	require.NoError(t, c.Recover(ctx))
	// The old pool drains asynchronously: closed eventually, while
	// the new handle already serves.
	require.Eventually(t, func() bool {
		return old.PingContext(ctx) != nil
	}, 5*time.Second, 5*time.Millisecond)
	require.NoError(t, c.Ping(ctx))
	require.NoError(t, c.Close(ctx))
}

// TestRecoverFailureKeepsOldHandle: a failed candidate owns nothing —
// the previous handle stays installed for the Pool to verify.
func TestRecoverFailureKeepsOldHandle(t *testing.T) {
	url := "sqlite3://" + filepath.Join(t.TempDir(), "keep.db")
	c, ctx := recoverTestConn(t, url)
	require.NoError(t, c.Dial(ctx))
	old := c.GetDB()

	c.url = "unknown-driver://unreachable"
	require.Error(t, c.Recover(ctx))
	require.Same(t, old, c.GetDB())
	require.NoError(t, c.Close(ctx))
}

// TestRecoverAfterCloseDisposesCandidate: racing the logical Close
// disposes the fresh candidate and reports closure; a second Close
// still returns at once (no orphaned retireWG count).
func TestRecoverAfterCloseDisposesCandidate(t *testing.T) {
	url := "sqlite3://" + filepath.Join(t.TempDir(), "race.db")
	c, ctx := recoverTestConn(t, url)
	require.NoError(t, c.Dial(ctx))

	require.NoError(t, c.Close(ctx))
	require.ErrorContains(t, c.Recover(ctx), "closed during recovery")
	require.NoError(t, c.Close(ctx))
}

// TestFacadeRoutesCurrentHandle: Exec/Query/BeginTx go through the
// installed handle without touching GetDB.
func TestFacadeRoutesCurrentHandle(t *testing.T) {
	url := "sqlite3://" + filepath.Join(t.TempDir(), "facade.db")
	c, ctx := recoverTestConn(t, url)
	require.NoError(t, c.Dial(ctx))

	_, err := c.ExecContext(ctx, `CREATE TABLE t (a BIGINT)`)
	require.NoError(t, err)
	_, err = c.ExecContext(ctx, `INSERT INTO t VALUES (1)`)
	require.NoError(t, err)
	rows, err := c.QueryContext(ctx, `SELECT a FROM t`)
	require.NoError(t, err)
	var v int64
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&v))
	require.Equal(t, int64(1), v)
	rows.Close()
	tx, err := c.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	require.NoError(t, c.Close(ctx))
}

// TestFacadeFollowsRecover: after a swap the facade serves the new
// handle; the old one is retired.
func TestFacadeFollowsRecover(t *testing.T) {
	url := "sqlite3://" + filepath.Join(t.TempDir(), "facadeswap.db")
	c, ctx := recoverTestConn(t, url)
	require.NoError(t, c.Dial(ctx))
	old := c.GetDB()

	require.NoError(t, c.Recover(ctx))
	require.NotSame(t, old, c.GetDB())
	_, err := c.ExecContext(ctx, `CREATE TABLE t (a BIGINT)`)
	require.NoError(t, err)
	require.NoError(t, c.Close(ctx))
}

// TestFacadeWithoutHandle errors instead of panicking on a nil handle.
func TestFacadeWithoutHandle(t *testing.T) {
	c := &SQLConnection{id: "no-handle"}
	ctx := mockContext.NewMockContext("facade", "op1")
	_, err := c.QueryContext(ctx, `SELECT 1`)
	require.ErrorContains(t, err, "no database handle")
	_, err = c.ExecContext(ctx, `SELECT 1`)
	require.ErrorContains(t, err, "no database handle")
	_, err = c.BeginTx(ctx, nil)
	require.ErrorContains(t, err, "no database handle")
}
