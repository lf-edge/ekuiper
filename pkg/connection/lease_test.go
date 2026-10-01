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
	"testing"

	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

// TestStaleLeaseAfterResetCannotDetach pins token uniqueness across
// resets: a Lease minted before a manager reset targets a retired
// attachment, so its Release must not detach the same key+refID
// freshly attached afterwards. The reset here is only an extreme ABA
// generator; no manager-generation semantics are involved.
func TestStaleLeaseAfterResetCannotDetach(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	ctx := mockContext.NewMockContext("rule1", "op1")
	opts := FetchOptions{ConnectionKey: "gen-key", RefID: "r1", Type: "mock"}
	l1, err := FetchConnectionWithOptions(ctx, opts)
	require.NoError(t, err)

	// Re-init swaps the manager generation wholesale.
	require.NoError(t, InitConnectionManager4Test())
	l2, err := FetchConnectionWithOptions(ctx, opts)
	require.NoError(t, err)

	// The old Lease is stale: releasing it leaves the new holder alone.
	require.NoError(t, l1.Release(ctx))
	require.Equal(t, 1, getConnectionRef("gen-key"))

	// The current Lease still owns its attachment.
	require.NoError(t, l2.Release(ctx))
	require.Equal(t, 0, getConnectionRef("gen-key"))
}

// TestStaleLeaseAfterReattachCannotDetach pins the token binding: two
// fetches of the same key+refID share one reference slot, and the
// first Lease must not delete the attachment owned by the second.
func TestStaleLeaseAfterReattachCannotDetach(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	ctx := mockContext.NewMockContext("rule1", "op1")
	opts := FetchOptions{ConnectionKey: "reattach-key", RefID: "r1", Type: "mock"}
	l1, err := FetchConnectionWithOptions(ctx, opts)
	require.NoError(t, err)
	l2, err := FetchConnectionWithOptions(ctx, opts)
	require.NoError(t, err)
	require.Equal(t, 1, getConnectionRef("reattach-key"))

	// The superseded attachment releases nothing.
	require.NoError(t, l1.Release(ctx))
	require.Equal(t, 1, getConnectionRef("reattach-key"))

	require.NoError(t, l2.Release(ctx))
	require.Equal(t, 0, getConnectionRef("reattach-key"))
}

// TestStaleLeaseReleaseDuringRecreation pins the ABA/new-generation
// contract: a superseded Lease released while its key is mid-creation
// is a no-op returning nil. A creating round has no published Meta,
// so the stale token cannot own anything in it — unlike Drop/Update,
// which operate on the key itself and wait the round out, a
// token-bound release must neither disturb the creation nor wait.
func TestStaleLeaseReleaseDuringRecreation(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	ctx := mockContext.NewMockContext("rule1", "op1")
	opts := FetchOptions{ConnectionKey: "recreate-key", RefID: "r1", Type: "mock"}
	l1, err := FetchConnectionWithOptions(ctx, opts)
	require.NoError(t, err)
	l2, err := FetchConnectionWithOptions(ctx, opts)
	require.NoError(t, err)

	// The current holder leaves; the old anonymous Meta is torn down
	// and the key disappears.
	require.NoError(t, l2.Release(ctx))
	m := globalConnectionManager
	m.RLock()
	_, ok := m.connectionPool["recreate-key"]
	m.RUnlock()
	require.False(t, ok)

	// A new creation round starts on the same key.
	entry, reserved := m.reserveCreating("recreate-key")
	require.True(t, reserved)

	// The stale Lease releases nothing and reports no error; the
	// ongoing creation is untouched.
	require.NoError(t, l1.Release(ctx))
	m.RLock()
	cur, ok := m.connectionPool["recreate-key"]
	m.RUnlock()
	require.True(t, ok)
	require.Same(t, entry, cur)
	require.Equal(t, entryCreating, cur.state)
	require.Nil(t, cur.meta)

	// White-box cleanup: release the reservation for later tests.
	m.Lock()
	delete(m.connectionPool, "recreate-key")
	m.Unlock()
}
