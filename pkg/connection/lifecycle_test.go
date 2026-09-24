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
	goctx "context"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func isDoneClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func fetchFailDial(t *testing.T, ctx api.StreamContext, key, ref string) *ConnectionLease {
	t.Helper()
	lease, err := FetchConnectionWithOptions(ctx, FetchOptions{
		ConnectionKey: key,
		RefID:         ref,
		Type:          "faildial",
	})
	require.NoError(t, err)
	return lease
}

// TestMetaLifecycleIndependentOfFirstFetcher is the A1b core regression
// test: a shared anonymous connection is parented by the Manager, not by
// whichever rule fetched first. Canceling the first fetcher must neither
// stop the worker nor remove the Meta while other refs remain.
func TestMetaLifecycleIndependentOfFirstFetcher(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	rootA := mockContext.NewMockContext("ruleA", "op1")
	ctxA, cancelA := rootA.WithCancel()
	ctxB := mockContext.NewMockContext("ruleB", "op1")

	lA := fetchFailDial(t, ctxA, "lc-shared", "refA")
	lB := fetchFailDial(t, ctxB, "lc-shared", "refB")

	meta := getReadyTestMeta("lc-shared")
	require.NotNil(t, meta)

	cancelA()

	require.True(t, checkConn("lc-shared"), "Meta must survive first-fetcher stop")
	require.NoError(t, meta.lifecycleCtx.Err(), "Meta lifecycle must not follow the fetcher ctx")
	require.False(t, isDoneClosed(meta.done), "worker must keep retrying while refs remain")
	require.Equal(t, 2, meta.GetRefCount())

	// Release both refs; the anonymous Meta is removed by the existing
	// zero-ref path.
	require.NoError(t, lA.Release(ctxB))
	require.NoError(t, lB.Release(ctxB))
	_, ok := globalConnectionManager.connectionPool["lc-shared"]
	require.False(t, ok)
}

// TestManagerReinitTerminatesLifecycles verifies clean re-init: canceling
// the old Manager scope terminates every owned Meta lifecycle and no
// worker is left behind.
func TestManagerReinitTerminatesLifecycles(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	ctx := mockContext.NewMockContext("ruleR", "op1")
	fetchFailDial(t, ctx, "lc-reinit", "refR")
	oldMeta := getReadyTestMeta("lc-reinit")
	require.NotNil(t, oldMeta)
	require.False(t, isDoneClosed(oldMeta.done))

	require.NoError(t, InitConnectionManager4Test())

	require.Error(t, oldMeta.lifecycleCtx.Err(), "old Meta lifecycle must end with re-init")
	require.Eventually(t, func() bool {
		return isDoneClosed(oldMeta.done)
	}, 10*time.Second, 50*time.Millisecond, "old worker must exit after scope cancel")

	// The reset manager is fully functional and starts empty.
	_, err := GetConnectionDetail(ctx, "lc-reinit")
	require.Error(t, err)
	lNew := fetchFailDial(t, ctx, "lc-new", "refN")
	require.True(t, checkConn("lc-new"))
	require.NoError(t, lNew.Release(ctx))
}

// TestManagerResetClosesPublishedConnections proves reset retires the
// whole runtime: a published named connection with zero refs — which
// no detach path would ever stop — is physically Closed exactly once,
// while its KV record is left intact for the next bootstrap reload.
// The reset manager starts empty but fully usable.
func TestManagerResetClosesPublishedConnections(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	drainCountCloseRelease()
	countCloseCalls.Store(0)
	ctx := mockContext.NewMockContext("reset", "op1")

	_, err := CreateNamedConnection(ctx, "reset-named", "countclose", nil)
	require.NoError(t, err)
	_, err = FetchConnectionWithOptions(ctx, FetchOptions{
		ConnectionKey: "reset-anon", RefID: "r1", Type: "mock",
	})
	require.NoError(t, err)

	// Pre-supply the single blocking Close; stopAllRuntime runs
	// synchronously inside re-init.
	countCloseRelease <- struct{}{}
	require.NoError(t, InitConnectionManager4Test())
	require.Equal(t, int32(1), countCloseCalls.Load(), "published named conn must be Closed exactly once")

	// Old runtime holds nothing anymore.
	_, err = GetConnectionDetail(ctx, "reset-named")
	require.Error(t, err)
	_, err = GetConnectionDetail(ctx, "reset-anon")
	require.Error(t, err)

	// The named KV record survives reset for the next bootstrap.
	cfgs, kerr := conf.GetCfgFromKVStorage("connections", "countclose", "reset-named")
	require.NoError(t, kerr)
	require.NotEmpty(t, cfgs, "reset must not delete persistent records")

	// Replacement manager works normally.
	lReset, err := FetchConnectionWithOptions(ctx, FetchOptions{
		ConnectionKey: "reset-new", RefID: "r1", Type: "mock",
	})
	require.NoError(t, err)
	require.NoError(t, lReset.Release(ctx))

	// Leave no persistent residue for other tests (a Drop on the new
	// manager would find nothing to drop, so remove the record directly).
	require.NoError(t, conf.DropCfgKeyFromStorage("connections", "countclose", "reset-named"))
	cfgs, kerr = conf.GetCfgFromKVStorage("connections", "countclose", "reset-named")
	require.NoError(t, kerr)
	require.Empty(t, cfgs)
}

// TestWaitCallerCancelReturnsCtxErr proves Wait never returns (nil, nil):
// a caller going away while the worker still retries observes cancellation.
func TestWaitCallerCancelReturnsCtxErr(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	rootCtx := mockContext.NewMockContext("waitcancel", "op1")
	ctx, cancel := rootCtx.WithCancel()
	cw, err := FetchConnectionWithOptions(ctx, FetchOptions{
		ConnectionKey: "wait-cancel",
		RefID:         "r1",
		Type:          "faildial",
	})
	require.NoError(t, err)
	cancel()
	// The worker runs on the Manager scope, so it keeps retrying: only
	// the caller ctx is done, deterministically.
	_, err = cw.Wait(ctx)
	require.ErrorIs(t, err, goctx.Canceled)
	require.NoError(t, cw.Release(ctx))
}

// TestWaitLifecycleStopReturnsClosed proves a waiter on a terminated
// scope observes ErrConnectionClosed instead of a nil result or a
// connection of a dead scope.
func TestWaitLifecycleStopReturnsClosed(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	ctx := mockContext.NewMockContext("waitclosed", "op1")
	cw, err := FetchConnectionWithOptions(ctx, FetchOptions{
		ConnectionKey: "wait-closed",
		RefID:         "r1",
		Type:          "faildial",
	})
	require.NoError(t, err)
	require.NoError(t, cw.Release(ctx))
	_, err = cw.Wait(ctx)
	require.ErrorIs(t, err, ErrConnectionClosed)
}

// TestWaitBothDonePrefersCallerCancel pins the fixed precedence: when the
// caller and the lifecycle are both done, Wait reports ctx.Err(), never
// ErrConnectionClosed. Both scopes are settled before the call, so no
// scheduling is involved.
func TestWaitBothDonePrefersCallerCancel(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	rootCtx := mockContext.NewMockContext("bothdone", "op1")
	ctx, cancel := rootCtx.WithCancel()
	cw, err := FetchConnectionWithOptions(ctx, FetchOptions{
		ConnectionKey: "wait-both",
		RefID:         "r1",
		Type:          "faildial",
	})
	require.NoError(t, err)
	cancel()
	require.NoError(t, cw.Release(ctx))
	_, err = cw.Wait(ctx)
	require.ErrorIs(t, err, goctx.Canceled)
}
