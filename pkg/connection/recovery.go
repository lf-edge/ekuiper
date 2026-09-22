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
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

var (
	// recoveryVerifyTimeout bounds one verification Ping: a verdict,
	// never a stall. Same bound as the health probe.
	recoveryVerifyTimeout = 5 * time.Second
	// recoveryAttemptTimeout bounds one Recover attempt (candidate
	// build plus verification). Bounded per the attempt contract;
	// the rhythm across attempts belongs to the backoff below.
	recoveryAttemptTimeout = 30 * time.Second
	// newRecoveryBackoff builds the per-worker backoff rhythm. A
	// variable (not a const) so tests run the same loop on
	// millisecond intervals.
	newRecoveryBackoff = func() *backoff.ExponentialBackOff {
		return newExponentialBackOff(0)
	}
)

// startRecoveryWorker launches the Meta-owned recovery loop after a
// successful initial Dial, so even a zero-ref named connection
// recovers. Only pool-recoverable providers qualify: anything else
// returns without starting a worker (recoveryDone stays nil, which
// stop() treats as "nothing to join"). A provider implementing both
// StatefulDialer and PoolRecoverableConnection is treated as
// self-recovering per the c1 contract — the Pool never starts its
// worker. Call before publishing the handle; stop() relies on
// recoveryDone being written before done closes.
func (meta *Meta) startRecoveryWorker(conn modules.Connection) {
	rc, ok := conn.(modules.PoolRecoverableConnection)
	if !ok {
		return
	}
	if _, self := conn.(modules.StatefulDialer); self {
		return
	}
	meta.recoveryDone = make(chan struct{})
	go meta.recoveryLoop(rc, newRecoveryBackoff())
}

// recoveryLoop parks until a fresh wakeup sequence arrives, acts on
// it exactly once, then absorbs every report that landed during the
// attempt: those describe the pre-attempt handle, superseded by its
// verdict. A stale buffered slot (sequence already absorbed) wakes
// the select and re-parks without touching the provider — no Ping,
// no Recover, no status write.
//
// No wakeup is ever lost: every sequence bump pairs with a
// non-blocking send, and a non-empty buffer always trips the select,
// so a parked worker either observes the bump by re-reading the
// sequence or is forced to re-read by the slot. The loop exits only
// on lifecycle termination.
func (meta *Meta) recoveryLoop(conn modules.PoolRecoverableConnection, bo *backoff.ExponentialBackOff) {
	defer close(meta.recoveryDone)
	var acted uint64
	for {
		if meta.currentSuspectSeq() == acted {
			select {
			case <-meta.lifecycleCtx.Done():
				return
			case <-meta.suspectCh:
			}
			continue
		}
		meta.handleSuspect(conn, bo)
		acted = meta.currentSuspectSeq()
	}
}

// handleSuspect acts on one fresh wakeup. The truth table (hard
// invariant 4: the channel is only the wakeup, Meta state is truth):
//
//	connected + verifying -> Ping verify: healthy reopens the gate,
//	  failed records disconnected and falls through to recovery.
//	disconnected          -> confirmed fault (consumer report or a
//	  probe flip): the worker owns the episode from here, retrying
//	  with backoff until success or lifecycle end.
//	anything else         -> stale: connected+ready, recovering owned
//	  by an in-flight attempt, or connecting owned by initial dial.
//
// Provider I/O always runs outside every lock on a bounded
// server-owned attempt scope. A verdict for a superseded episode is
// impossible by construction: reports arriving mid-attempt are
// absorbed by the loop above, and the Ping-healthy reopen is always
// fresher than any report it supersedes.
func (meta *Meta) handleSuspect(conn modules.PoolRecoverableConnection, bo *backoff.ExponentialBackOff) {
	meta.stateMu.RLock()
	status, verifying := meta.status, meta.verifying
	meta.stateMu.RUnlock()
	if status == api.ConnectionConnected && verifying {
		pingCtx, cancel := attemptStreamContext(meta.lifecycleCtx, recoveryVerifyTimeout)
		err := conn.Ping(pingCtx)
		cancel()
		if meta.lifecycleCtx.Err() != nil {
			return
		}
		if err == nil {
			meta.NotifyStatus(api.ConnectionConnected, "")
			return
		}
		meta.NotifyStatus(api.ConnectionDisconnected, err.Error())
		status = api.ConnectionDisconnected
	}
	if status != api.ConnectionDisconnected {
		return
	}
	// Own the disconnected episode: attempt until success or
	// lifecycle end. One attempt per backoff step, never an internal
	// tight loop. New wakeups during the episode are absorbed by the
	// loop above (sequence), so concurrent reports never fork a
	// second episode — and a parked consumer never needs to fail
	// again to keep recovery going. Each cycle re-announces through
	// disconnected/recovering so lastError stays fresh for operators;
	// all of it shares the one parked generation.
	for {
		if !meta.sleepBackoff(bo) {
			return
		}
		meta.NotifyStatus(ConnectionRecovering, "")
		recCtx, cancel := attemptStreamContext(meta.lifecycleCtx, recoveryAttemptTimeout)
		err := conn.Recover(recCtx)
		cancel()
		if meta.lifecycleCtx.Err() != nil {
			return
		}
		if err != nil {
			meta.NotifyStatus(api.ConnectionDisconnected, err.Error())
			continue
		}
		bo.Reset()
		break
	}
	// Success: the provider installed and verified the candidate, so
	// the new handle is healthy by construction. Only this worker
	// leaves recovering for a pool-recovered connection (suspects
	// during recovering are ignored, the probe skips non-connected),
	// and stop() joins this worker before Close, so reopening the
	// gate here is exact.
	meta.NotifyStatus(api.ConnectionConnected, "")
}

// sleepBackoff waits one backoff step, interruptible by lifecycle
// termination. False means stop was requested: return without acting.
func (meta *Meta) sleepBackoff(bo *backoff.ExponentialBackOff) bool {
	timer := time.NewTimer(bo.NextBackOff())
	defer timer.Stop()
	select {
	case <-meta.lifecycleCtx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// currentSuspectSeq reads the wakeup sequence. Guarded by stateMu.
func (meta *Meta) currentSuspectSeq() uint64 {
	meta.stateMu.RLock()
	defer meta.stateMu.RUnlock()
	return meta.suspectSeq
}

// nudgeRecovery records one probe-confirmed fault and wakes the
// recovery worker. The flip the probe just performed is the fact;
// the sequence bump lets the worker tell it apart from a stale
// wakeup. No-op when no worker exists: the slot just sits buffered
// (coalesced) until one ever starts — or forever, harmlessly.
func (meta *Meta) nudgeRecovery() {
	meta.stateMu.Lock()
	meta.suspectSeq++
	meta.stateMu.Unlock()
	select {
	case meta.suspectCh <- struct{}{}:
	default:
	}
}
