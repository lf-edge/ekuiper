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

package connection

import (
	"fmt"
	"maps"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

// FetchOptions carries the explicit connection identity for the new fetch
// path (DESIGN §8, A1a). ConnectionKey selects which logical connection to
// share; RefID identifies which consumer holds it. The two must never be
// mixed: Pool never derives one from the other.
type FetchOptions struct {
	// ConnectionKey is the opaque logical-connection identity. For named
	// connections it is the connectionSelector; for anonymous ones it is
	// the connector-provided canonical key (e.g. resolved SQL DB URL).
	ConnectionKey string
	// RefID identifies the holding consumer. Source/Sink use the
	// rule+op+instance identity; lookup tables use the framework-injected
	// lookup resource identity. It must be saved by the consumer at
	// Connect time and passed back verbatim at detach time.
	RefID string
	// RequireExisting marks a fetch that may only attach to an already
	// existing logical connection: a missing ConnectionKey is an error
	// and anonymous creation is never performed. Used for
	// connectionSelector/named references. False allows creating an
	// anonymous Meta when the key is absent. This describes the fetch
	// mode, not the Meta lifecycle type (Meta.Named).
	RequireExisting bool
	Type            string
	Props           map[string]any
	// StatusHandler receives connection status changes for this ref.
	StatusHandler api.StatusChangeHandler
}

// ConsumerRefID derives the stable Source/Sink consumer identity
// (ruleID + opID + instanceID) for the current context. Connect must save
// the returned value and pass it back verbatim to DetachConnectionByRef.
func ConsumerRefID(ctx api.StreamContext) string {
	return extractRefId(ctx)
}

// FetchConnectionWithOptions is the explicit-identity fetch path. Callers
// must canonicalize props/key and compute RefID before calling; Pool treats
// ConnectionKey as an opaque identity.
func FetchConnectionWithOptions(ctx api.StreamContext, opts FetchOptions) (*ConnWrapper, error) {
	failpoint.Inject("FetchConnectionErr", func() {
		failpoint.Return(nil, fmt.Errorf("FetchConnectionErr"))
	})
	if opts.ConnectionKey == "" {
		return nil, fmt.Errorf("connection key should be defined")
	}
	if opts.RefID == "" {
		return nil, fmt.Errorf("connection ref id should be defined")
	}
	if opts.Type == "" {
		return nil, fmt.Errorf("connection type should be defined")
	}
	// No Manager lock held here: fetchInternal manages lock/unlock
	// internally (it must release the lock while waiting on or running
	// creation).
	return fetchInternal(ctx, opts)
}

// FetchConnection is the legacy compatibility shim. It keeps the historical
// connectionKey derivation (connectionSelector-or-refId) so unmigrated
// connectors keep resolving the same logical connection. The consumer ref,
// however, is normalized to ConsumerRefID(ctx): legacy DetachConnection
// derives exactly that value, and the historical habit of passing
// connection-identity material (endpoint/topic/URL) as refId never
// identified the consumer. Without this normalization every legacy Close
// would miss its ref and leak the reference. New code must use
// FetchConnectionWithOptions instead.
func FetchConnection(ctx api.StreamContext, refId, typ string, props map[string]interface{}, sc api.StatusChangeHandler) (*ConnWrapper, error) {
	failpoint.Inject("FetchConnectionErr", func() {
		failpoint.Return(nil, fmt.Errorf("FetchConnectionErr"))
	})
	if refId == "" {
		return nil, fmt.Errorf("connection ref id should be defined")
	}
	conId := extractSelID(props, refId)
	opts := FetchOptions{
		ConnectionKey:   conId,
		RefID:           ConsumerRefID(ctx),
		RequireExisting: conId != refId,
		Type:            typ,
		Props:           props,
		StatusHandler:   sc,
	}
	// Same as above: fetchInternal owns its locking.
	return fetchInternal(ctx, opts)
}

// reserveCreating installs a creating reservation for key under one
// critical section. It reports false when the key is already present,
// so Fetch/Create/Reload share a single reservation path.
func (m *Manager) reserveCreating(key string) (*poolEntry, bool) {
	m.Lock()
	defer m.Unlock()
	if _, ok := m.connectionPool[key]; ok {
		return nil, false
	}
	e := &poolEntry{state: entryCreating, ready: make(chan struct{})}
	m.connectionPool[key] = e
	return e, true
}

// fetchPlanKind is the locked decision for one fetch attempt. The lock is
// held only inside planFetch (defer-unlocked); waiting, creation and
// logging-free returns happen outside it — except attach, which stays
// under the lock so a ready attach and a last-detach teardown remain
// mutually exclusive (attach still reaches the preexisting GetStatus path,
// A2 debt, stated here so nobody mistakes this for final lock discipline).
type fetchPlanKind int

const (
	fetchCreate fetchPlanKind = iota
	fetchAttached
	fetchWaitCreating
	fetchWaitRemoving
	fetchFailed
)

type fetchPlan struct {
	kind fetchPlanKind
	// fetchCreate: reserved entry plus the immutable props clone.
	entry *poolEntry
	props map[string]any
	// fetchAttached: handle attached atomically under the lock.
	cw *ConnWrapper
	// fetchAttached: Meta owning the handle, for the post-unlock
	// initial delivery (deliverInitial runs outside the Manager
	// lock per the lock invariant; the plan only registers).
	attachedMeta *Meta
	// fetchWaitCreating: creation entry to wait on (e.err read after
	// ready-close is safe: immutable past close).
	waitEntry *poolEntry
	wait      <-chan struct{}
	// fetchWaitRemoving: teardown channel to wait out.
	removed <-chan struct{}
	err     error
}

// planFetch decides one fetch attempt under a single critical section.
func (m *Manager) planFetch(ctx api.StreamContext, opts FetchOptions) fetchPlan {
	m.Lock()
	defer m.Unlock()
	e, ok := m.connectionPool[opts.ConnectionKey]
	if !ok {
		if opts.RequireExisting {
			return fetchPlan{kind: fetchFailed, err: fmt.Errorf("connection %s not existed", opts.ConnectionKey)}
		}
		e := &poolEntry{state: entryCreating, ready: make(chan struct{})}
		m.connectionPool[opts.ConnectionKey] = e
		// Shallow-copy the caller map: Meta.Props is immutable once
		// inside the Pool. maps.Clone(nil) is nil, so no nil guard
		// needed. Connectors requiring deep-copy semantics must
		// normalize before fetching.
		return fetchPlan{kind: fetchCreate, entry: e, props: maps.Clone(opts.Props)}
	}
	switch e.state {
	case entryReady:
		if err := checkCompatible(e.meta, opts); err != nil {
			return fetchPlan{kind: fetchFailed, err: err}
		}
		conf.Log.Infof("FetchConnection return existed conn %s", e.meta.ID)
		if e.meta.ID != opts.RefID {
			conf.Log.Infof("action=reuse_connection connId=%s type=%s connectionKey=%s rule=%s op=%s refId=%s", e.meta.ID, opts.Type, e.meta.ID, ctx.GetRuleId(), ctx.GetOpId(), opts.RefID)
		}
		cw, err := attachToMeta(e.meta, opts.RefID, opts.StatusHandler)
		if err != nil {
			return fetchPlan{kind: fetchFailed, err: err}
		}
		return fetchPlan{kind: fetchAttached, cw: cw, attachedMeta: e.meta}
	case entryCreating:
		return fetchPlan{kind: fetchWaitCreating, waitEntry: e, wait: e.ready}
	default: // entryRemoving
		if opts.RequireExisting {
			return fetchPlan{kind: fetchFailed, err: ErrConnectionRemoving}
		}
		// A teardown owns this key. Wait it out (or caller
		// cancel) and re-resolve: after cleanup the key is
		// absent and this fetch creates fresh, exactly as if
		// it had blocked on the old global-lock Close path.
		if e.removed == nil {
			return fetchPlan{kind: fetchFailed, err: ErrConnectionRemoving}
		}
		return fetchPlan{kind: fetchWaitRemoving, removed: e.removed}
	}
}

// fetchInternal implements lookup-or-create plus attach. Resolution and
// attach share one Manager critical section per attempt (inside
// planFetch), so a ready attach and a last-detach teardown are mutually
// exclusive: either the fetch lands first (the later detach sees refs
// and stands down) or the teardown wins (the fetch observes removing,
// never the dying Meta).
//
//   - Ready entry: compatibility-check, then attach, all under the lock.
//   - Creating entry: wait for the round, then re-resolve by key (the
//     entry pointer may have turned over while waiting).
//   - Removing entry: anonymous fetches wait for cleanup and retry the
//     whole lookup; RequireExisting fetches fail fast.
//   - Missing key: RequireExisting fails; otherwise this caller becomes
//     the single-flight creator.
//
// The initiating caller ctx only bounds waiting. Shared creation runs on
// the manager scope, so one caller going away never cancels creation for
// the others.
//
// fetchInternal itself never touches the Manager lock; every attempt goes
// through planFetch.
func fetchInternal(ctx api.StreamContext, opts FetchOptions) (*ConnWrapper, error) {
	for {
		m := globalConnectionManager.Load()
		plan := m.planFetch(ctx, opts)
		switch plan.kind {
		case fetchAttached:
			// Initial delivery runs outside the Manager lock
			// (lock invariant): a slow consumer stalls only its
			// own delivery, never the Pool.
			plan.attachedMeta.deliverInitial(opts.RefID, opts.StatusHandler)
			return plan.cw, nil
		case fetchFailed:
			return nil, plan.err
		case fetchCreate:
			meta, err := m.runCreation(plan.entry, opts.ConnectionKey, opts.Type, plan.props, false, nil, opts.RefID, opts.StatusHandler)
			if err != nil {
				return nil, err
			}
			conf.Log.Infof("FetchConnection return new conn %s", meta.ID)
			return meta.cw, nil
		case fetchWaitCreating:
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-plan.wait:
			}
			// plan.waitEntry is immutable past ready-close, so reading
			// err here is safe even if the key has since turned over.
			if plan.waitEntry.err != nil {
				return nil, plan.waitEntry.err
			}
			continue
		default: // fetchWaitRemoving
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-plan.removed:
				continue
			}
		}
	}
}

// Return the unique connection id and whether it is set explicitly
func extractSelID(props map[string]interface{}, anomId string) string {
	if len(props) < 1 {
		return anomId
	}
	v, ok := props["connectionSelector"]
	if !ok {
		return anomId
	}
	id, ok := v.(string)
	if !ok {
		return anomId
	}
	return id
}

func extractRefId(ctx api.StreamContext) string {
	return fmt.Sprintf("%s_%s_%d", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId())
}
