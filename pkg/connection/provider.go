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
	"strings"

	"github.com/cenkalti/backoff/v4"
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

// provisionConnection runs provider lookup plus the synchronous static
// Provision phase. It performs no Dial, no retry and no remote
// round-trip: static errors fail here, before any Meta is published or
// worker started.
//
// Ownership follows modules.Connection: Provision builds the local
// candidate (validation plus any local candidate state/resource) and a
// nil return transfers it to the Pool. The Pool may Close the candidate
// at any time afterwards — including on a later creation-step failure
// before any Dial, or when the lifecycle stops before Dial completes —
// so Close must stay safe on a provisioned-but-never-dialed object.
// A non-nil return means Provision itself failed: the provider owns
// cleanup of any partial state, the Pool closes nothing.
func provisionConnection(ctx api.StreamContext, key, typ string, props map[string]any) (modules.Connection, error) {
	connRegister, ok := modules.GetConnectionProvider(strings.ToLower(typ))
	if !ok {
		return nil, fmt.Errorf("unknown connection type")
	}
	conn := connRegister(ctx)
	if err := conn.Provision(ctx, key, props); err != nil {
		return nil, err
	}
	return conn, nil
}

// dialInitial runs the unbounded initial Dial retry loop for an already
// provisioned connection. The loop only ends on success, on a permanent
// (non-IO) Dial error, or on context death. Context death is reported as
// a permanent error carrying ctx.Err(): lifecycle termination must never
// look like a successful Dial. Callers map it further (the Pool worker
// translates its own lifecycle death to ErrConnectionClosed).
func dialInitial(connCtx api.StreamContext, meta *Meta, conn modules.Connection) (modules.Connection, error) {
	var err error
	sc, isStateful := conn.(modules.StatefulDialer)
	if isStateful {
		sc.SetStatusChangeHandler(connCtx, meta.NotifyStatus)
	}
	err = backoff.Retry(func() error {
		select {
		case <-connCtx.Done():
			return backoff.Permanent(connCtx.Err())
		default:
		}
		meta.NotifyStatus(api.ConnectionConnecting, "")
		connCtx.GetLogger().Debugf("connection retry: %s", meta.ID)
		err = conn.Dial(connCtx)
		if err == nil {
			if !isStateful {
				meta.NotifyStatus(api.ConnectionConnected, "")
			}
			return nil
		}
		connCtx.GetLogger().Debugf("connection failed: %s, %v", meta.ID, err)
		meta.NotifyStatus(api.ConnectionDisconnected, err.Error())
		if errorx.IsIOError(err) {
			return err
		}
		return backoff.Permanent(err)
		// No max elapsed time: pooled connections keep retrying until their
		// lifecycle context ends. Consumers decide whether and when to wait
		// for the ConnWrapper to become ready.
	}, NewExponentialBackOffWithMaxElapsedTime(0))
	return conn, err
}
