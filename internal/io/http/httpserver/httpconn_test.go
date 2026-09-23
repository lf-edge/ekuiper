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

package httpserver

import (
	"testing"

	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

// TestHttpPushConnectionEndpointOwnership pins the Dial/Close
// ownership: Dial registers, Close unregisters only what this
// instance registered. A pre-Dial Close deletes nothing, a second
// Dial does not double-count, and of two instances sharing one
// endpoint the first Close leaves the route for the other.
func TestHttpPushConnectionEndpointOwnership(t *testing.T) {
	ip := "127.0.0.1"
	port := 10084
	InitGlobalServerManager(ip, port, nil)
	defer ShutDown()
	ctx := mockContext.NewMockContext("push", "op1")
	props := map[string]any{"datasource": "/owned", "method": "POST"}

	// Pre-Dial Close is a no-op.
	idle := &HttpPushConnection{}
	require.NoError(t, idle.Provision(ctx, "idle", props))
	require.NoError(t, idle.Close(ctx))
	require.Equal(t, map[string]struct{}{}, GetEndpoints())

	h1 := &HttpPushConnection{}
	require.NoError(t, h1.Provision(ctx, "h1", props))
	h2 := &HttpPushConnection{}
	require.NoError(t, h2.Provision(ctx, "h2", props))
	require.NoError(t, h1.Dial(ctx))
	require.NoError(t, h2.Dial(ctx))
	require.Equal(t, map[string]struct{}{"/owned$$POST": {}}, GetEndpoints())

	// Dial re-entry on an already-registered instance counts nothing.
	require.NoError(t, h1.Dial(ctx))

	// First holder leaves: route stays for the other.
	require.NoError(t, h1.Close(ctx))
	require.Equal(t, map[string]struct{}{"/owned$$POST": {}}, GetEndpoints())

	// Last holder leaves: no residue; a second Close is a no-op.
	require.NoError(t, h2.Close(ctx))
	require.Equal(t, map[string]struct{}{}, GetEndpoints())
	require.NoError(t, h2.Close(ctx))
	require.Equal(t, map[string]struct{}{}, GetEndpoints())
}
