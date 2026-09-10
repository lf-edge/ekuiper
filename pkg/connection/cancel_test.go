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
	stdContext "context"
	"sync"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

type pendingConnection struct {
	mockConnection
	started chan struct{}
	release chan struct{}
	closed  chan struct{}
	once    sync.Once
	retry   bool
}

func (c *pendingConnection) Dial(ctx api.StreamContext) error {
	c.once.Do(func() { close(c.started) })
	if c.retry {
		return errorx.NewIOErr("not available")
	}
	<-c.release
	return nil
}

func (c *pendingConnection) Close(ctx api.StreamContext) error {
	close(c.closed)
	return nil
}

func TestRemovePendingNamedConnection(t *testing.T) {
	for _, operation := range []string{"drop", "update"} {
		for _, retry := range []bool{true, false} {
			name := "inflight"
			if retry {
				name = "retry"
			}
			t.Run(operation+"/"+name, func(t *testing.T) {
				require.NoError(t, InitConnectionManager4Test())
				ctx, cancel := context.Background().WithCancel()
				defer cancel()
				conn := &pendingConnection{
					started: make(chan struct{}), release: make(chan struct{}),
					closed: make(chan struct{}), retry: retry,
				}
				modules.RegisterConnection("pending", func(api.StreamContext) modules.Connection { return conn })
				cw, err := CreateNamedConnection(ctx, "pending", "pending", nil)
				require.NoError(t, err)
				select {
				case <-conn.started:
				case <-time.After(2 * time.Second):
					t.Fatal("connection did not start")
				}
				require.False(t, cw.IsInitialized())
				if operation == "update" {
					replacement, err := UpdateConnection(ctx, "pending", "mock", nil)
					require.NoError(t, err)
					_, err = replacement.Wait(ctx)
					require.NoError(t, err)
					require.True(t, checkConn("pending"))
					require.NoError(t, DropNameConnection(ctx, "pending"))
				} else {
					require.NoError(t, DropNameConnection(ctx, "pending"))
				}
				close(conn.release)
				require.False(t, checkConn("pending"))
				select {
				case <-conn.closed:
				case <-time.After(2 * time.Second):
					t.Fatal("deleted connection was not closed")
				}
				_, err = cw.Wait(ctx)
				require.ErrorIs(t, err, stdContext.Canceled)
				require.NoError(t, ctx.Err())
			})
		}
	}
}
