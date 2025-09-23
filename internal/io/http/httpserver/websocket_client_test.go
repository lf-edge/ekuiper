// Copyright 2024 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package httpserver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/io/memory/pubsub"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestWebsocketClient(t *testing.T) {
	tc := newTC()
	s := createWServer(tc)
	defer func() {
		s.Close()
	}()
	ctx := mockContext.NewMockContext("1", "2")
	wc := NewWebsocketClient("ws", s.URL[len("http://"):], "/ws", nil)
	require.NoError(t, wc.Connect())
	rt, st := wc.Run(ctx)
	pubsub.CreatePub(st)
	defer func() {
		pubsub.RemovePub(st)
	}()
	// wait process start
	time.Sleep(100 * time.Millisecond)
	data := []byte("123")
	pubsub.ProduceAny(ctx, st, data)
	require.Equal(t, data, <-tc.recvCh)
	ch := pubsub.CreateSub(rt, nil, "", 1024)
	defer func() {
		pubsub.CloseSourceConsumerChannel(rt, "")
	}()
	tc.sendCh <- data
	require.Equal(t, data, <-ch)
	require.NoError(t, wc.Close(ctx))
}
