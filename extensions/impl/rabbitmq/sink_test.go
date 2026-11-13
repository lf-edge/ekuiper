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

package rabbitmq

import (
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestSinkProvision(t *testing.T) {
	ctx := mockContext.NewMockContext("rule", "op")
	tests := []struct {
		name  string
		props map[string]any
		conf  *sinkConf
		err   string
	}{
		{
			name:  "invalid server",
			props: map[string]any{"routingKey": "q"},
			err:   "server is required",
		},
		{
			name:  "default exchange without routing key",
			props: map[string]any{"server": "amqp://localhost/"},
			err:   "routingKey is required when publishing to the default exchange",
		},
		{
			name:  "invalid type",
			props: map[string]any{"server": "amqp://localhost/", "routingKey": "q", "headers": "a"},
			err:   "read properties fail with error: 1 error(s) decoding:\n\n* 'headers' expected a map, got 'string'",
		},
		{
			name:  "default exchange",
			props: map[string]any{"server": "amqp://localhost/", "routingKey": "q"},
			conf:  &sinkConf{RoutingKey: "q"},
		},
		{
			name:  "exchange without routing key",
			props: map[string]any{"server": "amqp://localhost/", "exchange": "fanout"},
			conf:  &sinkConf{Exchange: "fanout"},
		},
		{
			name: "all",
			props: map[string]any{
				"server": "amqp://localhost/", "exchange": "ex", "routingKey": "{{.key}}", "contentType": "application/json",
				"persistent": true, "publisherConfirm": true, "headers": map[string]any{"h": "{{.h}}"},
			},
			conf: &sinkConf{
				Exchange: "ex", RoutingKey: "{{.key}}", ContentType: "application/json", Persistent: true,
				PublisherConfirm: true, Headers: map[string]string{"h": "{{.h}}"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := GetSink().(*sink)
			err := s.Provision(ctx, tt.props)
			if tt.err != "" {
				require.EqualError(t, err, tt.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.conf, s.sc)
		})
	}
}

func TestSinkNotConnected(t *testing.T) {
	ctx := mockContext.NewMockContext("rule", "op")
	s := GetSink().(*sink)
	require.NoError(t, s.Provision(ctx, map[string]any{"server": closedServer(t), "routingKey": "q"}))
	r := &statusRecorder{}
	// Connect does not fail the rule so that it recovers once the server is available
	require.NoError(t, s.Connect(ctx, r.handle))
	status, message, n := r.last()
	require.Equal(t, api.ConnectionDisconnected, status)
	require.Contains(t, message, "connect to amqp://127.0.0.1:")
	require.NotContains(t, message, "secret")
	require.Equal(t, 1, n)

	item := &testx.MockRawTuple{Content: []byte("hello")}
	// Do not reconnect for every message before the backoff elapses
	err := s.Collect(ctx, item)
	require.True(t, errorx.IsIOError(err))
	require.EqualError(t, err, "rabbitmq sink is not connected")
	_, _, n = r.last()
	require.Equal(t, 1, n)
	// Reconnect after the backoff
	s.retryAt = time.Time{}
	err = s.Collect(ctx, item)
	require.True(t, errorx.IsIOError(err))
	require.ErrorContains(t, err, "connect to amqp://127.0.0.1:")
	_, _, n = r.last()
	require.Equal(t, 2, n)
	require.True(t, s.retryAt.After(time.Now()))
	require.NoError(t, s.Close(ctx))
}
