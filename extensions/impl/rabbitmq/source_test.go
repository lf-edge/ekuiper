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
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestSourceProvision(t *testing.T) {
	ctx := mockContext.NewMockContext("rule", "op")
	tests := []struct {
		name  string
		props map[string]any
		conf  *sourceConf
		err   string
	}{
		{
			name:  "invalid server",
			props: map[string]any{"datasource": "q"},
			err:   "server is required",
		},
		{
			name:  "missing queue",
			props: map[string]any{"server": "amqp://localhost/"},
			err:   "datasource is required, it must be the name of the queue to consume",
		},
		{
			name:  "negative prefetchCount",
			props: map[string]any{"server": "amqp://localhost/", "datasource": "q", "prefetchCount": -1},
			err:   "prefetchCount must be in range 0-65535",
		},
		{
			name:  "too large prefetchCount",
			props: map[string]any{"server": "amqp://localhost/", "datasource": "q", "prefetchCount": 65536},
			err:   "prefetchCount must be in range 0-65535",
		},
		{
			name:  "invalid type",
			props: map[string]any{"server": "amqp://localhost/", "datasource": "q", "autoAck": "yes"},
			err:   "read properties fail with error: 1 error(s) decoding:\n\n* 'autoAck' expected type 'bool', got unconvertible type 'string', value: 'yes'",
		},
		{
			name:  "default",
			props: map[string]any{"server": "amqp://localhost/", "datasource": "q"},
			conf:  &sourceConf{Queue: "q"},
		},
		{
			name: "all",
			props: map[string]any{
				"server": "amqp://localhost/", "datasource": "q", "consumerTag": "tag", "prefetchCount": 10, "autoAck": true,
			},
			conf: &sourceConf{Queue: "q", ConsumerTag: "tag", PrefetchCount: 10, AutoAck: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := GetSource().(*source)
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

func TestDeliveryMeta(t *testing.T) {
	ts := time.UnixMilli(1700000000123)
	tests := []struct {
		name     string
		delivery amqp.Delivery
		meta     map[string]any
	}{
		{
			name:     "minimal",
			delivery: amqp.Delivery{RoutingKey: "q"},
			meta:     map[string]any{"exchange": "", "routingKey": "q", "redelivered": false},
		},
		{
			name: "all",
			delivery: amqp.Delivery{
				Exchange:        "ex",
				RoutingKey:      "rk",
				Redelivered:     true,
				ContentType:     "application/json",
				ContentEncoding: "gzip",
				CorrelationId:   "cid",
				ReplyTo:         "reply",
				MessageId:       "mid",
				Type:            "event",
				AppId:           "app",
				Timestamp:       ts,
				Headers: amqp.Table{
					"str":     "v",
					"bytes":   []byte("b"),
					"bool":    true,
					"int8":    int8(-8),
					"uint8":   uint8(8),
					"int16":   int16(-16),
					"uint16":  uint16(16),
					"int32":   int32(-32),
					"uint32":  uint32(32),
					"int64":   int64(64),
					"float32": float32(1.5),
					"float64": 2.5,
					"decimal": amqp.Decimal{Scale: 2, Value: 12345},
					"time":    ts,
					"nil":     nil,
					"table":   amqp.Table{"nested": int32(1), "list": []any{[]byte("x"), int16(2)}},
				},
			},
			meta: map[string]any{
				"exchange":        "ex",
				"routingKey":      "rk",
				"redelivered":     true,
				"contentType":     "application/json",
				"contentEncoding": "gzip",
				"correlationId":   "cid",
				"replyTo":         "reply",
				"messageId":       "mid",
				"type":            "event",
				"appId":           "app",
				"timestamp":       int64(1700000000123),
				"headers": map[string]any{
					"str":     "v",
					"bytes":   "b",
					"bool":    true,
					"int8":    int64(-8),
					"uint8":   int64(8),
					"int16":   int64(-16),
					"uint16":  int64(16),
					"int32":   int64(-32),
					"uint32":  int64(32),
					"int64":   int64(64),
					"float32": 1.5,
					"float64": 2.5,
					"decimal": 123.45,
					"time":    int64(1700000000123),
					"nil":     nil,
					"table":   map[string]any{"nested": int64(1), "list": []any{"x", int64(2)}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.meta, deliveryMeta(tt.delivery))
		})
	}
}

func TestSourceRetry(t *testing.T) {
	ctx, cancel := mockContext.NewMockContext("rule", "op").WithCancel()
	defer cancel()
	s := GetSource().(*source)
	require.NoError(t, s.Provision(ctx, map[string]any{"server": closedServer(t), "datasource": "q"}))
	r := &statusRecorder{}
	// Connect does not fail the rule so that it recovers once the server is available
	require.NoError(t, s.Connect(ctx, r.handle))
	status, message, _ := r.last()
	require.Equal(t, api.ConnectionDisconnected, status)
	require.Contains(t, message, "connect to amqp://127.0.0.1:")
	require.NotContains(t, message, "secret")

	done := make(chan error)
	go func() {
		done <- s.Subscribe(ctx, func(api.StreamContext, []byte, map[string]any, time.Time) {
			t.Error("unexpected message")
		}, nil)
	}()
	// Subscribe keeps retrying
	require.Eventually(t, func() bool {
		_, _, n := r.last()
		return n >= 3
	}, 5*time.Second, 10*time.Millisecond)
	status, _, _ = r.last()
	require.Equal(t, api.ConnectionDisconnected, status)
	// and stops once the rule stops
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("subscribe does not stop")
	}
	require.NoError(t, s.Close(ctx))
}
