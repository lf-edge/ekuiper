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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

type received struct {
	payload []byte
	meta    map[string]any
}

// Set RABBITMQ_TEST_URL such as amqp://guest:guest@127.0.0.1:5672/ to run against a disposable RabbitMQ server.
func TestSourceSinkRoundTrip(t *testing.T) {
	server := os.Getenv("RABBITMQ_TEST_URL")
	if server == "" {
		t.Skip("RABBITMQ_TEST_URL is not set")
	}
	conn, err := amqp.Dial(server)
	require.NoError(t, err)
	defer conn.Close()
	admin, err := conn.Channel()
	require.NoError(t, err)
	queue := fmt.Sprintf("ekuiper_test_%d", time.Now().UnixNano())
	_, err = admin.QueueDeclare(queue, true, false, false, false, nil)
	require.NoError(t, err)
	defer func() {
		_, err := admin.QueueDelete(queue, false, false, false)
		assert.NoError(t, err)
	}()

	ctx, cancel := mockContext.NewMockContext("rule", "op").WithCancel()
	defer cancel()

	// Ping
	require.NoError(t, GetSource().(*source).Ping(ctx, map[string]any{"server": server, "datasource": queue}))
	require.ErrorContains(t, GetSource().(*source).Ping(ctx, map[string]any{"server": server, "datasource": queue + "_missing"}), "NOT_FOUND")
	require.NoError(t, GetSink().(*sink).Ping(ctx, map[string]any{"server": server}))

	// Source
	src := GetSource().(*source)
	require.NoError(t, src.Provision(ctx, map[string]any{"server": server, "datasource": queue, "prefetchCount": 2}))
	srcStatus := &statusRecorder{}
	require.NoError(t, src.Connect(ctx, srcStatus.handle))
	status, _, _ := srcStatus.last()
	require.Equal(t, api.ConnectionConnected, status)
	messages := make(chan received, 10)
	subscribed := make(chan error)
	go func() {
		subscribed <- src.Subscribe(ctx, func(_ api.StreamContext, payload []byte, meta map[string]any, _ time.Time) {
			messages <- received{payload: payload, meta: meta}
		}, nil)
	}()
	receive := func() received {
		select {
		case m := <-messages:
			return m
		case <-time.After(10 * time.Second):
			t.Fatal("no message received")
		}
		return received{}
	}

	// Sink
	snk := GetSink().(*sink)
	require.NoError(t, snk.Provision(ctx, map[string]any{
		"server":           server,
		"routingKey":       "{{.key}}",
		"contentType":      "application/json",
		"persistent":       true,
		"publisherConfirm": true,
		"headers":          map[string]any{"device": "{{.device}}", "fixed": "f"},
	}))
	snkStatus := &statusRecorder{}
	require.NoError(t, snk.Connect(ctx, snkStatus.handle))
	status, _, _ = snkStatus.last()
	require.Equal(t, api.ConnectionConnected, status)
	item := &testx.MockRawTuple{
		Content:  []byte(`{"temperature":20}`),
		Template: map[string]string{"{{.key}}": queue, "{{.device}}": "d1"},
	}
	for i := 0; i < 3; i++ {
		require.NoError(t, snk.Collect(ctx, item))
	}
	for i := 0; i < 3; i++ {
		m := receive()
		require.Equal(t, `{"temperature":20}`, string(m.payload))
		require.Equal(t, "", m.meta["exchange"])
		require.Equal(t, queue, m.meta["routingKey"])
		require.Equal(t, false, m.meta["redelivered"])
		require.Equal(t, "application/json", m.meta["contentType"])
		require.Equal(t, map[string]any{"device": "d1", "fixed": "f"}, m.meta["headers"])
		require.IsType(t, int64(0), m.meta["timestamp"])
	}
	// The messages are acknowledged so the queue is drained
	require.Eventually(t, func() bool {
		q, err := admin.QueueDeclarePassive(queue, false, false, false, false, nil)
		return err == nil && q.Messages == 0
	}, 5*time.Second, 50*time.Millisecond)

	// The source reconnects after the server cancels the consumer
	_, err = admin.QueueDelete(queue, false, false, false)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		status, message, _ := srcStatus.last()
		return status == api.ConnectionDisconnected && message != ""
	}, 5*time.Second, 10*time.Millisecond)
	_, err = admin.QueueDeclare(queue, true, false, false, false, nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		status, _, _ := srcStatus.last()
		return status == api.ConnectionConnected
	}, 15*time.Second, 10*time.Millisecond)
	require.NoError(t, snk.Collect(ctx, item))
	require.Equal(t, `{"temperature":20}`, string(receive().payload))

	// Publishing to a missing exchange closes the channel. It is reported and the sink reconnects for the next message.
	snk.sc.Exchange = queue + "_missing"
	err = snk.Collect(ctx, item)
	require.True(t, errorx.IsIOError(err))
	require.Eventually(t, func() bool {
		status, message, _ := snkStatus.last()
		return status == api.ConnectionDisconnected && message != ""
	}, 5*time.Second, 10*time.Millisecond)
	snk.sc.Exchange = ""
	require.NoError(t, snk.Collect(ctx, item))
	status, _, _ = snkStatus.last()
	require.Equal(t, api.ConnectionConnected, status)
	require.Equal(t, `{"temperature":20}`, string(receive().payload))

	require.NoError(t, snk.Close(ctx))
	cancel()
	require.NoError(t, <-subscribed)
	require.NoError(t, src.Close(ctx))
}
