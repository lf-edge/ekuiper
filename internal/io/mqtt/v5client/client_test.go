// Copyright 2025 EMQ Technologies Co., Ltd.
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

package v5client

import (
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	storefile "github.com/eclipse/paho.golang/paho/store/file"
	"github.com/lf-edge/ekuiper/contract/v2/api"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestStateFilePrefix(t *testing.T) {
	// Same logical connection and role: stable namespace across restarts.
	require.Equal(t, stateFilePrefix("connA", "cli"), stateFilePrefix("connA", "cli"))
	// Different connection keys must not share session state files.
	require.NotEqual(t, stateFilePrefix("connA", "cli"), stateFilePrefix("connB", "cli"))
	// Client and server stores must not share session state files.
	require.NotEqual(t, stateFilePrefix("connA", "cli"), stateFilePrefix("connA", "srv"))
	// Identity material hostile to file names is hashed away, and the
	// resulting prefix must be usable by the file store.
	for _, id := range []string{
		"tcp://user:pass@host:1883/a/b#c",
		"ipc:///tmp/foo bar/../baz",
		"rule/op+#topic",
	} {
		prefix := stateFilePrefix(id, "cli")
		require.NotContains(t, prefix, "/")
		require.NotContains(t, prefix, "#")
		require.NotContains(t, prefix, ":")
		_, err := storefile.New(t.TempDir(), prefix, ".pkt")
		require.NoError(t, err)
	}
}

func TestValidateConfigGeneratesTLSConfig(t *testing.T) {
	ctx, _ := mockContext.NewMockContext("ruleTls", "op1").WithCancel()
	cc, err := ValidateConfig(ctx, map[string]any{
		"server":             "ssl://broker.emqx.io:8883",
		"insecureSkipVerify": true,
	})
	require.NoError(t, err)
	require.Equal(t, "ssl", cc.serverUrl.Scheme)
	require.NotNil(t, cc.tls)
	require.True(t, cc.tls.InsecureSkipVerify)
}

func TestV5MultiTopicSubscribe(t *testing.T) {
	server := mqtt.New(nil)
	// Allow all connections.
	_ = server.AddHook(new(auth.AllowHook), nil)
	// Create a TCP listener on a standard port.
	tcp := listeners.NewTCP(listeners.Config{ID: "testcon", Address: ":12884"})
	err := server.AddListener(tcp)
	require.NoError(t, err)
	go func() {
		_ = server.Serve()
	}()
	defer func() {
		server.Close()
	}()
	url := "mqtt://127.0.0.1:12884"
	dataDir, err := conf.GetDataLoc()
	require.NoError(t, err)
	require.NoError(t, store.SetupDefault(dataDir))
	require.NoError(t, connection.InitConnectionManager4Test())
	ctx, _ := mockContext.NewMockContext("ruleEof", "op1").WithCancel()
	c, err := Provision(ctx, "ruleEof-op1-test1,test2-mqtt", map[string]any{
		"server":     url,
		"datasource": "test1,test2",
		"qos":        0,
	}, func(ctx api.StreamContext) {
	}, func(ctx api.StreamContext, e error) {
	}, func(ctx api.StreamContext) {
	})
	require.NoError(t, err)
	require.NoError(t, c.Connect(ctx))
	// Create a channel to receive the result
	resultCh := make(chan any, 10)
	require.NoError(t, c.Subscribe(ctx, "topic1,topic2", 0, func(ctx api.StreamContext, msg any) {
		resultCh <- msg
	}))
	require.NoError(t, c.Publish(ctx, "topic1", 0, false, []byte{41}, nil))
	require.NoError(t, c.Publish(ctx, "topic2", 0, false, []byte{42}, nil))
	v1 := <-resultCh
	m1, ok := v1.(*paho.Publish)
	require.True(t, ok)
	require.Equal(t, m1.Payload, []byte{41})
	v2 := <-resultCh
	m2, ok := v2.(*paho.Publish)
	require.True(t, ok)
	require.Equal(t, m2.Payload, []byte{42})
	require.NoError(t, c.Unsubscribe(ctx, "test1,test2"))
	c.Disconnect(ctx)
}

// TestV5ResubscribeSameTopic covers the reconnect path: onConnect calls Subscribe again for a topic
// that is already known. The SUBSCRIBE must still carry the topic filter, because MQTT 5 forbids an
// empty one and the broker closes the connection. See https://github.com/lf-edge/ekuiper/issues/4182
func TestV5ResubscribeSameTopic(t *testing.T) {
	server := mqtt.New(nil)
	_ = server.AddHook(new(auth.AllowHook), nil)
	tcp := listeners.NewTCP(listeners.Config{ID: "testresub", Address: ":12885"})
	require.NoError(t, server.AddListener(tcp))
	go func() {
		_ = server.Serve()
	}()
	defer func() {
		server.Close()
	}()
	dataDir, err := conf.GetDataLoc()
	require.NoError(t, err)
	require.NoError(t, store.SetupDefault(dataDir))
	require.NoError(t, connection.InitConnectionManager4Test())
	ctx, _ := mockContext.NewMockContext("ruleResub", "op1").WithCancel()
	c, err := Provision(ctx, map[string]any{
		"server":     "mqtt://127.0.0.1:12885",
		"datasource": "resub",
		"qos":        1,
	}, func(ctx api.StreamContext) {
	}, func(ctx api.StreamContext, e error) {
	}, func(ctx api.StreamContext) {
	})
	require.NoError(t, err)
	require.NoError(t, c.Connect(ctx))
	resultCh := make(chan any, 10)
	handler := func(ctx api.StreamContext, msg any) {
		resultCh <- msg
	}
	require.NoError(t, c.Subscribe(ctx, "resub", 1, handler))
	errCh := make(chan error, 1)
	go func() {
		errCh <- c.Subscribe(ctx, "resub", 1, handler)
	}()
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("re-subscribe did not complete")
	}
	require.NoError(t, c.Publish(ctx, "resub", 1, false, []byte{43}, nil))
	select {
	case v := <-resultCh:
		m, ok := v.(*paho.Publish)
		require.True(t, ok)
		require.Equal(t, []byte{43}, m.Payload)
	case <-time.After(5 * time.Second):
		t.Fatal("no message received after re-subscribe")
	}
	// The route is registered once, so a re-subscribe does not deliver the message twice.
	select {
	case <-resultCh:
		t.Fatal("message delivered twice after re-subscribe")
	case <-time.After(500 * time.Millisecond):
	}
	require.NoError(t, c.Unsubscribe(ctx, "resub"))
	c.Disconnect(ctx)
}
