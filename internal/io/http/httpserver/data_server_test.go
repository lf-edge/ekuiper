// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/io/memory/pubsub"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
)

func TestEndpoints(t *testing.T) {
	ip := "127.0.0.1"
	port := 10082
	InitGlobalServerManager(ip, port, nil)
	defer ShutDown()
	endpoints := []string{
		"/ee1", "/eb2", "/ec3",
	}
	RegisterEndpoint(endpoints[0], "POST")
	RegisterEndpoint(endpoints[1], "PUT")
	RegisterEndpoint(endpoints[2], "POST")
	require.Equal(t, map[string]struct{}{
		"/ee1$$POST": {}, "/eb2$$PUT": {}, "/ec3$$POST": {},
	}, GetEndpoints())
	UnregisterEndpoint(endpoints[0], "POST")
	UnregisterEndpoint(endpoints[1], "PUT")
	UnregisterEndpoint(endpoints[2], "POST")
	require.Equal(t, map[string]struct{}{}, GetEndpoints())

	urlPrefix := fmt.Sprintf("http://%v:%v", ip, port)
	client := &http.Client{}
	RegisterEndpoint(endpoints[0], "POST")
	RegisterEndpoint(endpoints[1], "PUT")
	var err error
	// wait for http server start
	for i := 0; i < 3; i++ {
		err = testx.TestHttp(client, urlPrefix+endpoints[1], "PUT")
		if err == nil {
			break
		}
		time.Sleep(time.Millisecond * 500)
	}
	require.NoError(t, err)
}

func GetEndpoints() map[string]struct{} {
	return manager.GetEndpoints()
}

func (m *GlobalServerManager) GetEndpoints() map[string]struct{} {
	ma := make(map[string]struct{})
	m.Lock()
	defer m.Unlock()
	for k := range m.endpoint {
		ma[k] = struct{}{}
	}
	return ma
}

// TestSamePathMethodsRouteIndependently pins the per-method routing:
// POST and PUT on one path are independent registrations sharing only
// the path. Each request must reach its own topic, and unregistering
// POST must leave PUT serving (no 404 from a shared route slot).
func TestSamePathMethodsRouteIndependently(t *testing.T) {
	ip := "127.0.0.1"
	port := 10085
	InitGlobalServerManager(ip, port, nil)
	defer ShutDown()
	urlPrefix := fmt.Sprintf("http://%v:%v", ip, port)
	client := &http.Client{}

	postTopic, err := RegisterEndpoint("/dual", "POST")
	require.NoError(t, err)
	putTopic, err := RegisterEndpoint("/dual", "PUT")
	require.NoError(t, err)
	require.NotEqual(t, postTopic, putTopic)
	postSub := pubsub.CreateSub(postTopic, nil, "dual-post", 16)
	putSub := pubsub.CreateSub(putTopic, nil, "dual-put", 16)

	// wait for http server start
	var lastErr error
	for i := 0; i < 6; i++ {
		lastErr = testx.TestHttp(client, urlPrefix+"/dual", "PUT")
		if lastErr == nil {
			break
		}
		time.Sleep(time.Millisecond * 500)
	}
	require.NoError(t, lastErr)
	require.NoError(t, testx.TestHttp(client, urlPrefix+"/dual", "POST"))

	select {
	case <-postSub:
	case <-time.After(2 * time.Second):
		t.Fatal("POST request did not reach the POST topic")
	}
	select {
	case <-putSub:
	case <-time.After(2 * time.Second):
		t.Fatal("PUT request did not reach the PUT topic")
	}

	// Unregistering POST leaves PUT serving; POST goes 404.
	UnregisterEndpoint("/dual", "POST")
	require.Error(t, testx.TestHttp(client, urlPrefix+"/dual", "POST"))
	require.NoError(t, testx.TestHttp(client, urlPrefix+"/dual", "PUT"))

	UnregisterEndpoint("/dual", "PUT")
	require.Equal(t, map[string]struct{}{}, GetEndpoints())
}

// HttpPushConnection: two holders (e.g. a named and an anonymous
// connection) may register the same endpoint; the first Unregister
// only drops its own reference, and the route disappears only after
// the last holder leaves. Unregistering a never-registered endpoint
// is a no-op.
// TestSharedEndpointRefcount pins the registry ownership backing
// HttpPushConnection: two holders (e.g. a named and an anonymous
// connection) may register the same endpoint; the first Unregister
// only drops its own reference, and the route disappears only after
// the last holder leaves. Unregistering a never-registered endpoint
// is a no-op.
func TestSharedEndpointRefcount(t *testing.T) {
	ip := "127.0.0.1"
	port := 10083
	InitGlobalServerManager(ip, port, nil)
	defer ShutDown()

	UnregisterEndpoint("/nope", "POST")
	require.Equal(t, map[string]struct{}{}, GetEndpoints())

	topic1, err := RegisterEndpoint("/shared", "POST")
	require.NoError(t, err)
	topic2, err := RegisterEndpoint("/shared", "POST")
	require.NoError(t, err)
	require.Equal(t, topic1, topic2)
	require.Equal(t, map[string]struct{}{"/shared$$POST": {}}, GetEndpoints())

	UnregisterEndpoint("/shared", "POST")
	require.Equal(t, map[string]struct{}{"/shared$$POST": {}}, GetEndpoints())

	UnregisterEndpoint("/shared", "POST")
	require.Equal(t, map[string]struct{}{}, GetEndpoints())
}
