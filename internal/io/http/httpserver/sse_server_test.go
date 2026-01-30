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

package httpserver

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/io/memory/pubsub"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestSSEConnectionCloseOnUnregister(t *testing.T) {
	ip := "127.0.0.1"
	port := 10086
	InitGlobalServerManager(ip, port, nil)
	defer ShutDown()

	ctx := mockContext.NewMockContext("1", "2")
	endpoint := "/test/sse1"
	_, sTopic, err := RegisterSSEEndpoint(ctx, endpoint)
	require.NoError(t, err)

	// Create SSE client connection
	clientCtx, clientCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer clientCancel()

	url := fmt.Sprintf("http://%s:%d%s", ip, port, endpoint)
	req, err := http.NewRequestWithContext(clientCtx, http.MethodGet, url, nil)
	require.NoError(t, err)
	req.Header.Set("Accept", "text/event-stream")

	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	// Channel to signal whether connection was closed
	connClosed := make(chan bool, 1)

	// Start reading from SSE stream in a goroutine
	go func() {
		reader := bufio.NewReader(resp.Body)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				// Connection closed
				connClosed <- true
				return
			}
			t.Logf("Received SSE line: %s", line)
		}
	}()

	// Send some data to verify connection is working
	pubsub.ProduceAny(ctx, sTopic, []byte("test message"))

	// Give time for message to be delivered
	time.Sleep(100 * time.Millisecond)

	// Now unregister the endpoint - this should close the connection
	t.Log("Unregistering SSE endpoint...")
	UnRegisterSSEEndpoint(endpoint)
	t.Log("UnRegisterSSEEndpoint returned")

	// Wait for connection to close with timeout
	select {
	case <-connClosed:
		t.Log("Connection closed successfully")
	case <-time.After(2 * time.Second):
		t.Fatal("Connection was not closed after UnRegisterSSEEndpoint")
	}
}

func TestSSEMultipleConnectionsCloseOnUnregister(t *testing.T) {
	ip := "127.0.0.1"
	port := 10087
	InitGlobalServerManager(ip, port, nil)
	defer ShutDown()

	ctx := mockContext.NewMockContext("1", "2")
	endpoint := "/test/sse2"
	_, sTopic, err := RegisterSSEEndpoint(ctx, endpoint)
	require.NoError(t, err)

	// Create multiple SSE client connections
	numClients := 3
	clientContexts := make([]context.Context, numClients)
	clientCancels := make([]context.CancelFunc, numClients)
	connClosed := make([]chan bool, numClients)

	for i := 0; i < numClients; i++ {
		clientContexts[i], clientCancels[i] = context.WithTimeout(context.Background(), 10*time.Second)
		defer clientCancels[i]()

		url := fmt.Sprintf("http://%s:%d%s", ip, port, endpoint)
		req, err := http.NewRequestWithContext(clientContexts[i], http.MethodGet, url, nil)
		require.NoError(t, err)
		req.Header.Set("Accept", "text/event-stream")

		client := &http.Client{
			Timeout: 10 * time.Second,
		}
		resp, err := client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		connClosed[i] = make(chan bool, 1)
		clientIdx := i

		// Start reading from SSE stream
		go func(idx int, r *http.Response) {
			reader := bufio.NewReader(r.Body)
			for {
				_, err := reader.ReadString('\n')
				if err != nil {
					connClosed[idx] <- true
					return
				}
			}
		}(clientIdx, resp)
	}

	// Send data to all clients
	pubsub.ProduceAny(ctx, sTopic, []byte("test message"))
	time.Sleep(100 * time.Millisecond)

	// Unregister endpoint - should close all connections
	t.Log("Unregistering SSE endpoint with multiple connections...")
	UnRegisterSSEEndpoint(endpoint)
	t.Log("UnRegisterSSEEndpoint returned")

	// Verify all connections were closed
	for i := 0; i < numClients; i++ {
		select {
		case <-connClosed[i]:
			t.Logf("Client %d connection closed successfully", i)
		case <-time.After(2 * time.Second):
			t.Fatalf("Client %d connection was not closed after UnRegisterSSEEndpoint", i)
		}
	}
}
