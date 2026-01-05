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

package runtime

import (
	"fmt"
	"testing"
	"time"

	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/rep"
	"go.nanomsg.org/mangos/v3/protocol/req"
	_ "go.nanomsg.org/mangos/v3/transport/ipc"
)

// TestReqSocketTimeoutBehavior tests how REQ socket behaves after recv timeout
func TestReqSocketTimeoutBehavior(t *testing.T) {
	symbolName := "test_timeout"
	url := fmt.Sprintf("ipc:///tmp/func_%s.ipc", symbolName)

	// Create server (REP socket) - like eKuiper side
	serverSock, err := rep.NewSocket()
	if err != nil {
		t.Fatalf("can't create rep socket: %v", err)
	}
	defer serverSock.Close()

	if err = serverSock.Listen(url); err != nil {
		t.Fatalf("can't listen: %v", err)
	}

	// Create client (REQ socket) - like SDK side
	clientSock, err := req.NewSocket()
	if err != nil {
		t.Fatalf("can't create req socket: %v", err)
	}
	defer clientSock.Close()

	// Set short recv timeout to test timeout behavior
	clientSock.SetOption(mangos.OptionRecvDeadline, 500*time.Millisecond)
	clientSock.SetOption(mangos.OptionRetryTime, 0)

	if err = clientSock.Dial(url); err != nil {
		t.Fatalf("can't dial: %v", err)
	}

	// Step 1: Send handshake
	t.Log("Step 1: Sending handshake")
	if err = clientSock.Send([]byte("handshake")); err != nil {
		t.Fatalf("can't send handshake: %v", err)
	}

	// Step 2: Receive handshake on server side, but DON'T reply immediately
	msg, err := serverSock.Recv()
	if err != nil {
		t.Fatalf("server can't recv handshake: %v", err)
	}
	t.Logf("Server received: %s", string(msg))

	// Step 3: Client waits for reply, should timeout
	t.Log("Step 3: Client waiting (should timeout)")
	_, err = clientSock.Recv()
	if err != mangos.ErrRecvTimeout {
		t.Fatalf("expected ErrRecvTimeout, got: %v", err)
	}
	t.Log("Step 3: Got expected timeout")

	// Step 4: Try to recv again WITHOUT sending - test protocol state
	t.Log("Step 4: Try recv again without send")
	_, err = clientSock.Recv()
	t.Logf("Step 4: Result: %v", err)

	// Step 5: Try to send again after timeout
	t.Log("Step 5: Try send after timeout")
	err = clientSock.Send([]byte("keepalive"))
	t.Logf("Step 5: Send result: %v", err)

	// Step 6: If send works, try recv again
	if err == nil {
		t.Log("Step 6: Try recv after send")
		// Server should have another message to receive and reply
		msg, _ = serverSock.Recv()
		t.Logf("Server received: %s", string(msg))
		serverSock.Send([]byte("reply"))

		reply, err := clientSock.Recv()
		t.Logf("Step 6: Recv result: %v, reply: %s", err, string(reply))
	}
}
