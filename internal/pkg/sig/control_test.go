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

package sig

import (
	"reflect"
	"sync"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/stretchr/testify/require"
)

// When initialize the instance, broker is not started. Start later
func TestInstances(ta *testing.T) {
	addr := "tcp://127.0.0.1:4883"
	// Shared resources
	var (
		server *mqtt.Server
		tcp    *listeners.TCP
		cli    paho.Client
		lock   sync.Mutex
	)
	
	setupBroker := func() {
		lock.Lock()
		defer lock.Unlock()
		if server != nil {
			return
		}
		server = mqtt.New(nil)
		_ = server.AddHook(new(auth.AllowHook), nil)
		tcp = listeners.NewTCP(listeners.Config{ID: "testcon", Address: ":4883"})
		err := server.AddListener(tcp)
		if err != nil {
			panic(err)
		}
		go func() {
			err = server.Serve()
			if err != nil {
				// panic(err) // might panic on close
			}
		}()
		// Wait for server? usually Serve is async, but listener is added.
		time.Sleep(100 * time.Millisecond)
	}
	
	setupClient := func(t *testing.T) {
		lock.Lock()
		defer lock.Unlock()
		if cli != nil && cli.IsConnected() {
			return
		}
		opts := paho.NewClientOptions().AddBroker(addr).SetProtocolVersion(4)
		cli = paho.NewClient(opts)
		token := cli.Connect()
		require.NoError(t, handleToken(token))
	}

	teardown := func() {
		if cli != nil && cli.IsConnected() {
			cli.Disconnect(250)
		}
		if server != nil {
			server.Close()
		}
	}
	defer teardown()

	ta.Run("init when no broker", func(t *testing.T) {
		cins1 := NewMQTTControl(addr, "c1")
		cins1.Add("stream1")
		cins1.Add("stream2")
		time.Sleep(1 * time.Second)
		
		setupBroker()
		setupClient(t)
		
		messages := make([]string, 0, 2)
		wg := &sync.WaitGroup{}
		wg.Add(1)
		token := cli.Subscribe(CtrlTopic, 1, func(client paho.Client, message paho.Message) {
			if len(messages) < 2 {
				messages = append(messages, string(message.Payload()))
			}
			if len(messages) == 2 {
				wg.Done()
			}
		})
		require.NoError(t, handleToken(token))
		
		if waitTimeout(wg, 5*time.Second) {
			t.Fatal("timeout waiting for messages")
		}
		
		exp1 := []string{"stream1", "stream2"}
		exp2 := []string{"stream2", "stream1"}
		require.True(t, reflect.DeepEqual(messages, exp1) || reflect.DeepEqual(messages, exp2))
		
		token = cli.Unsubscribe(CtrlTopic)
		require.NoError(t, handleToken(token))
		
		// Test Ack
		token = cli.Publish(CtrlAckTopic, 0, false, "stream1")
		require.NoError(t, handleToken(token))
		time.Sleep(time.Second)
		
		wg = &sync.WaitGroup{}
		wg.Add(1)
		token = cli.Subscribe(CtrlTopic, 1, func(client paho.Client, message paho.Message) {
			require.Equal(t, "stream2", string(message.Payload()))
			wg.Done()
		})
		require.NoError(t, handleToken(token))
		
		if waitTimeout(wg, 5*time.Second) {
			t.Fatal("timeout waiting for stream2")
		}
		token = cli.Unsubscribe(CtrlTopic)
		require.NoError(t, handleToken(token))

		cins1.Rem("stream2")
		require.Nil(t, cins1.cancel)
	})

	ta.Run("init with broker open", func(t *testing.T) {
		// Ensure broker is running (if run standalone, this ensures it starts)
		setupBroker()
		setupClient(t)
		
		cins2 := NewMQTTControl(addr, "c2")
		cins2.Add("stream1")
		
		wg := &sync.WaitGroup{}
		wg.Add(1)
		token := cli.Subscribe(CtrlTopic, 1, func(client paho.Client, message paho.Message) {
			if string(message.Payload()) == "stream1" {
				wg.Done()
			}
		})
		require.NoError(t, handleToken(token))
		
		if waitTimeout(wg, 5*time.Second) {
			// It might be missed if we subscribed too late relative to publish?
			// cins2 is new, it connects then publishes.
			// cli is already connected.
			// Should be fine.
			t.Fatal("timeout waiting for stream1")
		}
		token = cli.Unsubscribe(CtrlTopic)
		require.NoError(t, handleToken(token))

		cins2.Rem("stream1")
		require.Nil(t, cins2.cancel)
		
		cins2.Add("stream2")
		
		wg2 := &sync.WaitGroup{}
		wg2.Add(1)
		token = cli.Subscribe(CtrlTopic, 1, func(client paho.Client, message paho.Message) {
			if string(message.Payload()) == "stream2" {
				wg2.Done()
			}
		})
		require.NoError(t, handleToken(token))
		
		if waitTimeout(wg2, 5*time.Second) {
			t.Fatal("timeout waiting for stream2")
		}
		token = cli.Unsubscribe(CtrlTopic)
		require.NoError(t, handleToken(token)) // cleanup
		cins2.Rem("stream2")
		require.Nil(t, cins2.cancel)
	})
}

func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}
