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
	"fmt"
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
	var cli paho.Client
	ta.Run("init when no broker", func(t *testing.T) {
		cins1 := NewMQTTControl(addr, "c1")
		cins1.Add("stream1")
		cins1.Add("stream2")
		time.Sleep(1 * time.Second)
		// Create the new MQTT Server.
		server := mqtt.New(nil)
		// Allow all connections.
		_ = server.AddHook(new(auth.AllowHook), nil)
		// Create a TCP listener on a standard port.
		tcp := listeners.NewTCP(listeners.Config{ID: "testcon", Address: ":4883"})
		err := server.AddListener(tcp)
		require.NoError(t, err)
		go func() {
			err = server.Serve()
			require.NoError(t, err)
		}()
		fmt.Println(tcp.Address())
		opts := paho.NewClientOptions().AddBroker(tcp.Address()).SetProtocolVersion(4)
		cli = paho.NewClient(opts)
		token := cli.Connect()
		err = handleToken(token)
		require.NoError(t, err)
		messages := make([]string, 0, 2)
		wg := &sync.WaitGroup{}
		wg.Add(1)
		cli.Subscribe(CtrlTopic, 1, func(client paho.Client, message paho.Message) {
			if len(messages) < 2 {
				messages = append(messages, string(message.Payload()))
			}
			if len(messages) == 2 {
				exp1 := []string{"stream1", "stream2"}
				exp2 := []string{"stream2", "stream1"}
				fmt.Println(messages)
				require.True(t, reflect.DeepEqual(messages, exp1) || reflect.DeepEqual(messages, exp2))
				cli.Unsubscribe("ctrl/subready")
				wg.Done()
			}
		})
		wg.Wait()
		cli.Publish(CtrlAckTopic, 0, false, "stream1")
		time.Sleep(time.Second)
		cli.Subscribe(CtrlTopic, 1, func(client paho.Client, message paho.Message) {
			require.Equal(t, "stream2", string(message.Payload()))
			cli.Unsubscribe("ctrl/subready")
		})
		time.Sleep(time.Second)
		cins1.Rem("stream2")
		require.Nil(t, cins1.cancel)
	})
	ta.Run("init with broker open", func(t *testing.T) {
		cins2 := NewMQTTControl(addr, "c2")
		cins2.Add("stream1")
		wg := &sync.WaitGroup{}
		wg.Add(1)
		cli.Subscribe(CtrlTopic, 1, func(client paho.Client, message paho.Message) {
			require.Equal(t, "stream1", string(message.Payload()))
			cli.Unsubscribe("ctrl/subready")
			wg.Done()
		})
		wg.Wait()
		cins2.Rem("stream1")
		require.Nil(t, cins2.cancel)
		cins2.Add("stream2")
		wg2 := &sync.WaitGroup{}
		wg2.Add(1)
		cli.Subscribe(CtrlTopic, 1, func(client paho.Client, message paho.Message) {
			require.Equal(t, "stream2", string(message.Payload()))
			cli.Unsubscribe("ctrl/subready")
			wg2.Done()
		})
		wg2.Wait()
		cins2.Rem("stream2")
		require.Nil(t, cins2.cancel)
	})
}
