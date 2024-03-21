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

package mqtt

import (
	"log"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/testx"
	"github.com/lf-edge/ekuiper/internal/topo/connection/factory"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/mock"
	mockContext "github.com/lf-edge/ekuiper/pkg/mock/context"
)

// NOTICE!!! Need to run a MQTT broker in localhost:1883 for this test or change the url to your broker
const url = "tcp://127.0.0.1:1883"

func init() {
	factory.InitClientsFactory()
	testx.InitEnv("mqtt_source_connector")
}

func TestPing(t *testing.T) {
	tests := []struct {
		name  string
		props map[string]any
		err   string
	}{
		{
			name: "Valid configuration",
			props: map[string]any{
				"server": url,
			},
		},
		{
			name: "Invalid configuration",
			props: map[string]any{
				"server": make(chan any),
			},
			err: "1 error(s) decoding:\n\n* 'server' expected type 'string'",
		},
		{
			name: "Runtime error",
			props: map[string]any{
				"server": "not exist",
			},
			err: "found error when connecting for not exist: no servers defined to connect to",
		},
	}
	sc := &SourceConnector{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := sc.Ping("demo", tt.props)
			if tt.err != "" {
				assert.Error(t, err)
				require.True(t, strings.HasPrefix(err.Error(), tt.err))
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestOpen(t *testing.T) {
	sc := &SourceConnector{}
	// Test configure
	err := sc.Configure("demo", map[string]any{
		"server": url,
	})
	assert.NoError(t, err)
	mc := conf.Clock.(*clock.Mock)

	// Open and subscribe before sending data
	mock.TestSourceConnector(t, sc, []api.SourceTuple{
		api.NewDefaultRawTuple([]byte("hello"), map[string]any{
			"topic":     "demo",
			"messageId": uint16(0),
			"qos":       byte(0),
		}, mc.Now()),
	}, func() {
		opts := mqtt.NewClientOptions().AddBroker(url)
		client := mqtt.NewClient(opts)
		defer client.Disconnect(0)
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			panic(token.Error())
		}
		data := [][]byte{
			[]byte("hello"),
		}
		topic := "demo"
		for _, d := range data {
			time.Sleep(time.Duration(10) * time.Millisecond)
			if token := client.Publish(topic, 0, false, d); token.Wait() && token.Error() != nil {
				log.Println(token.Error())
			} else {
				log.Println("publish success")
			}
		}
	})
}

func TestOnMsgCancel(t *testing.T) {
	sc := &SourceConnector{}
	sc.consumer = make(chan<- api.SourceTuple, 10)
	sc.onMessage(mockContext.NewMockContext("1", "1"), MockMessage{})
	sc.onError(mockContext.NewMockContext("1", "1"), nil)

	require.NoError(t, failpoint.Enable("github.com/lf-edge/ekuiper/internal/io/mqtt/ctxCancel", "return(ture)"))
	defer func() {
		failpoint.Disable("github.com/lf-edge/ekuiper/internal/io/mqtt/ctxCancel")
	}()
	ctx, cancel := context.Background().WithCancel()
	cancel()
	time.Sleep(100 * time.Millisecond)
	sc.onMessage(ctx, nil)
	sc.onError(ctx, nil)
}

// MockMessage implements the Message interface and allows for control over the returned data when a MessageHandler is
// invoked.
type MockMessage struct {
	payload []byte
	topic   string
}

func (mm MockMessage) Payload() []byte {
	return mm.payload
}

func (MockMessage) Duplicate() bool {
	panic("function not expected to be invoked")
}

func (MockMessage) Qos() byte {
	return 0
}

func (MockMessage) Retained() bool {
	panic("function not expected to be invoked")
}

func (mm MockMessage) Topic() string {
	return mm.topic
}

func (MockMessage) MessageID() uint16 {
	return 0
}

func (MockMessage) Ack() {
	panic("function not expected to be invoked")
}
