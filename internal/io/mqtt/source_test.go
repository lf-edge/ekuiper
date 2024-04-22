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

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/internal/topo/connection/factory"
	"github.com/lf-edge/ekuiper/v2/internal/topo/topotest/mockclock"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/mock"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

// NOTICE!!! Need to run a MQTT broker in localhost:1883 for this test or change the url to your broker
const url = "tcp://127.0.0.1:1883"

func init() {
	factory.InitClientsFactory()
	testx.InitEnv("mqtt_source_connector")
}

func TestProvision(t *testing.T) {
	tests := []struct {
		name  string
		props map[string]any
		err   string
	}{
		{
			name: "Valid configuration",
			props: map[string]any{
				"server":     url,
				"datasource": "demo",
			},
		},
		{
			name: "Invalid configuration",
			props: map[string]any{
				"server":     make(chan any),
				"datasource": "demo",
			},
			err: "1 error(s) decoding:\n\n* 'server' expected type 'string'",
		},
	}
	sc := &SourceConnector{}
	ctx := mockContext.NewMockContext("testprov", "source")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := sc.Provision(ctx, tt.props)
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
	mc := mockclock.GetMockClock()

	// Open and subscribe before sending data
	mock.TestSourceConnector(t, sc, map[string]any{
		"server":     url,
		"datasource": "demo",
	}, []api.Tuple{
		model.NewDefaultRawTuple([]byte("hello"), xsql.Message{
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
