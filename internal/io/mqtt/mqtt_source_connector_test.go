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
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/io/mock"
	"github.com/lf-edge/ekuiper/internal/testx"
	"github.com/lf-edge/ekuiper/internal/topo/connection/factory"
	"github.com/lf-edge/ekuiper/pkg/api"
)

// NOTICE!!! Need to run a MQTT broker in localhost:1883 for this test or change the url to your broker
const url = "tcp://syno.home:1883"

func init() {
	factory.InitClientsFactory()
	testx.InitEnv("mqtt_source_connector")
}

func TestPing(t *testing.T) {
	sc := &SourceConnector{}
	err := sc.Ping("demo", map[string]any{
		"server": url,
	})
	assert.NoError(t, err)
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
