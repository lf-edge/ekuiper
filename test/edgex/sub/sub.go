// Copyright 2021-2025 EMQ Technologies Co., Ltd.
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

package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/edgexfoundry/go-mod-messaging/v4/messaging"
	"github.com/edgexfoundry/go-mod-messaging/v4/pkg/types"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

func subEventsFromMQTT(host string) {
	msgConfig1 := types.MessageBusConfig{
		Broker: types.HostInfo{
			Host:     host,
			Port:     1883,
			Protocol: "tcp",
		},
		Type: messaging.MQTT,
	}

	if msgClient, err := messaging.NewMessageClient(msgConfig1); err != nil {
		conf.Log.Fatal(err)
	} else {
		if ec := msgClient.Connect(); ec != nil {
			conf.Log.Fatal(ec)
		} else {
			// log.Infof("The connection to edgex messagebus is established successfully.")
			messages := make(chan types.MessageEnvelope)
			topics := []types.TopicChannel{{Topic: "result", Messages: messages}}
			err := make(chan error)
			if e := msgClient.Subscribe(topics, err); e != nil {
				// log.Errorf("Failed to subscribe to edgex messagebus topic %s.\n", e)
				conf.Log.Fatal(e)
			} else {
				count := 0
				for {
					select {
					case e1 := <-err:
						conf.Log.Errorf("%s\n", e1)
						return
					case env := <-messages:
						count++
						r, _ := json.Marshal(env.Payload)
						fmt.Printf("%s\n", r)
						if count == 1 {
							return
						}
					}
				}
			}
		}
	}
}

func main() {
	if len(os.Args) == 3 {
		if v := os.Args[1]; v == "mqtt" {
			subEventsFromMQTT(os.Args[2])
		}
		if v := os.Args[1]; v == "redis" {
			panic("edgex v4 does not support redis")
		}
	}
}
