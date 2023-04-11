//go:build edgex
// +build edgex

/*
 * Copyright 2023 EMQ Technologies Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"fmt"
	"github.com/edgexfoundry/go-mod-messaging/v3/messaging"
	"github.com/edgexfoundry/go-mod-messaging/v3/pkg/types"
	"github.com/lf-edge/ekuiper/internal/conf"
	"os"
)

func subEventsFromMQTT(host string) {
	var msgConfig1 = types.MessageBusConfig{
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
			//log.Infof("The connection to edgex messagebus is established successfully.")
			messages := make(chan types.MessageEnvelope)
			topics := []types.TopicChannel{{Topic: "result", Messages: messages}}
			err := make(chan error)
			if e := msgClient.Subscribe(topics, err); e != nil {
				//log.Errorf("Failed to subscribe to edgex messagebus topic %s.\n", e)
				conf.Log.Fatal(e)
			} else {
				var count = 0
				for {
					select {
					case e1 := <-err:
						conf.Log.Errorf("%s\n", e1)
						return
					case env := <-messages:
						count++
						fmt.Printf("%s\n", env.Payload)
						if count == 1 {
							return
						}
					}
				}
			}
		}
	}
}

func subEventsFromRedis(host string) {
	var msgConfig1 = types.MessageBusConfig{
		Broker: types.HostInfo{
			Host:     host,
			Port:     6379,
			Protocol: "redis",
		},
		Type: messaging.Redis,
	}

	if msgClient, err := messaging.NewMessageClient(msgConfig1); err != nil {
		conf.Log.Fatal(err)
	} else {
		if ec := msgClient.Connect(); ec != nil {
			conf.Log.Fatal(ec)
		} else {
			//log.Infof("The connection to edgex messagebus is established successfully.")
			messages := make(chan types.MessageEnvelope)
			topics := []types.TopicChannel{{Topic: "result", Messages: messages}}
			err := make(chan error)
			if e := msgClient.Subscribe(topics, err); e != nil {
				//log.Errorf("Failed to subscribe to edgex messagebus topic %s.\n", e)
				conf.Log.Fatal(e)
			} else {
				var count = 0
				for {
					select {
					case e1 := <-err:
						conf.Log.Errorf("%s\n", e1)
						return
					case env := <-messages:
						count++
						fmt.Printf("%s\n", env.Payload)
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
			subEventsFromRedis(os.Args[2])
		}
	}
}
