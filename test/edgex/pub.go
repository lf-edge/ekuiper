// Copyright 2021 EMQ Technologies Co., Ltd.
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

//go:build edgex
// +build edgex

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/edgexfoundry/go-mod-core-contracts/v2/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v2/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v2/dtos/requests"
	"github.com/edgexfoundry/go-mod-messaging/v2/messaging"
	"github.com/edgexfoundry/go-mod-messaging/v2/pkg/types"
	"log"
	"os"
	"time"
)

var msgConfig1 = types.MessageBusConfig{
	PublishHost: types.HostInfo{
		Host:     "*",
		Port:     5563,
		Protocol: "tcp",
	},
	Type: messaging.ZeroMQ,
}

func pubEventClientZeroMq() {
	if msgClient, err := messaging.NewMessageClient(msgConfig1); err != nil {
		log.Fatal(err)
	} else {
		if ec := msgClient.Connect(); ec != nil {
			log.Fatal(ec)
		} else {
			//r := rand.New(rand.NewSource(time.Now().UnixNano()))
			for i := 0; i < 10; i++ {
				//temp := r.Intn(100)
				//humd := r.Intn(100)

				var testEvent = dtos.NewEvent("demoProfile", "demo", "demoSource")
				testEvent.Origin = 123
				err := testEvent.AddSimpleReading("Temperature", common.ValueTypeInt64, int64(i*8))
				if err != nil {
					fmt.Printf("Add reading error for %d.Temperature: %v\n", i, i*8)
				}
				err = testEvent.AddSimpleReading("Humidity", common.ValueTypeInt64, int64(i*9))
				if err != nil {
					fmt.Printf("Add reading error for %d.Humidity: %v\n", i, i*9)
				}
				err = testEvent.AddSimpleReading("b1", common.ValueTypeBool, i%2 == 0)
				if err != nil {
					fmt.Printf("Add reading error for %d.b1: %v\n", i, i%2 == 0)
				}
				err = testEvent.AddSimpleReading("i1", common.ValueTypeInt64, int64(i))
				if err != nil {
					fmt.Printf("Add reading error for %d.i1: %v\n", i, i)
				}
				err = testEvent.AddSimpleReading("f1", common.ValueTypeFloat64, float64(i)/2.0)
				if err != nil {
					fmt.Printf("Add reading error for %d.f1: %v\n", i, float64(i)/2.0)
				}
				err = testEvent.AddSimpleReading("ui64", common.ValueTypeUint64, uint64(10796529505058023104))
				if err != nil {
					fmt.Printf("Add reading error for %d.ui64: %v\n", i, uint64(10796529505058023104))
				}

				fmt.Printf("readings: %v\n", testEvent.Readings)
				data, err := json.Marshal(testEvent)
				if err != nil {
					fmt.Printf("unexpected error MarshalEvent %v", err)
				} else {
					fmt.Println(string(data))
				}

				env := types.NewMessageEnvelope(data, context.Background())
				env.ContentType = "application/json"

				if e := msgClient.Publish(env, "events"); e != nil {
					log.Fatal(e)
				} else {
					fmt.Printf("Pub successful: %s\n", data)
				}
				time.Sleep(1500 * time.Millisecond)
			}
		}
	}
}

func pubToAnother() {
	var msgConfig2 = types.MessageBusConfig{
		PublishHost: types.HostInfo{
			Host:     "*",
			Port:     5571,
			Protocol: "tcp",
		},
		Type: messaging.ZeroMQ,
	}
	if msgClient, err := messaging.NewMessageClient(msgConfig2); err != nil {
		log.Fatal(err)
	} else {
		if ec := msgClient.Connect(); ec != nil {
			log.Fatal(ec)
		}

		testEvent := dtos.NewEvent("demo1Profile", "demo1", "demo1Source")
		testEvent.Origin = 123
		err := testEvent.AddSimpleReading("Temperature", common.ValueTypeInt64, int64(20))
		if err != nil {
			fmt.Printf("Add reading error for Temperature: %v\n", 20)
		}
		err = testEvent.AddSimpleReading("Humidity", common.ValueTypeInt64, int64(30))
		if err != nil {
			fmt.Printf("Add reading error for Humidity: %v\n", 20)
		}

		req := requests.NewAddEventRequest(testEvent)

		data, err := json.Marshal(req)
		if err != nil {
			fmt.Printf("unexpected error marshal request %v", err)
		} else {
			fmt.Println(string(data))
		}

		env := types.NewMessageEnvelope(data, context.Background())
		env.ContentType = "application/json"

		if e := msgClient.Publish(env, "application"); e != nil {
			log.Fatal(e)
		} else {
			fmt.Printf("pubToAnother successful: %s\n", data)
		}
		time.Sleep(1500 * time.Millisecond)
	}
}

func pubArrayMessage() {
	var msgConfig2 = types.MessageBusConfig{
		PublishHost: types.HostInfo{
			Host:     "*",
			Port:     5563,
			Protocol: "tcp",
		},
		Type: messaging.ZeroMQ,
	}
	if msgClient, err := messaging.NewMessageClient(msgConfig2); err != nil {
		log.Fatal(err)
	} else {
		if ec := msgClient.Connect(); ec != nil {
			log.Fatal(ec)
		}
		testEvent := dtos.NewEvent("demo1Profile", "demo1", "demo1Source")
		testEvent.Origin = 123
		err := testEvent.AddSimpleReading("ba", common.ValueTypeBoolArray, []bool{true, true, false})
		if err != nil {
			fmt.Printf("Add reading error for ba: %v\n", []bool{true, true, false})
		}
		err = testEvent.AddSimpleReading("ia", common.ValueTypeInt32Array, []int32{30, 40, 50})
		if err != nil {
			fmt.Printf("Add reading error for ia: %v\n", []int32{30, 40, 50})
		}
		err = testEvent.AddSimpleReading("fa", common.ValueTypeFloat64Array, []float64{3.14, 3.1415, 3.1415926})
		if err != nil {
			fmt.Printf("Add reading error for fa: %v\n", []float64{3.14, 3.1415, 3.1415926})
		}
		testEvent.Readings[len(testEvent.Readings)-1].Value = "[3.14, 3.1415, 3.1415926]"

		data, err := json.Marshal(testEvent)
		if err != nil {
			fmt.Printf("unexpected error MarshalEvent %v", err)
		} else {
			fmt.Println(string(data))
		}

		env := types.NewMessageEnvelope(data, context.Background())
		env.ContentType = "application/json"

		if e := msgClient.Publish(env, "events"); e != nil {
			log.Fatal(e)
		}
		time.Sleep(1500 * time.Millisecond)
	}
}

func pubToMQTT(host string) {
	var msgConfig2 = types.MessageBusConfig{
		PublishHost: types.HostInfo{
			Host:     host,
			Port:     1883,
			Protocol: "tcp",
		},
		Optional: map[string]string{
			"ClientId": "0001_client_id",
		},
		Type: messaging.MQTT,
	}
	if msgClient, err := messaging.NewMessageClient(msgConfig2); err != nil {
		log.Fatal(err)
	} else {
		if ec := msgClient.Connect(); ec != nil {
			log.Fatal(ec)
		}
		testEvent := dtos.NewEvent("demo1Profile", "demo1", "demo1Source")
		testEvent.Origin = 123
		err := testEvent.AddSimpleReading("Temperature", common.ValueTypeInt64, int64(20))
		if err != nil {
			fmt.Printf("Add reading error for Temperature: %v\n", 20)
		}
		err = testEvent.AddSimpleReading("Humidity", common.ValueTypeInt64, int64(30))
		if err != nil {
			fmt.Printf("Add reading error for Humidity: %v\n", 20)
		}

		data, err := json.Marshal(testEvent)
		if err != nil {
			fmt.Printf("unexpected error MarshalEvent %v", err)
		} else {
			fmt.Println(string(data))
		}

		env := types.NewMessageEnvelope(data, context.Background())
		env.ContentType = "application/json"

		if e := msgClient.Publish(env, "events"); e != nil {
			log.Fatal(e)
		} else {
			fmt.Printf("pubToAnother successful: %s\n", data)
		}
		time.Sleep(1500 * time.Millisecond)
	}
}

func pubToRedis(host string) {
	var msgConfig2 = types.MessageBusConfig{
		PublishHost: types.HostInfo{
			Host:     host,
			Port:     6379,
			Protocol: "redis",
		},
		Type: messaging.Redis,
	}
	if msgClient, err := messaging.NewMessageClient(msgConfig2); err != nil {
		log.Fatal(err)
	} else {
		if ec := msgClient.Connect(); ec != nil {
			log.Fatal(ec)
		}
		testEvent := dtos.NewEvent("demo1Profile", "demo1", "demo1Source")
		testEvent.Origin = 123
		err := testEvent.AddSimpleReading("Temperature", common.ValueTypeInt64, int64(20))
		if err != nil {
			fmt.Printf("Add reading error for Temperature: %v\n", 20)
		}
		err = testEvent.AddSimpleReading("Humidity", common.ValueTypeInt64, int64(30))
		if err != nil {
			fmt.Printf("Add reading error for Humidity: %v\n", 20)
		}

		data, err := json.Marshal(testEvent)
		if err != nil {
			fmt.Printf("unexpected error MarshalEvent %v", err)
		} else {
			fmt.Println(string(data))
		}

		env := types.NewMessageEnvelope(data, context.Background())
		env.ContentType = "application/json"

		if e := msgClient.Publish(env, "events"); e != nil {
			log.Fatal(e)
		} else {
			fmt.Printf("pubToRedis successful: %s\n", data)
		}
		time.Sleep(1500 * time.Millisecond)
	}
}

func pubMetaSource() {
	if msgClient, err := messaging.NewMessageClient(msgConfig1); err != nil {
		log.Fatal(err)
	} else {
		if ec := msgClient.Connect(); ec != nil {
			log.Fatal(ec)
		} else {
			evtDevice := []string{"demo1", "demo2"}
			for i, device := range evtDevice {
				j := int64(i) + 1
				testEvent := dtos.NewEvent("demo1Profile", device, "demo1Source")
				testEvent.Origin = 13 * j
				err := testEvent.AddSimpleReading("Temperature", common.ValueTypeInt64, j*8)
				if err != nil {
					fmt.Printf("Add reading error for %d.Temperature: %v\n", i, j*8)
				}
				testEvent.Readings[0].Origin = 24 * j
				testEvent.Readings[0].DeviceName = "Temperature sensor"
				err = testEvent.AddSimpleReading("Humidity", common.ValueTypeInt64, j*8)
				if err != nil {
					fmt.Printf("Add reading error for %d.Humidity: %v\n", i, j*8)
				}
				testEvent.Readings[1].Origin = 34 * j
				testEvent.Readings[1].DeviceName = "Humidity sensor"

				testEvent.AddBinaryReading("raw", []byte("Hello World"), "application/text")

				data, err := json.Marshal(testEvent)
				if err != nil {
					fmt.Printf("unexpected error MarshalEvent %v", err)
				} else {
					fmt.Println(string(data))
				}

				env := types.NewMessageEnvelope([]byte(data), context.Background())
				env.ContentType = "application/json"

				if e := msgClient.Publish(env, "events"); e != nil {
					log.Fatal(e)
				} else {
					fmt.Printf("Pub successful: %s\n", data)
				}
				time.Sleep(1500 * time.Millisecond)
			}

		}
	}
}

func main() {
	if len(os.Args) == 1 {
		pubEventClientZeroMq()
	} else if len(os.Args) == 2 {
		if v := os.Args[1]; v == "another" {
			pubToAnother()
		} else if v == "meta" {
			pubMetaSource()
		} else if v == "array" {
			pubArrayMessage()
		}
	} else if len(os.Args) == 3 {
		if v := os.Args[1]; v == "mqtt" {
			//The 2nd parameter is MQTT broker server address
			pubToMQTT(os.Args[2])
		}
		if v := os.Args[1]; v == "redis" {
			//The 2nd parameter is MQTT broker server address
			pubToRedis(os.Args[2])
		}
	}
}
