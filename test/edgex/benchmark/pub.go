//go:build benchmark
// +build benchmark

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

// Not necessary to build the file, until for the edgex benchmark test
package main

import (
	"context"
	"encoding/json"
	"fmt"
	v3 "github.com/edgexfoundry/go-mod-core-contracts/v3/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v3/dtos"
	"github.com/edgexfoundry/go-mod-messaging/v3/messaging"
	"github.com/edgexfoundry/go-mod-messaging/v3/pkg/types"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

var msgConfig1 = types.MessageBusConfig{
	Broker: types.HostInfo{
		Host:     "172.31.1.144",
		Port:     6379,
		Protocol: "redis",
	},
	Type: messaging.Redis,
}

type data struct {
	temperature int
	humidity    int
}

var mockup = []data{
	{temperature: 10, humidity: 15},
	{temperature: 15, humidity: 20},
	{temperature: 20, humidity: 25},
	{temperature: 25, humidity: 30},
	{temperature: 30, humidity: 35},
	{temperature: 35, humidity: 40},
	{temperature: 40, humidity: 45},
	{temperature: 45, humidity: 50},
	{temperature: 50, humidity: 55},
	{temperature: 55, humidity: 60},
}

func pubEventClientRedis(count int, wg *sync.WaitGroup) {
	defer wg.Done()
	if msgClient, err := messaging.NewMessageClient(msgConfig1); err != nil {
		log.Fatal(err)
	} else {
		if ec := msgClient.Connect(); ec != nil {
			log.Fatal(ec)
		} else {
			index := 0
			for i := 0; i < count; i++ {
				if i%10 == 0 {
					index = 0
				}

				testEvent := dtos.NewEvent("demoProfile", "demo", "demoSource")
				err := testEvent.AddSimpleReading("Temperature", v3.ValueTypeInt32, int32(mockup[index].temperature))
				if err != nil {
					fmt.Errorf("Add reading error for Temperature: %v\n", int32(mockup[index].temperature))
				}
				testEvent.Readings[0].DeviceName = "Temperature device"

				err = testEvent.AddSimpleReading("Humidity", v3.ValueTypeInt32, int32(mockup[index].humidity))
				if err != nil {
					fmt.Errorf("Add reading error for Humidity: %v\n", int32(mockup[index].temperature))
				}
				testEvent.Readings[1].DeviceName = "Humidity device"
				index++

				data, err := json.Marshal(testEvent)
				if err != nil {
					fmt.Errorf("unexpected error MarshalEvent %v", err)
				}

				env := types.NewMessageEnvelope([]byte(data), context.Background())
				env.ContentType = "application/json"

				if e := msgClient.Publish(env, "events"); e != nil {
					log.Fatal(e)
				} else {
					//fmt.Printf("%d - %s\n", index, string(data))
				}
				time.Sleep(100 * time.Nanosecond)
			}
		}
	}
}

func main() {
	start := time.Now()
	count := 1000
	if len(os.Args) == 2 {
		v := os.Args[1]
		if c, err := strconv.Atoi(v); err != nil {
			fmt.Errorf("%s\n", err)
		} else {
			count = c
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < 1; i++ {
		wg.Add(1)
		go pubEventClientRedis(count, &wg)
	}
	wg.Wait()
	t := time.Now()
	elapsed := t.Sub(start)

	fmt.Printf("elapsed %2fs\n", elapsed.Seconds())
}
