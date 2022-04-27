// Copyright 2022 EMQ Technologies Co., Ltd.
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
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

const (
	count    = 300                      // number of rules to create
	interval = 10                       // interval between requests
	url      = "http://127.0.0.1:9081/" // eKuiper url to send rule creation requests to
	mqttUrl  = "tcp://127.0.0.1:1883"   // mqtt broker url
)

type rule struct {
	Id      string                   `json:"id"`
	Sql     string                   `json:"sql"`
	Actions []map[string]interface{} `json:"actions"`
	Options map[string]interface{}   `json:"options"`
}

func create() {
	fmt.Println("create stream")
	createStream()
	fmt.Println("create rules")
	createRules()
}

func createStream() {
	s := `{"sql":"CREATE STREAM rawdata() WITH (DATASOURCE=\"rawdata\", SHARED=\"TRUE\");"}`
	resp, err := http.Post(url+"streams", "application/json", bytes.NewReader([]byte(s)))
	if err != nil {
		fmt.Println(err)
	}
	if resp.StatusCode != http.StatusCreated {
		fmt.Printf("%v\n", resp)
	}
}

func createRules() {
	i := 0
	for ; i <= count; i++ {
		r := &rule{
			Id:  fmt.Sprintf("rule%d", i),
			Sql: "SELECT temperature FROM rawdata WHERE temperature > 20",
			Actions: []map[string]interface{}{
				{
					"nop": map[string]interface{}{},
				},
			},
			Options: map[string]interface{}{},
		}
		if i%10 == 0 { // Send 1/10 requests to mqtt broker
			r.Actions = []map[string]interface{}{
				{
					"mqtt": map[string]interface{}{
						"server": mqttUrl,
						"topic":  "demoSink",
					},
				},
			}
		}
		s, err := json.Marshal(r)
		if err != nil {
			fmt.Println(err)
			break
		}
		resp, err := http.Post(url+"rules", "application/json", bytes.NewReader(s))
		if err != nil {
			fmt.Println(err)
			break
		}
		if resp.StatusCode != http.StatusCreated {
			fmt.Printf("%v\n", resp)
			break
		}
		time.Sleep(time.Duration(interval) * time.Millisecond)
	}
	fmt.Printf("Run %d\n", i-1)
}
