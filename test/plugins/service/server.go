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

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"time"
)

func alert(w http.ResponseWriter, req *http.Request) {
	buf, bodyErr := io.ReadAll(req.Body)
	if bodyErr != nil {
		log.Print("bodyErr ", bodyErr.Error())
		http.Error(w, bodyErr.Error(), http.StatusInternalServerError)
		return
	}

	rdr1 := io.NopCloser(bytes.NewBuffer(buf))
	log.Printf("BODY: %q", rdr1)
}

var count = 0

type Sensor struct {
	Temperature int `json: "temperature""`
	Humidity    int `json: "humidiy"`
}

var s = &Sensor{}

func pullSrv(w http.ResponseWriter, req *http.Request) {
	buf, bodyErr := io.ReadAll(req.Body)
	if bodyErr != nil {
		log.Print("bodyErr ", bodyErr.Error())
		http.Error(w, bodyErr.Error(), http.StatusInternalServerError)
		return
	} else {
		fmt.Println(string(buf))
	}

	if count%2 == 0 {
		rand.Seed(time.Now().UnixNano())
		s.Temperature = rand.Intn(100)
		s.Humidity = rand.Intn(100)
	}
	fmt.Printf("%v\n", s)
	count++
	sd, err := json.Marshal(s)
	if err != nil {
		fmt.Println(err)
		return
	} else {
		if _, e := fmt.Fprintf(w, "%s", sd); e != nil {
			fmt.Println(e)
		}
	}
}

func main() {
	http.Handle("/", http.FileServer(http.Dir("web")))
	http.HandleFunc("/alert", alert)
	http.HandleFunc("/pull", pullSrv)

	http.ListenAndServe(":9090", nil)
}
