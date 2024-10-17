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

package fvt

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/lf-edge/ekuiper/v2/cmd"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

const (
	URL              = "http://127.0.0.1:9081"
	DataPath         = "fvt/data"
	ResultPath       = "fvt/result"
	RulesPath        = "fvt/rules"
	MQTTBroker       = "tcp://127.0.0.1:1883"
	ConstantInterval = 100 * time.Millisecond
)

var (
	PWD     string
	client  *SDK
	started bool
)

func init() {
	// Get pwd
	dir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	dir = filepath.Join(dir, "..")
	fmt.Println("Current PWD:", dir)
	PWD = dir
	client, err = NewSdk(URL)
	if err != nil {
		log.Fatal(err)
	}
	// Create data/log dir
	err = os.MkdirAll(filepath.Join(PWD, "data"), 0o750)
	if err != nil {
		fmt.Println(err)
	}
	err = os.MkdirAll(filepath.Join(PWD, "log"), 0o750)
	if err != nil {
		fmt.Println(err)
	}
	conf.IsTesting = false
	timex.IsTesting = false
	timex.InitClock()
	// Start eKuiper
	cmd.Version = "fvt"
	go cmd.Main()
	count := 10
	for count > 0 {
		time.Sleep(ConstantInterval)
		resp, err := client.Get("ping")
		if err == nil && resp.StatusCode == http.StatusOK {
			fmt.Println("service ready")
			break
		}
		count--
	}
	if count == 0 {
		fmt.Println("service not ready after 10 tries")
	}
}
