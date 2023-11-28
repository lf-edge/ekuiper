// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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
	_ "github.com/influxdata/influxdb1-client/v2"

	influx "github.com/lf-edge/ekuiper/extensions/sinks/influx/ext"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/pkg/api"
)

func Influx() api.Sink {
	return influx.GetSink()
}

// This is for manual test
func main() {
	i := Influx()
	err := i.Configure(map[string]interface{}{
		"addr":        "http://127.0.0.1:8086",
		"measurement": "test",
		"database":    "mydb",
		"tags": map[string]interface{}{
			"tag": "{{.humidity}}",
		},
	})
	if err != nil {
		panic(err)
	}
	contextLogger := conf.Log.WithField("rule", "ruleInflux")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	err = i.Open(ctx)
	if err != nil {
		panic(err)
	}
	err = i.Collect(ctx, map[string]interface{}{"temperature": 30, "humidity": 80})
	if err != nil {
		panic(err)
	}
	err = i.Close(ctx)
	if err != nil {
		panic(err)
	}
}
