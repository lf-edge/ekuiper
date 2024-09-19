// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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
	_ "github.com/influxdata/influxdb-client-go/v2"
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/extensions/impl/influx2"
)

func Influx2() api.Sink {
	return influx2.GetSink()
}

//// This is for manual test
//func main() {
//	i := Influx2()
//	err := i.Configure(map[string]interface{}{
//		"addr":        "http://127.0.0.1:8086",
//		"token":       "q1w2e3r4",
//		"measurement": "m1",
//		"org":         "test",
//		"bucket":      "test",
//		"tags": map[string]interface{}{
//			"tag": "value",
//		},
//	})
//	if err != nil {
//		panic(err)
//	}
//	contextLogger := conf.Log.WithField("rule", "rule2")
//	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
//	err = i.Open(ctx)
//	if err != nil {
//		panic(err)
//	}
//	err = i.Collect(ctx, map[string]interface{}{"temperature": 30})
//	if err != nil {
//		panic(err)
//	}
//	err = i.Close(ctx)
//	if err != nil {
//		panic(err)
//	}
//}
