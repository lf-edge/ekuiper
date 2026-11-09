// Copyright 2023 EMQ Technologies Co., Ltd.
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

//go:build msgpack

package service

import (
	"net"
	"reflect"
	"testing"

	"github.com/msgpack-rpc/msgpack-rpc-go/rpc"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/internal/topo/topotest"
)

func TestMsgpackService(t *testing.T) {
	if testx.Race {
		t.Skip("skip msgpack service test in race mode")
	}
	// mock server
	res := Resolver{"SayHello": reflect.ValueOf(SayHello), "object_detection": reflect.ValueOf(object_detection), "get_feature": reflect.ValueOf(get_feature), "getStatus": reflect.ValueOf(getStatus)}
	serv := rpc.NewServer(res, true, nil)
	l, _ := net.Listen("tcp", ":50000")
	serv.Listen(l)
	go serv.Run()
	// Comment out because the bug in the msgpack rpc
	// defer serv.Stop()

	// Reset
	streamList := []string{"helloStr", "commands", "fakeBin"}
	topotest.HandleStream(false, streamList, t)
	// Data setup
	tests := []topotest.RuleTest{
		{
			Name: `TestRestRule1`,
			Sql:  `SELECT helloFromMsgpack(name) as wc FROM helloStr`,
			R: [][]map[string]interface{}{
				{{
					"wc": map[string]interface{}{
						"message": "world",
					},
				}},
				{{
					"wc": map[string]interface{}{
						"message": "golang",
					},
				}},
				{{
					"wc": map[string]interface{}{
						"message": "peacock",
					},
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(3),
				"op_2_project_0_records_out_total":  int64(3),

				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(3),
				"sink_memory_0_0_records_out_total": int64(3),
			},
		}, {
			Name: `TestRestRule2`,
			Sql:  `SELECT objectDetectFromMsgpack(*)->result FROM commands`,
			R: [][]map[string]interface{}{
				{{
					"kuiper_field_0": "get success",
				}},
				{{
					"kuiper_field_0": "detect success",
				}},
				{{
					"kuiper_field_0": "delete success",
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(3),
				"op_2_project_0_records_out_total":  int64(3),

				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(3),
				"sink_memory_0_0_records_out_total": int64(3),
			},
		}, {
			Name: `TestRestRule3`,
			Sql:  `SELECT getFeatureFromMsgpack(self)->feature[0]->box->h FROM fakeBin`,
			R: [][]map[string]interface{}{
				{{
					"kuiper_field_0": int64(106), // Convert by the testing tool
				}},
				{{
					"kuiper_field_0": int64(107),
				}},
				{{
					"kuiper_field_0": int64(108),
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(3),
				"op_2_project_0_records_out_total":  int64(3),

				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(3),
				"sink_memory_0_0_records_out_total": int64(3),
			},
			//}, {
			//	Name: `TestRestRule4`,
			//	Sql:  `SELECT getStatusFromMsgpack(), command FROM commands`,
			//	R: [][]map[string]interface{}{
			//		{{
			//			"getStatusFromRest": true,
			//			"command": "get",
			//		}},
			//		{{
			//			"getStatusFromRest": true,
			//			"command": "detect",
			//		}},
			//		{{
			//			"getStatusFromRest": true,
			//			"command": "delete",
			//		}},
			//	},
			//	M: map[string]interface{}{
			//		"op_2_project_0_exceptions_total":   int64(0),
			//		"op_2_project_0_process_latency_us": int64(0),
			//		"op_2_project_0_records_in_total":   int64(3),
			//		"op_2_project_0_records_out_total":  int64(3),
			//
			//		"sink_memory_0_0_exceptions_total":  int64(0),
			//		"sink_memory_0_0_records_in_total":  int64(3),
			//		"sink_memory_0_0_records_out_total": int64(3),
			//	},
		},
	}
	topotest.HandleStream(true, streamList, t)
	topotest.DoRuleTest(t, tests, &def.RuleOption{
		BufferLength: 100,
		SendError:    true,
	}, 0)
}
