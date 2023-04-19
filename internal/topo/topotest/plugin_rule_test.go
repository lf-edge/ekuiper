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

package topotest

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/binder"
	"github.com/lf-edge/ekuiper/internal/binder/function"
	"github.com/lf-edge/ekuiper/internal/binder/io"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/plugin/native"
	"github.com/lf-edge/ekuiper/internal/topo/planner"
	"github.com/lf-edge/ekuiper/internal/topo/topotest/mockclock"
	"github.com/lf-edge/ekuiper/pkg/api"
	"os"
	"testing"
	"time"
)

func init() {
	nativeManager, err := native.InitManager()
	if err != nil {
		panic(err)
	}
	nativeEntry := binder.FactoryEntry{Name: "native plugin", Factory: nativeManager}
	err = function.Initialize([]binder.FactoryEntry{nativeEntry})
	if err != nil {
		panic(err)
	}
	err = io.Initialize([]binder.FactoryEntry{nativeEntry})
	if err != nil {
		panic(err)
	}
}

// This cannot be run in Windows. And the plugins must be built to so before running this
// For Windows, run it in wsl with go test -tags test internal/topo/topotest/plugin_rule_test.go internal/topo/topotest/mock_topo.go
var CACHE_FILE = "cache"

// Test for source, sink, func and agg func extensions
// The .so files must be in the plugins folder
func TestExtensions(t *testing.T) {
	log := conf.Log
	//Reset
	streamList := []string{"ext", "ext2"}
	HandleStream(false, streamList, t)
	os.Remove(CACHE_FILE)
	os.Create(CACHE_FILE)
	var tests = []struct {
		name      string
		rj        string
		minLength int
		maxLength int
	}{
		{
			name:      `TestExtensionsRule1`,
			rj:        "{\"sql\": \"SELECT count(echo(count)) as c, echo(count) as e, countPlusOne(count) as p FROM ext where count > 49\",\"actions\": [{\"file\":  {\"path\":\"" + CACHE_FILE + "\"}}]}",
			minLength: 5,
		}, {
			name:      `TestExtensionsRule2`,
			rj:        "{\"sql\": \"SELECT count(echo(count)) as c, echo(count) as e, countPlusOne(count) as p FROM ext2\",\"actions\": [{\"file\":  {\"path\":\"" + CACHE_FILE + "\"}}]}",
			maxLength: 2,
		},
	}
	HandleStream(true, streamList, t)
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		mockclock.ResetClock(1541152486000)
		// Create rule
		rs, err := CreateRule(tt.name, tt.rj)
		if err != nil {
			t.Errorf("failed to create rule: %s.", err)
			continue
		}
		tp, err := planner.Plan(rs)
		if err != nil {
			t.Errorf("fail to init rule: %v", err)
			continue
		}
		go func() {
			select {
			case err := <-tp.Open():
				log.Println(err)
				tp.Cancel()
			case <-time.After(900 * time.Millisecond):
				tp.Cancel()
			}
		}()
		time.Sleep(1000 * time.Millisecond)
		log.Printf("exit main program after a second")
		results := getResults()
		log.Infof("get results %v", results)
		os.Remove(CACHE_FILE)
		var maps [][]map[string]interface{}
		for _, v := range results {
			var mapRes []map[string]interface{}
			err := json.Unmarshal([]byte(v), &mapRes)
			if err != nil {
				t.Errorf("Failed to parse the input into map")
				continue
			}
			maps = append(maps, mapRes)
		}

		if tt.minLength > 0 {
			if len(maps) < tt.minLength {
				t.Errorf("%d. %q\n\nresult length is smaller than minlength:\n\ngot=%#v\n\n", i, tt.rj, maps)
				break
			}
		}

		if tt.maxLength > 0 {
			if len(maps) > tt.maxLength {
				t.Errorf("%d. %q\n\nresult length is bigger than maxLength:\n\ngot=%#v\n\n", i, tt.rj, maps)
				break
			}
		}

		for _, r := range maps {
			if len(r) != 1 {
				t.Errorf("%d. %q\n\nresult mismatch:\n\ngot=%#v\n\n", i, tt.rj, maps)
				break
			}
			r := r[0]
			c := int((r["c"]).(float64))
			if c != 1 {
				t.Errorf("%d. %q\n\nresult mismatch:\n\ngot=%#v\n\n", i, tt.rj, maps)
				break
			}
			e := int((r["e"]).(float64))
			if e != 50 && e != 51 {
				t.Errorf("%d. %q\n\nresult mismatch:\n\ngot=%#v\n\n", i, tt.rj, maps)
				break
			}
			p := int(r["p"].(float64))
			if p != 2 {
				t.Errorf("%d. %q\n\nresult mismatch:\n\ngot=%#v\n\n", i, tt.rj, maps)
				break
			}
		}
	}
}

func getResults() []string {
	f, err := os.Open(CACHE_FILE)
	if err != nil {
		panic(err)
	}
	result := make([]string, 0)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		result = append(result, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
	f.Close()
	return result
}

func TestFuncState(t *testing.T) {
	//Reset
	streamList := []string{"text"}
	HandleStream(false, streamList, t)
	//Data setup
	var tests = []RuleTest{
		{
			Name: `TestFuncStateRule1`,
			Sql:  `SELECT accumulateWordCount(slogan, " ") as wc FROM text`,
			R: [][]map[string]interface{}{
				{{
					"wc": float64(3),
				}},
				{{
					"wc": float64(6),
				}},
				{{
					"wc": float64(8),
				}},
				{{
					"wc": float64(16),
				}},
				{{
					"wc": float64(20),
				}},
				{{
					"wc": float64(25),
				}},
				{{
					"wc": float64(27),
				}},
				{{
					"wc": float64(30),
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(8),
				"op_2_project_0_records_out_total":  int64(8),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(8),
				"sink_mockSink_0_records_out_total": int64(8),

				"source_text_0_exceptions_total":  int64(0),
				"source_text_0_records_in_total":  int64(8),
				"source_text_0_records_out_total": int64(8),
			},
		},
	}
	HandleStream(true, streamList, t)
	DoRuleTest(t, tests, 0, &api.RuleOption{
		BufferLength: 100,
		SendError:    true,
	}, 0)
}

func TestFuncStateCheckpoint(t *testing.T) {
	streamList := []string{"text"}
	HandleStream(false, streamList, t)
	var tests = []RuleCheckpointTest{
		{
			RuleTest: RuleTest{
				Name: `TestFuncStateCheckpointRule1`,
				Sql:  `SELECT accumulateWordCount(slogan, " ") as wc FROM text`,
				R: [][]map[string]interface{}{
					{{
						"wc": float64(3),
					}},
					{{
						"wc": float64(6),
					}},
					{{
						"wc": float64(8),
					}},
					{{
						"wc": float64(8),
					}},
					{{
						"wc": float64(16),
					}},
					{{
						"wc": float64(20),
					}},
					{{
						"wc": float64(25),
					}},
					{{
						"wc": float64(27),
					}},
					{{
						"wc": float64(30),
					}},
				},
				M: map[string]interface{}{
					"op_2_project_0_exceptions_total":   int64(0),
					"op_2_project_0_process_latency_us": int64(0),
					"op_2_project_0_records_in_total":   int64(6),
					"op_2_project_0_records_out_total":  int64(6),

					"sink_mockSink_0_exceptions_total":  int64(0),
					"sink_mockSink_0_records_in_total":  int64(6),
					"sink_mockSink_0_records_out_total": int64(6),

					"source_text_0_exceptions_total":  int64(0),
					"source_text_0_records_in_total":  int64(6),
					"source_text_0_records_out_total": int64(6),
				},
			},
			PauseSize: 3,
			Cc:        1,
			PauseMetric: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(3),
				"op_2_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),

				"source_text_0_exceptions_total":  int64(0),
				"source_text_0_records_in_total":  int64(3),
				"source_text_0_records_out_total": int64(3),
			},
		},
	}
	HandleStream(true, streamList, t)
	DoCheckpointRuleTest(t, tests, 0, &api.RuleOption{
		BufferLength:       100,
		Qos:                api.AtLeastOnce,
		CheckpointInterval: 2000,
		SendError:          true,
	})
}
