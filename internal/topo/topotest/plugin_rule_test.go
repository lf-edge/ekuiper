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

package topotest

import (
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/v2/internal/binder"
	"github.com/lf-edge/ekuiper/v2/internal/binder/function"
	"github.com/lf-edge/ekuiper/v2/internal/binder/io"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/io/memory/pubsub"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/plugin/native"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

// This cannot be run in Windows. And the plugins must be built to so before running this
// For Windows, run it in wsl with go test -trimpath -tags test internal/topo/topotest/plugin_rule_test.go internal/topo/topotest/mock_topo.go

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

// Test for source, func and agg func extensions. Sink plugin is tested in fvt
// The .so files must be in the plugins folder
func TestExtensions(t *testing.T) {
	// Reset
	streamList := []string{"ext", "ext2"}
	HandleStream(false, streamList, t)
	tests := []RuleTest{
		{
			Name: "TestExtensionsRule1",
			Sql:  `SELECT count(echo(count)) as c, echo(count) as e, countPlusOne(count) as p FROM ext where count > 49`,
			R: [][]map[string]interface{}{
				{
					{
						"c": 1, "e": int64(50), "p": 2,
					},
				},
			},
		},
		{
			Name: "TestExtensionsRule2",
			Sql:  `SELECT count(echo(count)) as c, echo(count) as e, countPlusOne(count) as p FROM ext2 where count > 49`,
			R: [][]map[string]interface{}{
				{
					{
						"c": 1, "e": 50, "p": 2,
					},
				},
			},
		},
	}
	HandleStream(true, streamList, t)
	options := []*def.RuleOption{
		{
			BufferLength: 100,
			SendError:    true,
		},
	}
	for _, opt := range options {
		// customized result func to compare only first result
		DoRuleTestWithResultFunc(t, tests, opt, 0, func(result []any) [][]map[string]any {
			maps := make([][]map[string]any, 0, len(result))
			for _, v := range result {
				switch rt := v.(type) {
				case pubsub.MemTuple:
					m := rt.ToMap()
					maps = append(maps, []map[string]any{m})
					break
				case []pubsub.MemTuple:
					nm := make([]map[string]any, 0, len(rt))
					for _, mm := range rt {
						nm = append(nm, mm.ToMap())
						break
					}
					maps = append(maps, nm)
					break
				default:
					conf.Log.Errorf("receive wrong tuple %v", rt)
				}
			}
			return maps[:1]
		})
	}
}

func TestFuncState(t *testing.T) {
	// Reset
	streamList := []string{"text"}
	HandleStream(false, streamList, t)
	// Data setup
	tests := []RuleTest{
		{
			Name: `TestFuncStateRule1`,
			Sql:  `SELECT accumulateWordCount(slogan, " ") as wc FROM text`,
			R: [][]map[string]interface{}{
				{{
					"wc": 3,
				}},
				{{
					"wc": 6,
				}},
				{{
					"wc": 8,
				}},
				{{
					"wc": 16,
				}},
				{{
					"wc": 20,
				}},
				{{
					"wc": 25,
				}},
				{{
					"wc": 27,
				}},
				{{
					"wc": 30,
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(8),
				"op_2_project_0_records_out_total":  int64(8),

				"source_text_0_exceptions_total":  int64(0),
				"source_text_0_records_in_total":  int64(8),
				"source_text_0_records_out_total": int64(8),
			},
		},
	}
	HandleStream(true, streamList, t)
	DoRuleTest(t, tests, &def.RuleOption{
		BufferLength: 100,
		SendError:    true,
	}, 0)
}

func TestFuncStateCheckpoint(t *testing.T) {
	streamList := []string{"text"}
	HandleStream(false, streamList, t)
	tests := []RuleCheckpointTest{
		{
			RuleTest: RuleTest{
				Name: `TestFuncStateCheckpointRule1`,
				Sql:  `SELECT accumulateWordCount(slogan, " ") as wc FROM text`,
				R: [][]map[string]interface{}{
					{{
						"wc": 8,
					}},
					{{
						"wc": 16,
					}},
					{{
						"wc": 20,
					}},
					{{
						"wc": 25,
					}},
					{{
						"wc": 27,
					}},
					{{
						"wc": 30,
					}},
				},
				M: map[string]interface{}{
					"op_2_project_0_process_latency_us": int64(0),
					"op_2_project_0_records_in_total":   int64(6),
					"op_2_project_0_records_out_total":  int64(6),

					"source_text_0_exceptions_total":  int64(0),
					"source_text_0_records_in_total":  int64(6),
					"source_text_0_records_out_total": int64(6),
				},
			},
			PauseSize: 3,
			Cc:        1,
		},
	}
	HandleStream(true, streamList, t)
	DoCheckpointRuleTest(t, tests, &def.RuleOption{
		BufferLength:       100,
		Qos:                def.AtLeastOnce,
		CheckpointInterval: cast.DurationConf(2 * time.Second),
		SendError:          true,
	}, 0)
}
