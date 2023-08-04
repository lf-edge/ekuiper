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
	"testing"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
)

// Full lifecycle test: Run window rule; trigger checkpoints by mock timer; restart rule; make sure the result is right;
func TestCheckpoint(t *testing.T) {
	conf.IsTesting = true
	streamList := []string{"demo"}
	HandleStream(false, streamList, t)
	tests := []RuleCheckpointTest{
		{
			RuleTest: RuleTest{
				Name: `TestCheckpointRule1`,
				Sql:  `SELECT * FROM demo GROUP BY HOPPINGWINDOW(ss, 2, 1)`,
				R: [][]map[string]interface{}{
					{{
						"color": "red",
						"size":  float64(3),
						"ts":    float64(1541152486013),
					}, {
						"color": "blue",
						"size":  float64(6),
						"ts":    float64(1541152486822),
					}},
					{{
						"color": "red",
						"size":  float64(3),
						"ts":    float64(1541152486013),
					}, {
						"color": "blue",
						"size":  float64(6),
						"ts":    float64(1541152486822),
					}},
					{{
						"color": "blue",
						"size":  float64(2),
						"ts":    float64(1541152487632),
					}},
					{{
						"color": "blue",
						"size":  float64(2),
						"ts":    float64(1541152487632),
					}, {
						"color": "yellow",
						"size":  float64(4),
						"ts":    float64(1541152488442),
					}},
					{{
						"color": "yellow",
						"size":  float64(4),
						"ts":    float64(1541152488442),
					}, {
						"color": "red",
						"size":  float64(1),
						"ts":    float64(1541152489252),
					}},
				},
				M: map[string]interface{}{
					"op_3_project_0_records_in_total":  int64(4),
					"op_3_project_0_records_out_total": int64(4),

					"sink_mockSink_0_records_in_total":  int64(4),
					"sink_mockSink_0_records_out_total": int64(4),

					"source_demo_0_records_in_total":  int64(3),
					"source_demo_0_records_out_total": int64(3),

					"op_2_window_0_records_in_total":  int64(3),
					"op_2_window_0_records_out_total": int64(4),
				},
			},
			PauseSize: 3,
			Cc:        2,
			PauseMetric: map[string]interface{}{
				"op_3_project_0_records_in_total":  int64(1),
				"op_3_project_0_records_out_total": int64(1),

				"sink_mockSink_0_records_in_total":  int64(1),
				"sink_mockSink_0_records_out_total": int64(1),

				"source_demo_0_records_in_total":  int64(3),
				"source_demo_0_records_out_total": int64(3),

				"op_2_window_0_records_in_total":  int64(3),
				"op_2_window_0_records_out_total": int64(1),
			},
		},
	}
	HandleStream(true, streamList, t)
	options := []*api.RuleOption{
		{
			BufferLength:       100,
			Qos:                api.AtLeastOnce,
			CheckpointInterval: 600,
			SendError:          true,
		}, {
			BufferLength:       100,
			Qos:                api.ExactlyOnce,
			CheckpointInterval: 600,
			SendError:          true,
		},
	}
	for j, opt := range options {
		DoCheckpointRuleTest(t, tests, j, opt)
	}
}

func TestTableJoinCheckpoint(t *testing.T) {
	conf.IsTesting = true
	streamList := []string{"demo", "table1"}
	HandleStream(false, streamList, t)
	tests := []RuleCheckpointTest{
		{
			RuleTest: RuleTest{
				Name: `TestCheckpointRule2`,
				Sql:  `SELECT * FROM demo INNER JOIN table1 on demo.ts = table1.id`,
				R: [][]map[string]interface{}{
					{{
						"id":    float64(1541152486013),
						"name":  "name1",
						"color": "red",
						"size":  float64(3),
						"ts":    float64(1541152486013),
					}},
					{{
						"id":    float64(1541152487632),
						"name":  "name2",
						"color": "blue",
						"size":  float64(2),
						"ts":    float64(1541152487632),
					}},
					{{
						"id":    float64(1541152487632),
						"name":  "name2",
						"color": "blue",
						"size":  float64(2),
						"ts":    float64(1541152487632),
					}},
					{{
						"id":    float64(1541152489252),
						"name":  "name3",
						"color": "red",
						"size":  float64(1),
						"ts":    float64(1541152489252),
					}},
				},
				M: map[string]interface{}{
					"op_3_join_aligner_0_records_in_total":  int64(4),
					"op_3_join_aligner_0_records_out_total": int64(3),

					"op_4_join_0_exceptions_total":  int64(0),
					"op_4_join_0_records_in_total":  int64(3),
					"op_4_join_0_records_out_total": int64(2),

					"op_5_project_0_exceptions_total":  int64(0),
					"op_5_project_0_records_in_total":  int64(2),
					"op_5_project_0_records_out_total": int64(2),

					"sink_mockSink_0_exceptions_total":  int64(0),
					"sink_mockSink_0_records_in_total":  int64(2),
					"sink_mockSink_0_records_out_total": int64(2),

					"source_demo_0_exceptions_total":  int64(0),
					"source_demo_0_records_in_total":  int64(3),
					"source_demo_0_records_out_total": int64(3),

					"source_table1_0_exceptions_total":  int64(0),
					"source_table1_0_records_in_total":  int64(4),
					"source_table1_0_records_out_total": int64(1),
				},
			},
			PauseSize: 3,
			Cc:        2,
			PauseMetric: map[string]interface{}{
				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(2),
				"sink_mockSink_0_records_out_total": int64(2),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(3),
				"source_demo_0_records_out_total": int64(3),

				"source_table1_0_exceptions_total":  int64(0),
				"source_table1_0_records_in_total":  int64(4),
				"source_table1_0_records_out_total": int64(1),
			},
		},
	}
	HandleStream(true, streamList, t)
	options := []*api.RuleOption{
		{
			BufferLength:       100,
			Qos:                api.AtLeastOnce,
			CheckpointInterval: 600,
			SendError:          true,
		},
	}
	for j, opt := range options {
		DoCheckpointRuleTest(t, tests, j, opt)
	}
}
