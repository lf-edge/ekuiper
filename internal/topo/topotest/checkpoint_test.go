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

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

// Full lifecycle test: Run window rule; trigger checkpoints by mock timer; restart rule; make sure the result is right;
func TestCheckpoint(t *testing.T) {
	t.Skip()
	streamList := []string{"demo"}
	HandleStream(false, streamList, t)
	tests := []RuleCheckpointTest{
		{
			RuleTest: RuleTest{
				Name: `TestCheckpointRule1`,
				Sql:  `SELECT * FROM demo GROUP BY HOPPINGWINDOW(ss, 2, 1)`,
				R: [][]map[string]interface{}{
					//{{ // 7000
					//	"color": "red",
					//	"size":  3,
					//	"ts":    1541152486013,
					//}, {
					//	"color": "blue",
					//	"size":  6,
					//	"ts":    1541152486822,
					//}},
					{{ // restore in 8000
						"color": "red",
						"size":  3,
						"ts":    1541152486013,
					}, {
						"color": "blue",
						"size":  6,
						"ts":    1541152486822,
					}},
					{{
						"color": "blue",
						"size":  2,
						"ts":    1541152487632,
					}},
					{
						{
							"color": "blue",
							"size":  2,
							"ts":    1541152487632,
						},
						{ // 9000
							"color": "yellow",
							"size":  4,
							"ts":    1541152488442,
						},
					},
				},
				M: map[string]interface{}{
					"sink_memory_0_0_records_in_total":  int64(3),
					"sink_memory_0_0_records_out_total": int64(3),

					"source_demo_0_records_in_total":  int64(3),
					"source_demo_0_records_out_total": int64(3),

					"op_2_window_0_records_in_total":  int64(3),
					"op_2_window_0_records_out_total": int64(3),
				},
			},
			PauseSize: 3,
			Cc:        2,
		},
	}
	HandleStream(true, streamList, t)
	options := []*def.RuleOption{
		{ // Need to clean up checkpoint cache before running twice
			//	BufferLength:       100,
			//	Qos:                def.AtLeastOnce,
			//	CheckpointInterval: cast.DurationConf(600 * time.Millisecond),
			//	SendError:          true,
			//}, {
			BufferLength:       100,
			Qos:                def.ExactlyOnce,
			CheckpointInterval: cast.DurationConf(600 * time.Millisecond),
			SendError:          true,
		},
	}
	for j, opt := range options {
		DoCheckpointRuleTest(t, tests, opt, j)
	}
}

func TestTableJoinCheckpoint(t *testing.T) {
	streamList := []string{"demo", "table1"}
	HandleStream(false, streamList, t)
	tests := []RuleCheckpointTest{
		{
			RuleTest: RuleTest{
				Name: `TestCheckpointRule2`,
				Sql:  `SELECT * FROM demo INNER JOIN table1 on demo.ts = table1.id`,
				R: [][]map[string]interface{}{
					//{{
					//	"id":    float64(1541152486013),
					//	"name":  "name1",
					//	"color": "red",
					//	"size":  float64(3),
					//	"ts":    float64(1541152486013),
					//}},
					{
						{
							"id":    int64(1541152487632),
							"name":  "name2",
							"color": "blue",
							"size":  2,
							"ts":    1541152487632,
						},
						{
							"id":    int64(1541152487632),
							"name":  "name2",
							"color": "blue",
							"size":  2,
							"ts":    1541152487632,
						},
					},
					{ // One from cache, one from newly sent
						{
							"id":    int64(1541152489252),
							"name":  "name3",
							"color": "red",
							"size":  1,
							"ts":    1541152489252,
						},
						{
							"id":    int64(1541152489252),
							"name":  "name3",
							"color": "red",
							"size":  1,
							"ts":    1541152489252,
						},
					},
				},
				M: map[string]interface{}{
					"sink_memory_0_0_records_in_total":  int64(2),
					"sink_memory_0_0_records_out_total": int64(2),

					"source_demo_0_records_in_total":  int64(3),
					"source_demo_0_records_out_total": int64(3),

					"source_table1_0_records_in_total":  int64(1),
					"source_table1_0_records_out_total": int64(1),
				},
			},
			PauseSize: 3,
			Cc:        2,
		},
	}
	HandleStream(true, streamList, t)
	options := []*def.RuleOption{
		{
			BufferLength:       100,
			Qos:                def.AtLeastOnce,
			CheckpointInterval: cast.DurationConf(600 * time.Millisecond),
			SendError:          true,
		},
	}
	for j, opt := range options {
		DoCheckpointRuleTest(t, tests, opt, j)
	}
}
