// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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

package processor

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

func TestRuleActionParse_Apply(t *testing.T) {
	tests := []struct {
		ruleStr string
		result  *def.Rule
	}{
		{
			ruleStr: `{
			  "id": "ruleTest",
			  "sql": "SELECT * from demo",
			  "actions": [
				{
				  	"funcName": "RFC_READ_TABLE",
					"ashost":   "192.168.1.100",
					"sysnr":    "02",
					"client":   "900",
					"user":     "SPERF",
					"passwd":   "PASSPASS",
					"params": {
						"QUERY_TABLE": "VBAP",
						"ROWCOUNT":    10,
						"FIELDS": [
							{"FIELDNAME": "MANDT"},
							{"FIELDNAME": "VBELN"},
							{"FIELDNAME": "POSNR"}
						]
					}
				}
			  ],
              "options": {
				"restartStrategy": {
				  "attempts": 20
				}
			  }
			}`,
			result: &def.Rule{
				Triggered: true,
				Id:        "ruleTest",
				Sql:       "SELECT * from demo",
				Actions: []map[string]interface{}{
					{
						"funcName": "RFC_READ_TABLE",
						"ashost":   "192.168.1.100",
						"sysnr":    "02",
						"client":   "900",
						"user":     "SPERF",
						"passwd":   "PASSPASS",
						"params": map[string]interface{}{
							"QUERY_TABLE": "VBAP",
							"ROWCOUNT":    float64(10),
							"FIELDS": []interface{}{
								map[string]interface{}{"FIELDNAME": "MANDT"},
								map[string]interface{}{"FIELDNAME": "VBELN"},
								map[string]interface{}{"FIELDNAME": "POSNR"},
							},
						},
					},
				},
				Options: &def.RuleOption{
					IsEventTime:        false,
					LateTol:            cast.DurationConf(time.Second),
					Concurrency:        1,
					BufferLength:       1024,
					SendMetaToSink:     false,
					Qos:                def.AtMostOnce,
					CheckpointInterval: cast.DurationConf(5 * time.Minute),
					SendError:          true,
					RestartStrategy: &def.RestartStrategy{
						Attempts:     20,
						Delay:        cast.DurationConf(time.Second),
						Multiplier:   2,
						MaxDelay:     cast.DurationConf(30 * time.Second),
						JitterFactor: 0.1,
					},
				},
			},
		},
		{
			ruleStr: `{
				"id": "ruleTest2",
				"sql": "SELECT * from demo",
				"actions": [
					{
						"log": ""
					},
					{
						"sap": {
							"funcName": "RFC_READ_TABLE",
							"ashost": "192.168.100.10",
							"sysnr": "02",
							"client": "900",
							"user": "uuu",
							"passwd": "ppp."
						}
					}
				],
				"options": {
					"isEventTime": true,
					"lateTolerance": 1000,
					"bufferLength": 10240,
					"qos": 2,
					"checkpointInterval": "60s"
				}
			}`,
			result: &def.Rule{
				Triggered: true,
				Id:        "ruleTest2",
				Sql:       "SELECT * from demo",
				Actions: []map[string]interface{}{
					{
						"log": "",
					}, {
						"sap": map[string]interface{}{
							"funcName": "RFC_READ_TABLE",
							"ashost":   "192.168.100.10",
							"sysnr":    "02",
							"client":   "900",
							"user":     "uuu",
							"passwd":   "ppp.",
						},
					},
				},
				Options: &def.RuleOption{
					IsEventTime:        true,
					LateTol:            cast.DurationConf(time.Second),
					Concurrency:        1,
					BufferLength:       10240,
					SendMetaToSink:     false,
					Qos:                def.ExactlyOnce,
					CheckpointInterval: cast.DurationConf(time.Minute),
					SendError:          true,
					RestartStrategy: &def.RestartStrategy{
						Attempts:     0,
						Delay:        cast.DurationConf(time.Second),
						Multiplier:   2,
						MaxDelay:     cast.DurationConf(30 * time.Second),
						JitterFactor: 0.1,
					},
				},
			},
		},
		{
			ruleStr: `{
			  "id": "ruleTest",
			  "sql": "SELECT * from demo",
			  "actions": [
			  	{"log": {}}
			  ],
              "triggered": false
			}`,
			result: &def.Rule{
				Triggered: false,
				Id:        "ruleTest",
				Sql:       "SELECT * from demo",
				Actions: []map[string]interface{}{
					{
						"log": map[string]interface{}{},
					},
				},
				Options: &def.RuleOption{
					IsEventTime:        false,
					LateTol:            cast.DurationConf(time.Second),
					Concurrency:        1,
					BufferLength:       1024,
					SendMetaToSink:     false,
					Qos:                def.AtMostOnce,
					CheckpointInterval: cast.DurationConf(5 * time.Minute),
					SendError:          true,
					RestartStrategy: &def.RestartStrategy{
						Attempts:     0,
						Delay:        cast.DurationConf(time.Second),
						Multiplier:   2,
						MaxDelay:     cast.DurationConf(30 * time.Second),
						JitterFactor: 0.1,
					},
				},
			},
		},
	}

	p := NewRuleProcessor()
	for _, tt := range tests {
		t.Run(tt.ruleStr, func(t *testing.T) {
			r, err := p.GetRuleByJson(tt.result.Id, tt.ruleStr)
			assert.NoError(t, err)
			assert.Equal(t, tt.result, r)
		})
	}
}

func TestAllRules(t *testing.T) {
	expected := map[string]string{
		"rule1": "{\"id\": \"rule1\",\"sql\": \"SELECT * FROM demo\",\"actions\": [{  \"log\": {}}]}",
		"rule2": "{\"id\": \"rule2\",\"sql\": \"SELECT * FROM demo\",\"actions\": [{  \"log\": {}}]}",
		"rule3": "{\"id\": \"rule3\",\"sql\": \"SELECT * FROM demo\",\"actions\": [{  \"log\": {}}]}",
	}
	sp := NewStreamProcessor()
	defer func() {
		err := sp.db.Clean()
		assert.NoError(t, err)
	}()
	_, err := sp.ExecStmt(`CREATE STREAM demo () WITH (DATASOURCE="users", FORMAT="JSON")`)
	assert.NoError(t, err)
	p := NewRuleProcessor()
	err = p.db.Clean()
	assert.NoError(t, err)

	for k, v := range expected {
		_, err := p.ExecCreateWithValidation(k, v)
		if err != nil {
			t.Error(err)
			return
		}
		// Intend to drop after all running done
		defer p.ExecDrop(k)
	}

	all, err := p.GetAllRulesJson()
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, expected, all)
}

func TestValidateRuleID(t *testing.T) {
	testcases := []struct {
		id  string
		err error
	}{
		{
			"abc",
			nil,
		},
		{
			"ABC",
			nil,
		},
		{
			"123",
			nil,
		},
		{
			"1/2",
			fmt.Errorf("ruleID:%s contains invalidChar:%v", "1/2", "/"),
		},
		{
			"1#2",
			fmt.Errorf("ruleID:%s contains invalidChar:%v", "1#2", "#"),
		},
		{
			"1%2",
			fmt.Errorf("ruleID:%s contains invalidChar:%v", "1%2", "%"),
		},
		{
			id:  "\t123",
			err: fmt.Errorf("ruleID: %v should be trimed", "\t123"),
		},
		{
			id:  "123\t",
			err: fmt.Errorf("ruleID: %v should be trimed", "123\t"),
		},
	}
	for _, tc := range testcases {
		require.Equal(t, tc.err, validateRuleID(tc.id))
	}
}
