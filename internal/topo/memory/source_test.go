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

package memory

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/testx"
	"reflect"
	"testing"
)

func TestTopic(t *testing.T) {
	var tests = []struct {
		wildcard string
		topics   []string
		results  []bool
		err      string
	}{
		{
			wildcard: "rule1/op1/+/temperature",
			topics: []string{
				"rule1/op1/ins1/temperature",
				"rule1/op1/ins2/temperature",
				"rule1/op1/ins1/humidity",
				"rule1/op2/ins1/temperature",
				"rule1/op1/ins1/sensor1/temperature",
			},
			results: []bool{
				true, true, false, false, false,
			},
		}, {
			wildcard: "rule1/op1/#",
			topics: []string{
				"rule1/op1/ins1/temperature",
				"rule1/op1/ins2/temperature",
				"rule1/op1/ins1/humidity",
				"rule1/op2/ins1/temperature",
				"rule1/op1/ins1/sensor1/temperature",
			},
			results: []bool{
				true, true, true, false, true,
			},
		}, {
			wildcard: "rule1/+/+/temperature",
			topics: []string{
				"rule1/op1/ins1/temperature",
				"rule1/op1/ins2/temperature",
				"rule1/op1/ins1/humidity",
				"rule1/op2/ins1/temperature",
				"rule1/op1/ins1/sensor1/temperature",
			},
			results: []bool{
				true, true, false, true, false,
			},
		}, {
			wildcard: "rule1/#/temperature",
			err:      "invalid topic rule1/#/temperature: # must at the last level",
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		r, err := getRegexp(tt.wildcard)
		if err != nil {
			if !reflect.DeepEqual(tt.err, testx.Errstring(err)) {
				t.Errorf("%d: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.err, err)
			}
			continue
		}
		results := make([]bool, len(tt.topics))
		for j, topic := range tt.topics {
			results[j] = r.MatchString(topic)
		}
		if !reflect.DeepEqual(tt.results, results) {
			t.Errorf("%d\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.results, results)
		}
	}
}
