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

package main

import (
	"reflect"
	"testing"

	"github.com/lf-edge/ekuiper/sdk/go/api"
	"github.com/lf-edge/ekuiper/sdk/go/mock"
)

func TestConfigure(t *testing.T) {
	var tests = []struct {
		p   map[string]interface{}
		err string
	}{
		{
			p: map[string]interface{}{
				"interval":    float64(-2),
				"seed":        float64(1),
				"pattern":     map[string]interface{}{"count": float64(5)},
				"deduplicate": 0,
			},
			err: "source `random` property `interval` must be a positive integer but got -2",
		}, {
			p: map[string]interface{}{
				"intervala":   float64(-2),
				"seed":        float64(1),
				"pattern":     map[string]interface{}{"count": float64(5)},
				"deduplicate": 0,
			},
			err: "source `random` property `interval` must be a positive integer but got 0",
		}, {
			p: map[string]interface{}{
				"interval":    float64(200),
				"seed":        float64(-1),
				"pattern":     map[string]interface{}{"count": float64(5)},
				"deduplicate": 0,
			},
			err: "source `random` property `seed` must be a positive integer but got -1",
		}, {
			p: map[string]interface{}{
				"interval":    float64(5),
				"seed":        float64(5),
				"deduplicate": 0,
			},
			err: "source `random` property `pattern` is required",
		},
	}
	t.Logf("The test bucket size is %d.", len(tests))
	for i, tt := range tests {
		r := &randomSource{}
		err := r.Configure("new", tt.p)
		if !reflect.DeepEqual(tt.err, Errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, err, tt.err, err)
		}
	}
}

func TestRunRandom(t *testing.T) {
	exp := []api.SourceTuple{
		api.NewDefaultSourceTuple(map[string]interface{}{"count": 50}, nil),
		api.NewDefaultSourceTuple(map[string]interface{}{"count": 50}, nil),
		api.NewDefaultSourceTuple(map[string]interface{}{"count": 50}, nil),
		api.NewDefaultSourceTuple(map[string]interface{}{"count": 50}, nil),
		api.NewDefaultSourceTuple(map[string]interface{}{"count": 50}, nil),
	}
	p := map[string]interface{}{
		"interval":    float64(100),
		"seed":        float64(1), // Use seed = 1, to make sure results always the same
		"pattern":     map[string]interface{}{"count": float64(50)},
		"deduplicate": 0,
	}
	r := &randomSource{}
	err := r.Configure("new", p)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	mock.TestSourceOpen(r, exp, t)
}

func Errstring(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}
