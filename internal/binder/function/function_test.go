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

package function

import (
	"fmt"
	"testing"
)

func TestManager(t *testing.T) {
	var tests = []struct {
		name  string
		found bool
	}{
		{
			name:  "sum",
			found: true,
		}, {
			name:  "agg",
			found: false,
		}, {
			name:  "ln",
			found: true,
		}, {
			name:  "regexp_matches",
			found: true,
		}, {
			name:  "encode",
			found: true,
		}, {
			name:  "json_path_query",
			found: true,
		}, {
			name:  "window_start",
			found: true,
		}, {
			name:  "cardinality",
			found: true,
		},
	}
	m := GetManager()
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for _, tt := range tests {
		f, _ := m.Function(tt.name)
		found := f != nil
		if tt.found != found {
			t.Errorf("%s result mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", tt.name, tt.found, found)
		}
	}
	h := m.HasFunctionSet("internal")
	if !h {
		t.Errorf("can't find function set internal")
	}
	h = m.HasFunctionSet("other")
	if h {
		t.Errorf("find undefined function set other")
	}
}
