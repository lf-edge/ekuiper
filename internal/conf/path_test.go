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

package conf

import (
	"strings"
	"testing"
)

func TestAbsolutePath(t *testing.T) {
	tests := []struct {
		r string
		a string
	}{
		{
			r: "etc/services",
			a: "/etc/kuiper/services",
		}, {
			r: "data/",
			a: "/var/lib/kuiper/data/",
		}, {
			r: logDir,
			a: "/var/log/kuiper",
		}, {
			r: "plugins",
			a: "/var/lib/kuiper/plugins",
		},
	}
	for i, tt := range tests {
		aa, err := absolutePath(tt.r)
		if err != nil {
			t.Errorf("error: %v", err)
		} else {
			if !(tt.a == aa) {
				t.Errorf("%d result mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.a, aa)
			}
		}
	}
}

func TestGetDataLoc_Funcs(t *testing.T) {
	d, err := GetDataLoc()
	if err != nil {
		t.Errorf("Errors when getting data loc: %s.", err)
	} else if !strings.HasSuffix(d, "kuiper/data/test") {
		t.Errorf("Unexpected data location %s", d)
	}
}
