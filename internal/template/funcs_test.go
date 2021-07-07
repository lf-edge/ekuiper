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

package template

import (
	"encoding/base64"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/testx"
	"reflect"
	"testing"
)

func TestBase64Encode(t *testing.T) {
	var tests = []struct {
		para   interface{}
		expect string
		err    string
	}{
		{
			para:   1,
			expect: "1",
		},

		{
			para:   float32(3.14),
			expect: "3.14",
		},

		{
			para:   float64(3.1415),
			expect: "3.1415",
		},
		{
			para:   "hello",
			expect: "hello",
		},
		{
			para:   "{\"hello\" : 3}",
			expect: "{\"hello\" : 3}",
		},
		{
			para: map[string]interface{}{
				"temperature": 30,
				"humidity":    20,
			},
			expect: `{"humidity":20,"temperature":30}`,
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		result, err := Base64Encode(tt.para)
		r, _ := base64.StdEncoding.DecodeString(result)
		if !reflect.DeepEqual(tt.err, testx.Errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.para, tt.err, err)

		} else if tt.err == "" && !reflect.DeepEqual(tt.expect, string(r)) {
			t.Errorf("%d. %q\n\n mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.para, tt.expect, string(r))
		}
	}
}
