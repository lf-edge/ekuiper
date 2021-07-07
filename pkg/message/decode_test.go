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

package message

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"path"
	"reflect"
	"testing"
)

func TestMessageDecode(t *testing.T) {
	image, err := ioutil.ReadFile(path.Join("../../docs", "cover.jpg"))
	if err != nil {
		t.Errorf("Cannot read image: %v", err)
	}
	b64img := base64.StdEncoding.EncodeToString(image)
	var tests = []struct {
		payload []byte
		format  string
		result  map[string]interface{}
	}{
		{
			payload: image,
			format:  "binary",
			result: map[string]interface{}{
				"self": image,
			},
		}, {
			payload: []byte(fmt.Sprintf(`{"format":"jpg","content":"%s"}`, b64img)),
			format:  "json",
			result: map[string]interface{}{
				"format":  "jpg",
				"content": b64img,
			},
		},
	}
	for i, tt := range tests {
		result, err := Decode(tt.payload, tt.format)
		if err != nil {
			t.Errorf("%d decode error: %v", i, err)
		}
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d result mismatch:\n\nexp=%s\n\ngot=%s\n\n", i, tt.result, result)
		}
	}
}
