// Copyright 2022 EMQ Technologies Co., Ltd.
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

package binary

import (
	"os"
	"path"
	"reflect"
	"testing"
)

func TestMessageDecode(t *testing.T) {
	image, err := os.ReadFile(path.Join("../../../docs", "cover.jpg"))
	if err != nil {
		t.Errorf("Cannot read image: %v", err)
	}
	tests := []struct {
		payload []byte
		result  map[string]interface{}
	}{
		{
			payload: image,
			result: map[string]interface{}{
				"self": image,
			},
		},
	}
	conv, _ := GetConverter()
	for i, tt := range tests {
		result, err := conv.Decode(tt.payload)
		if err != nil {
			t.Errorf("%d decode error: %v", i, err)
		}
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d result mismatch:\n\nexp=%s\n\ngot=%s\n\n", i, tt.result, result)
		}
	}
}
