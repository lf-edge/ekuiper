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

package schema

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestSchemaInfo(t *testing.T) {
	tests := []struct {
		i   *Info
		err error
	}{
		{
			i: &Info{
				Type:    "static",
				Name:    "aa",
				Content: "bb",
				SoPath:  "dd",
			},
			err: errors.New("unsupported type: static"),
		},
		{
			i: &Info{
				Type:     "static",
				Name:     "aa",
				Content:  "bb",
				FilePath: "cc",
				SoPath:   "dd",
			},
			err: errors.New("cannot specify both content and file"),
		},
		{
			i: &Info{
				Type:     "protobuf",
				FilePath: "cc",
				SoPath:   "dd",
			},
			err: errors.New("name is required"),
		},
		{
			i: &Info{
				Type:   "protobuf",
				Name:   "aa",
				SoPath: "dd",
			},
			err: errors.New("must specify content or file"),
		},
		{
			i: &Info{
				Type:    "protobuf",
				Name:    "aa",
				Content: "bb",
				SoPath:  "dd",
			},
			err: nil,
		},
		{
			i: &Info{
				Type:    "protobuf",
				Name:    "aa",
				Content: "bb",
			},
			err: nil,
		},
		{
			i: &Info{
				Type:   "custom",
				Name:   "aa",
				SoPath: "bb",
			},
			err: nil,
		},
		{
			i: &Info{
				Type:    "custom",
				Name:    "aa",
				Content: "bb",
			},
			err: errors.New("soFile is required"),
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		err := tt.i.Validate()
		if !reflect.DeepEqual(err, tt.err) {
			t.Errorf("%d failed,\n expect: %v, \nbut got: %v", i, tt.err, err)
		}
	}
}
