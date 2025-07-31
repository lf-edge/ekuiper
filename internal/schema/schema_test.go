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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func TestSchemaInfo(t *testing.T) {
	modules.RegisterSchemaType(modules.PROTOBUF, &PbType{}, ".proto")
	modules.RegisterSchemaType(modules.CUSTOM, &CustomType{}, ".so")
	tests := []struct {
		i    *Info
		name string
		err  error
	}{
		{
			name: "invalid type",
			i: &Info{
				Type:    "static",
				Name:    "aa",
				Content: "bb",
				SoPath:  "dd",
			},
			err: errors.New("unsupported schema type static"),
		},
		{
			name: "invalid content",
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
			name: "missing name",
			i: &Info{
				Type:     "protobuf",
				FilePath: "cc",
				SoPath:   "dd",
			},
			err: errors.New("name is required"),
		},
		{
			name: "missing content",
			i: &Info{
				Type:   "protobuf",
				Name:   "aa",
				SoPath: "dd",
			},
			err: errors.New("must specify content or file"),
		},
		{
			name: "valid",
			i: &Info{
				Type:    "protobuf",
				Name:    "aa",
				Content: "bb",
				SoPath:  "dd",
			},
			err: nil,
		},
		{
			name: "valid2",
			i: &Info{
				Type:    "protobuf",
				Name:    "aa",
				Content: "bb",
			},
			err: nil,
		},
		{
			name: "valid custom",
			i: &Info{
				Type:   "custom",
				Name:   "aa",
				SoPath: "bb",
			},
			err: nil,
		},
		{
			name: "missing so",
			i: &Info{
				Type:    "custom",
				Name:    "aa",
				Content: "bb",
			},
			err: errors.New("soFile is required"),
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.i.Validate()
			if tt.err != nil {
				assert.EqualError(t, err, tt.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
