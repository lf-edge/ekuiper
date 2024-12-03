// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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
	"fmt"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
)

type Converter struct{}

var converter = &Converter{}

func GetConverter() (message.Converter, error) {
	return converter, nil
}

func (c *Converter) Encode(ctx api.StreamContext, d any) (b []byte, err error) {
	switch dt := d.(type) {
	case map[string]any:
		bb, ok := dt[message.DefaultField]
		if ok {
			return cast.ToByteA(bb, cast.CONVERT_SAMEKIND)
		} else {
			return nil, fmt.Errorf("field %s not exist", message.DefaultField)
		}
	}
	return nil, fmt.Errorf("unsupported type %v, must be a map", d)
}

func (c *Converter) Decode(ctx api.StreamContext, b []byte) (m any, err error) {
	result := make(map[string]interface{})
	result[message.DefaultField] = b
	return result, nil
}
