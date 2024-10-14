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

package urlencoded

import (
	"fmt"
	"net/url"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
)

type Converter struct{}

func (c *Converter) Encode(_ api.StreamContext, d any) (b []byte, err error) {
	defer func() {
		if err != nil {
			err = errorx.NewWithCode(errorx.CovnerterErr, err.Error())
		}
	}()
	switch m := d.(type) {
	case map[string]any:
		form := url.Values{}
		for key, value := range m {
			switch vt := value.(type) {
			case []any:
				for _, item := range vt {
					ss, _ := cast.ToString(item, cast.CONVERT_ALL)
					form.Add(key, ss)
				}
			default:
				ss, _ := cast.ToString(value, cast.CONVERT_ALL)
				form.Set(key, ss)
			}
		}
		return []byte(form.Encode()), nil
	default:
		return nil, fmt.Errorf("unsupported type %v, must be a map", d)
	}
}

func (c *Converter) Decode(ctx api.StreamContext, b []byte) (any, error) {
	values, err := url.ParseQuery(string(b))
	if err != nil {
		return nil, fmt.Errorf("fail to parse url encoded value %s", string(b))
	}
	queryMap := make(map[string]interface{})
	for key, value := range values {
		if len(value) == 1 {
			queryMap[key] = value[0]
		} else {
			queryMap[key] = value
		}
	}
	return queryMap, nil
}

var c = &Converter{}

func NewConverter(props map[string]any) (message.Converter, error) {
	return c, nil
}
