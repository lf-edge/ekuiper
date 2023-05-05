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

package context

import (
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/message"
)

const DecodeKey = "$$decode"

func (c *DefaultContext) Decode(data []byte) (map[string]interface{}, error) {
	v := c.Value(DecodeKey)
	f, ok := v.(message.Converter)
	if ok {
		t, err := f.Decode(data)
		if err != nil {
			return nil, fmt.Errorf("decode failed: %v", err)
		}
		if result, ok := t.(map[string]interface{}); ok {
			return result, nil
		} else {
			return nil, fmt.Errorf("only map[string]interface{} is supported but got: %v", t)
		}
	}
	return nil, fmt.Errorf("no decoder configured")
}

func (c *DefaultContext) DecodeIntoList(data []byte) ([]map[string]interface{}, error) {
	v := c.Value(DecodeKey)
	f, ok := v.(message.Converter)
	if ok {
		t, err := f.Decode(data)
		if err != nil {
			return nil, fmt.Errorf("decode failed: %v", err)
		}
		typeErr := fmt.Errorf("only map[string]interface{} and []map[string]interface{} is supported but got: %v", t)
		switch r := t.(type) {
		case map[string]interface{}:
			return []map[string]interface{}{r}, nil
		case []map[string]interface{}:
			return r, nil
		case []interface{}:
			rs := make([]map[string]interface{}, len(r))
			for i, v := range r {
				if vc, ok := v.(map[string]interface{}); ok {
					rs[i] = vc
				} else {
					return nil, typeErr
				}
			}
			return rs, nil
		}
		return nil, typeErr
	}
	return nil, fmt.Errorf("no decoder configured")
}
