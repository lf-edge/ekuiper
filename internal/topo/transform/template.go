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

package transform

import (
	"encoding/json"
	"fmt"
	"maps"
	"text/template"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

func GenTp(dt string) (*template.Template, error) {
	return template.New("sink").Funcs(conf.FuncMap).Parse(dt)
}

// TransItem If you do not need to convert data to []byte, you can use this function directly. Otherwise, use TransFunc.
func TransItem(input interface{}, dataField string, fields []string) (interface{}, bool, error) {
	if dataField == "" && len(fields) == 0 {
		return input, false, nil
	}
	if _, ok := input.([]byte); ok {
		var m interface{}
		err := json.Unmarshal(input.([]byte), &m)
		if err != nil {
			return input, false, fmt.Errorf("fail to decode data %s for error %v", string(input.([]byte)), err)
		}
		input = m
	}

	if dataField != "" {
		switch input.(type) {
		case map[string]interface{}:
			input = input.(map[string]interface{})[dataField]
		case []interface{}:
			if len(input.([]interface{})) == 0 {
				return nil, false, nil
			}
			input = input.([]interface{})[0].(map[string]interface{})[dataField]
		case []map[string]interface{}:
			if len(input.([]map[string]interface{})) == 0 {
				return nil, false, nil
			}
			input = input.([]map[string]interface{})[0][dataField]
		default:
			return nil, false, fmt.Errorf("fail to decode data %v", input)
		}
	}
	if inputArr, ok := input.([]any); ok {
		ma := make([]map[string]any, len(inputArr))
		for i, v := range inputArr {
			if out, isMap := v.(map[string]interface{}); !isMap {
				return nil, false, fmt.Errorf("unsupported type %v", input)
			} else {
				ma[i] = maps.Clone(out)
			}
		}
		input = ma
	}
	m, err := selectMap(input, fields)
	if err != nil && err.Error() != "fields cannot be empty" {
		return nil, false, fmt.Errorf("fail to decode data %v for error %v", input, err)
	} else {
		return m, true, nil
	}
}

// selectMap select fields from input map or array of map.
func selectMap(input interface{}, fields []string) (interface{}, error) {
	if len(fields) == 0 {
		return input, fmt.Errorf("fields cannot be empty")
	}

	outputs := make([]map[string]interface{}, 0)
	switch input.(type) {
	case map[string]interface{}:
		output := make(map[string]interface{})
		for _, field := range fields {
			output[field] = input.(map[string]interface{})[field]
		}
		return output, nil
	case []map[string]interface{}:
		for _, v := range input.([]map[string]interface{}) {
			output := make(map[string]interface{})
			for _, field := range fields {
				output[field] = v[field]
			}
			outputs = append(outputs, output)
		}
		return outputs, nil
	default:
		return input, fmt.Errorf("unsupported type %v", input)
	}
}
