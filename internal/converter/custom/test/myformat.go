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

package main

import (
	"encoding/json"
	"fmt"
)

type Hobbies struct {
	Indoor  []string `json:"indoor"`
	Outdoor []string `json:"outdoor"`
}

type Sample struct {
	Id      int64   `json:"id"`
	Name    string  `json:"name"`
	Age     int64   `json:"age"`
	Hobbies Hobbies `json:"hobbies"`
}

func (x *Sample) GetSchemaJson() string {
	// return a static schema
	return `{
		"id": {
			"type": "bigint"
	},
		"name": {
			"type": "string"
	},
		"age": {
			"type": "bigint"
	},
		"hobbies": {
			"type": "struct",
			"properties": {
			"indoor": {
				"type": "array",
					"items": {
						"type": "string"
				}
			},
			"outdoor": {
				"type": "array",
					"items": {
						"type": "string"
				}
			}
		}
	}
	}`
}

func (x *Sample) Encode(d interface{}) ([]byte, error) {
	switch r := d.(type) {
	case map[string]interface{}:
		return json.Marshal(r)
	default:
		return nil, fmt.Errorf("unsupported type %v, must be a map", d)
	}
}

func (x *Sample) Decode(b []byte) (interface{}, error) {
	result := make(map[string]interface{})
	err := json.Unmarshal(b, &result)
	return result, err
}

func GetSample() interface{} {
	return &Sample{}
}
