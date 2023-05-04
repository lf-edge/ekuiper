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

package json

import (
	"encoding/json"
	"github.com/lf-edge/ekuiper/pkg/message"
)

type Converter struct {
}

var converter = &Converter{}

func GetConverter() (message.Converter, error) {
	return converter, nil
}

func (c *Converter) Encode(d interface{}) ([]byte, error) {
	return json.Marshal(d)
}

func (c *Converter) Decode(b []byte) (interface{}, error) {
	r1 := make(map[string]interface{})
	err1 := json.Unmarshal(b, &r1)
	if err1 == nil {
		return r1, nil
	}
	r2 := make([]map[string]interface{}, 0)
	err2 := json.Unmarshal(b, &r2)
	if err2 == nil {
		return r2, nil
	}
	return nil, err2
}
