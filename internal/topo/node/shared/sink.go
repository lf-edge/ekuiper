// Copyright 2021 INTECH Process Automation Ltd.
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

package shared

import (
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/api"
)

type sink struct {
	id string
	ch *channels
}

func (s *sink) Open(ctx api.StreamContext) error {
	return nil
}

func (s *sink) Configure(props map[string]interface{}) error {
	return nil
}

func (s *sink) Collect(ctx api.StreamContext, data interface{}) error {
	if b, casted := data.([]byte); casted {
		d, err := toMap(b)
		if err != nil {
			return err
		}
		for _, el := range d {
			for _, c := range s.ch.consumers {
				c <- el
			}
		}
		return nil
	}
	return fmt.Errorf("unrecognized format of %s", data)
}

func (s *sink) Close(ctx api.StreamContext) error {
	return closeSink(s.id)
}

func toMap(data []byte) ([]map[string]interface{}, error) {
	res := make([]map[string]interface{}, 0)
	err := json.Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}
