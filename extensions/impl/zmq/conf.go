// Copyright 2024 EMQ Technologies Co., Ltd.
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

package zmq

import (
	"errors"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

type c struct {
	Topic  string `json:"datasource"`
	Server string `json:"server"`
}

func validate(_ api.StreamContext, props map[string]any) (*c, error) {
	sc := &c{}
	err := cast.MapToStruct(props, sc)
	if err != nil {
		return nil, err
	}
	if sc.Server == "" {
		return nil, errors.New("missing server address")
	}
	return sc, nil
}