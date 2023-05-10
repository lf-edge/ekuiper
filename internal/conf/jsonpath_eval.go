// Copyright 2021 EMQ Technologies Co., Ltd.
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

package conf

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/PaesslerAG/gval"
	"github.com/PaesslerAG/jsonpath"

	"github.com/lf-edge/ekuiper/pkg/cast"
)

var builder = gval.Full(jsonpath.PlaceholderExtension())

type JsonPathEval interface {
	Eval(data interface{}) (interface{}, error)
}

type gvalPathEval struct {
	valuer gval.Evaluable
}

func (e *gvalPathEval) Eval(data interface{}) (interface{}, error) {
	var input interface{}
	at := reflect.TypeOf(data)
	if at != nil {
		switch at.Kind() {
		case reflect.Map:
			input = cast.ConvertToInterfaceArr(data.(map[string]interface{}))
		case reflect.Slice:
			input = cast.ConvertSlice(data)
		case reflect.String:
			v, _ := data.(string)
			err := json.Unmarshal([]byte(v), &input)
			if err != nil {
				return nil, fmt.Errorf("data '%v' is not a valid json string", data)
			}
		default:
			return nil, fmt.Errorf("invalid data %v for jsonpath", data)
		}
	} else {
		return nil, fmt.Errorf("invalid data nil for jsonpath")
	}
	return e.valuer(context.Background(), input)
}

func GetJsonPathEval(jsonpath string) (JsonPathEval, error) {
	e, err := builder.NewEvaluable(jsonpath)
	if err != nil {
		return nil, err
	}
	return &gvalPathEval{valuer: e}, nil
}
