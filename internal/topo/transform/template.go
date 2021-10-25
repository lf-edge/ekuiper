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

package transform

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"text/template"
)

type TransFunc func(interface{}) ([]byte, bool, error)

func GenTransform(dt string) (TransFunc, error) {
	var tp *template.Template = nil
	if dt != "" {
		temp, err := template.New("sink").Funcs(conf.FuncMap).Parse(dt)
		if err != nil {
			return nil, err
		}
		tp = temp
	}
	return func(d interface{}) ([]byte, bool, error) {
		if tp != nil {
			var output bytes.Buffer
			err := tp.Execute(&output, d)
			if err != nil {
				return nil, false, fmt.Errorf("fail to encode data %v with dataTemplate", d)
			}
			return output.Bytes(), true, nil
		} else {
			j, err := json.Marshal(d)
			return j, false, err
		}
	}, nil
}

func GenTp(dt string) (*template.Template, error) {
	return template.New("sink").Funcs(conf.FuncMap).Parse(dt)
}
