// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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
	"text/template"

	"github.com/lf-edge/ekuiper/v2/pkg/props"
)

var FuncMap template.FuncMap

func init() {
	FuncMap = make(template.FuncMap)
	FuncMap["prop"] = func(k string) string {
		v, ok := props.SC.Get(k)
		if !ok {
			return k
		} else {
			return v
		}
	}
}
