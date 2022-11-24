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

package converter

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/converter/binary"
	"github.com/lf-edge/ekuiper/internal/converter/custom"
	"github.com/lf-edge/ekuiper/internal/converter/json"
	"github.com/lf-edge/ekuiper/pkg/message"
)

type Instantiator func(t string, schemaFileName string, SchemaMessageName string) (message.Converter, error)

var ( // init once and read only
	converters = map[string]Instantiator{
		message.FormatJson: func(t string, schemaFileName string, SchemaMessageName string) (message.Converter, error) {
			return json.GetConverter()
		},
		message.FormatBinary: func(t string, schemaFileName string, SchemaMessageName string) (message.Converter, error) {
			return binary.GetConverter()
		},
		message.FormatCustom: custom.LoadConverter,
	}
)

func GetOrCreateConverter(t string, schemaFileName string, SchemaMessageName string) (message.Converter, error) {
	if c, ok := converters[t]; ok {
		return c(t, schemaFileName, SchemaMessageName)
	}
	return nil, fmt.Errorf("format type %s not supported", t)
}
