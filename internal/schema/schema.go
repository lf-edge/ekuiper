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

package schema

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/pkg/def"
	"github.com/lf-edge/ekuiper/internal/schema/binary"
	"github.com/lf-edge/ekuiper/internal/schema/json"
	"github.com/lf-edge/ekuiper/internal/schema/protobuf"
	"github.com/lf-edge/ekuiper/pkg/message"
)

type Info struct {
	Type     def.SchemaType `json:"type"`
	Name     string         `json:"name"`
	Content  string         `json:"content"`
	FilePath string         `json:"file"`
}

var (
	schemaExt = map[def.SchemaType]string{
		def.PROTOBUF: ".proto",
	}
)

func GetOrCreateConverter(t string, schemaFile string, schemaId string) (message.Converter, error) {
	switch t {
	case message.FormatJson:
		return json.GetConverter()
	case message.FormatBinary:
		return binary.GetConverter()
	case message.FormatProtobuf:
		fileName, err := getSchemaFile(def.SchemaType(t), schemaFile)
		if err != nil {
			return nil, err
		}
		return protobuf.NewConverter(schemaId, fileName)
	default:
		return nil, fmt.Errorf("unsupported schema type: %s", t)
	}
}
