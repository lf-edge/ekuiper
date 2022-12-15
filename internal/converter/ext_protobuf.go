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

//go:build schema || !core
// +build schema !core

package converter

import (
	"github.com/lf-edge/ekuiper/internal/converter/protobuf"
	"github.com/lf-edge/ekuiper/internal/pkg/def"
	"github.com/lf-edge/ekuiper/internal/schema"
	"github.com/lf-edge/ekuiper/pkg/message"
)

func init() {
	converters[message.FormatProtobuf] = func(schemaFileName string, schemaMessageName string, _ string) (message.Converter, error) {
		ffs, err := schema.GetSchemaFile(def.PROTOBUF, schemaFileName)
		if err != nil {
			return nil, err
		}
		return protobuf.NewConverter(ffs.SchemaFile, ffs.SoFile, schemaMessageName)
	}
}
