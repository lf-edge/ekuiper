// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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

package converter

import (
	"strings"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/converter/protobuf"
	"github.com/lf-edge/ekuiper/v2/internal/schema"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func init() {
	modules.RegisterConverter(message.FormatProtobuf, func(_ api.StreamContext, schemaId string, _ map[string]*ast.JsonStreamField, props map[string]any) (message.Converter, error) {
		schemaFile := ""
		schemaName := ""
		if schemaId != "" {
			r := strings.Split(schemaId, ".")
			schemaFile = r[0]
			if len(r) >= 2 {
				schemaName = r[1]
			}
		}
		ffs, err := schema.GetSchemaFile(modules.PROTOBUF, schemaFile)
		if err != nil {
			return nil, err
		}
		return protobuf.NewConverter(ffs.SchemaFile, ffs.SoFile, schemaName)
	})
}
