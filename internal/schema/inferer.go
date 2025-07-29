// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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
	"strings"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func InferFromSchemaFile(schemaType string, schemaId string) (ast.StreamFields, error) {
	if c, ok := modules.SchemaTypeDefs[schemaType]; ok {
		fileId := ""
		messageId := ""
		if schemaId != "" {
			r := strings.Split(schemaId, ".")
			fileId = r[0]
			if len(r) >= 2 {
				messageId = r[1]
			}
		}
		// mock result for testing
		if conf.IsTesting {
			return ast.StreamFields{
				{
					Name: "field1",
					FieldType: &ast.BasicType{
						Type: ast.BIGINT,
					},
				},
				{
					Name: "field2",
					FieldType: &ast.BasicType{
						Type: ast.STRINGS,
					},
				},
			}, nil
		}
		ffs, err := GetSchemaFile(schemaType, fileId)
		if err != nil {
			return nil, err
		}
		return c.Infer(conf.Log, ffs.SchemaFile, messageId)
	} else {
		return nil, nil
	}
}
