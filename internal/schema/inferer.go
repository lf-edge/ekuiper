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
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"strings"
)

type inferer func(schemaFileName string, SchemaMessageName string) (ast.StreamFields, error)

var ( // init once and read only
	inferes = map[string]inferer{}
)

func InferFromSchemaFile(schemaType string, schemaId string) (ast.StreamFields, error) {
	r := strings.Split(schemaId, ".")
	if len(r) != 2 {
		return nil, fmt.Errorf("invalid schemaId: %s", schemaId)
	}
	if c, ok := inferes[schemaType]; ok {
		return c(r[0], r[1])
	} else {
		return nil, fmt.Errorf("unsupported type: %s", schemaType)
	}
}
