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

package converter

import (
	"fmt"
	"strings"

	"github.com/lf-edge/ekuiper/v2/internal/converter/binary"
	"github.com/lf-edge/ekuiper/v2/internal/converter/delimited"
	"github.com/lf-edge/ekuiper/v2/internal/converter/json"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func init() {
	modules.RegisterConverter(message.FormatJson, func(_ string, _ string, _ string, schema map[string]*ast.JsonStreamField) (message.Converter, error) {
		if schema == nil {
			return json.GetConverter()
		}
		return json.NewFastJsonConverter(schema), nil
	})
	modules.RegisterConverter(message.FormatBinary, func(_ string, _ string, _ string, _ map[string]*ast.JsonStreamField) (message.Converter, error) {
		return binary.GetConverter()
	})
	modules.RegisterConverter(message.FormatDelimited, func(_ string, _ string, delimiter string, _ map[string]*ast.JsonStreamField) (message.Converter, error) {
		return delimited.NewConverter(delimiter)
	})
}

func GetOrCreateConverter(options *ast.Options) (c message.Converter, err error) {
	defer func() {
		if err != nil {
			err = errorx.NewWithCode(errorx.CovnerterErr, err.Error())
		}
	}()

	t := strings.ToLower(options.FORMAT)
	if t == "" {
		t = message.FormatJson
	}

	schemaFile := ""
	schemaName := options.SCHEMAID
	if schemaName != "" {
		r := strings.Split(schemaName, ".")
		schemaFile = r[0]
		if len(r) >= 2 {
			schemaName = r[1]
		}
	}
	schema := options.Schema
	if options.IsWildCard {
		schema = nil
	}
	if c, ok := modules.Converters[t]; ok {
		return c(schemaFile, schemaName, options.DELIMITER, schema)
	}
	return nil, fmt.Errorf("format type %s not supported", t)
}
