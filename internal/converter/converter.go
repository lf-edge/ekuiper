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

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/converter/binary"
	"github.com/lf-edge/ekuiper/v2/internal/converter/delimited"
	"github.com/lf-edge/ekuiper/v2/internal/converter/json"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func init() {
	modules.RegisterConverter(message.FormatJson, func(_ api.StreamContext, _ string, schema map[string]*ast.JsonStreamField, props map[string]any) (message.Converter, error) {
		return json.NewFastJsonConverter(schema), nil
	})
	modules.RegisterConverter(message.FormatBinary, func(_ api.StreamContext, _ string, _ map[string]*ast.JsonStreamField, props map[string]any) (message.Converter, error) {
		return binary.GetConverter()
	})
	modules.RegisterConverter(message.FormatDelimited, func(_ api.StreamContext, _ string, _ map[string]*ast.JsonStreamField, props map[string]any) (message.Converter, error) {
		return delimited.NewConverter(props)
	})
}

func GetOrCreateConverter(ctx api.StreamContext, format string, schemaId string, schema map[string]*ast.JsonStreamField, props map[string]any) (c message.Converter, err error) {
	defer func() {
		if err != nil {
			err = errorx.NewWithCode(errorx.CovnerterErr, err.Error())
		}
	}()

	t := strings.ToLower(format)
	if t == "" {
		t = message.FormatJson
	}
	if c, ok := modules.Converters[t]; ok {
		return c(ctx, schemaId, schema, props)
	}
	return nil, fmt.Errorf("format type %s not supported", t)
}
