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

package converter

import (
	"fmt"
	"strings"

	"github.com/lf-edge/ekuiper/internal/converter/binary"
	"github.com/lf-edge/ekuiper/internal/converter/delimited"
	"github.com/lf-edge/ekuiper/internal/converter/json"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/errorx"
	"github.com/lf-edge/ekuiper/pkg/message"
)

// Instantiator The format, schema information are passed in by stream options
// The columns information is defined in the source side, like file source
type Instantiator func(schemaFileName string, SchemaMessageName string, delimiter string) (message.Converter, error)

// init once and read only
var converters = map[string]Instantiator{
	message.FormatJson: func(_ string, _ string, _ string) (message.Converter, error) {
		return json.GetConverter()
	},
	message.FormatBinary: func(_ string, _ string, _ string) (message.Converter, error) {
		return binary.GetConverter()
	},
	message.FormatDelimited: func(_ string, _ string, delimiter string) (message.Converter, error) {
		return delimited.NewConverter(delimiter)
	},
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
	if t == message.FormatJson {
		// it's unit test
		if options.RuleID == "" || options.StreamName == "" {
			return json.GetConverter()
		}
		return json.NewFastJsonConverter(options.RuleID, options.StreamName, options.Schema, options.IsWildCard, options.IsSchemaLess), nil
	}

	schemaFile := ""
	schemaName := options.SCHEMAID
	if schemaName != "" {
		r := strings.Split(schemaName, ".")
		if len(r) != 2 {
			return nil, fmt.Errorf("invalid schemaId: %s", schemaName)
		}
		schemaFile = r[0]
		schemaName = r[1]
	}
	if c, ok := converters[t]; ok {
		return c(schemaFile, schemaName, options.DELIMITER)
	}
	return nil, fmt.Errorf("format type %s not supported", t)
}
