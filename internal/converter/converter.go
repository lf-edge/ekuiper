// Copyright 2022-2025 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/v2/internal/converter/urlencoded"
	"github.com/lf-edge/ekuiper/v2/internal/schema"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func init() {
	modules.RegisterConverter(message.FormatJson, func(_ api.StreamContext, _ string, schema map[string]*ast.JsonStreamField, props map[string]any) (message.Converter, error) {
		return json.NewFastJsonConverter(schema, props), nil
	})
	modules.RegisterConverter(message.FormatBinary, func(_ api.StreamContext, _ string, _ map[string]*ast.JsonStreamField, props map[string]any) (message.Converter, error) {
		return binary.GetConverter()
	})
	modules.RegisterConverter(message.FormatDelimited, func(_ api.StreamContext, _ string, _ map[string]*ast.JsonStreamField, props map[string]any) (message.Converter, error) {
		return delimited.NewConverter(props)
	})
	modules.RegisterConverter(message.FormatUrlEncoded, func(_ api.StreamContext, _ string, _ map[string]*ast.JsonStreamField, props map[string]any) (message.Converter, error) {
		return urlencoded.NewConverter(props)
	})
	modules.RegisterWriterConverter(message.FormatDelimited, func(ctx api.StreamContext, _ string, _ map[string]*ast.JsonStreamField, props map[string]any) (message.ConvertWriter, error) {
		return delimited.NewCsvWriter(ctx, props)
	})
	modules.RegisterWriterConverter(message.FormatJson, func(ctx api.StreamContext, _ string, schema map[string]*ast.JsonStreamField, props map[string]any) (message.ConvertWriter, error) {
		return json.NewFastJsonConverter(schema, props), nil
	})
}

func GetOrCreateConverter(ctx api.StreamContext, format string, schemaId string, schemaFields map[string]*ast.JsonStreamField, props map[string]any) (c message.Converter, err error) {
	defer func() {
		if err != nil {
			err = errorx.NewWithCode(errorx.CovnerterErr, err.Error())
		}
	}()

	t := strings.ToLower(format)
	if t == "" {
		t = message.FormatJson
	}
	if cp, ok := modules.Converters[t]; ok {
		schemaPath, err := transSchemaId(t, schemaId, props)
		if err != nil {
			return nil, err
		}
		return cp(ctx, schemaPath, schemaFields, props)
	}
	return nil, fmt.Errorf("format type %s not supported", t)
}

func GetConvertWriter(ctx api.StreamContext, format string, schemaId string, schema map[string]*ast.JsonStreamField, props map[string]any) (message.ConvertWriter, error) {
	t := strings.ToLower(format)
	schemaPath, err := transSchemaId(t, schemaId, map[string]any{})
	if err != nil {
		return nil, err
	}
	if cw, ok := modules.ConvertWriters[t]; ok {
		return cw(ctx, schemaPath, schema, props)
	}
	c, err := GetOrCreateConverter(ctx, t, schemaPath, schema, props)
	if err != nil {
		return nil, err
	}
	ctx.GetLogger().Infof("writer %s not found, fall back to stack writer", t)
	return NewStackWriter(ctx, c)
}

func GetMerger(ctx api.StreamContext, format string, schemaId string, schemaFields map[string]*ast.JsonStreamField, props map[string]any) (modules.Merger, error) {
	t := strings.ToLower(format)
	if mp, ok := modules.Mergers[t]; ok {
		schemaPath, err := transSchemaId(t, schemaId, map[string]any{})
		if err != nil {
			return nil, err
		}
		return mp(ctx, schemaPath, schemaFields, props)
	} else {
		return nil, fmt.Errorf("merger %s not found", t)
	}
}

func transSchemaId(t, schemaId string, props map[string]any) (string, error) {
	schemaType, requireSchema := modules.ConverterSchemas[t]
	if requireSchema {
		schemaFileId := ""
		if schemaId != "" {
			r := strings.SplitN(schemaId, ".", 2)
			schemaFileId = r[0]
			if len(r) == 2 {
				props["$$messageName"] = r[1]
			}
		}
		ffs, err := schema.GetSchemaFile(schemaType, schemaFileId)
		if err != nil {
			return "", err
		}
		return ffs.SchemaFile, nil
	} else { // If not require schema, just return the schemaId. And the register function need to deal with it by itself. Only the specific implementation defines the schemaId format.
		return schemaId, nil
	}
}
