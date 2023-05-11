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

//go:build schema || !core

package schema

import (
	"encoding/json"
	"fmt"
	"plugin"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/def"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/message"
)

func init() {
	inferes[message.FormatCustom] = InferCustom
}

func InferCustom(schemaFile string, messageName string) (ast.StreamFields, error) {
	conf.Log.Infof("Load custom schema from file %s, for symbol Get%s", schemaFile, messageName)
	ffs, err := GetSchemaFile(def.CUSTOM, schemaFile)
	if err != nil {
		return nil, err
	}
	if ffs.SoFile == "" {
		return nil, fmt.Errorf("no so file found for custom schema %s", messageName)
	}
	sp, err := plugin.Open(ffs.SoFile)
	if err != nil {
		conf.Log.Errorf(fmt.Sprintf("custom schema file %s open error: %v", ffs.SoFile, err))
		return nil, fmt.Errorf("cannot open %s: %v", ffs.SoFile, err)
	}
	nf, err := sp.Lookup("Get" + messageName)
	if err != nil {
		conf.Log.Warnf(fmt.Sprintf("cannot find schemaId %s, please check if it is exported: Get%v", messageName, err))
		return nil, nil
	}
	nff, ok := nf.(func() interface{})
	if !ok {
		conf.Log.Errorf("exported symbol Get%s is not func to return interface{}", messageName)
		return nil, fmt.Errorf("load custom schema %s, message %s error", ffs.SoFile, messageName)
	}
	mc, ok := nff().(message.SchemaProvider)
	if ok {
		sj := mc.GetSchemaJson()
		var result ast.StreamFields
		err := json.Unmarshal([]byte(sj), &result)
		if err != nil {
			return nil, fmt.Errorf("invalid schema json %s: %v", sj, err)
		}
		return result, nil
	} else {
		return nil, fmt.Errorf("get schema converter failed, exported symbol %s is not type of message.Converter", messageName)
	}
}
