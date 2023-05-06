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

package transform

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/converter"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/message"
	"text/template"
)

type TransFunc func(interface{}) ([]byte, bool, error)

func GenTransform(dt string, format string, schemaId string, delimiter string, fields []string) (TransFunc, error) {
	var (
		tp  *template.Template = nil
		c   message.Converter
		out message.Converter
		err error
	)
	switch format {
	case message.FormatProtobuf, message.FormatCustom:
		c, err = converter.GetOrCreateConverter(&ast.Options{FORMAT: format, SCHEMAID: schemaId})
		if err != nil {
			return nil, err
		}
		out, _ = converter.GetOrCreateConverter(&ast.Options{FORMAT: format, SCHEMAID: schemaId})
	case message.FormatDelimited:
		c, err = converter.GetOrCreateConverter(&ast.Options{FORMAT: format, DELIMITER: delimiter})
		if err != nil {
			return nil, err
		}
		out, _ = converter.GetOrCreateConverter(&ast.Options{FORMAT: format, DELIMITER: delimiter})
	}

	if dt != "" {
		temp, err := template.New("sink").Funcs(conf.FuncMap).Parse(dt)
		if err != nil {
			return nil, err
		}
		tp = temp
	}
	return func(d interface{}) ([]byte, bool, error) {
		var (
			bs          []byte
			transformed bool
		)
		if tp != nil {
			var output bytes.Buffer
			err := tp.Execute(&output, d)
			if err != nil {
				return nil, false, fmt.Errorf("fail to encode data %v with dataTemplate for error %v", d, err)
			}
			bs = output.Bytes()
			transformed = true
		}
		switch format {
		case message.FormatJson:
			if transformed {
				return selectJson(bs, fields, transformed)
			}
			j, err := json.Marshal(d)
			if err != nil {
				return nil, false, fmt.Errorf("fail to encode data %v for error %v", d, err)
			}
			return selectJson(j, fields, transformed)
		case message.FormatProtobuf, message.FormatCustom, message.FormatDelimited:
			if transformed {
				m := make(map[string]interface{})
				err := json.Unmarshal(bs, &m)
				if err != nil {
					return nil, false, fmt.Errorf("fail to decode data %s after applying dataTemplate for error %v", string(bs), err)
				}
				d = m
			}
			b, err := c.Encode(d)
			if err != nil {
				return nil, false, fmt.Errorf("fail to encode data %v for error %v", d, err)
			}
			mm, ok := d.(map[string]interface{})
			if !ok {
				return b, false, fmt.Errorf("expect map[string]interface{} but got %T", mm)
			}
			outBytes, err := out.Encode(selectMap(mm, fields))
			if err != nil {
				return b, false, fmt.Errorf("fail to encode data %v for error %v", d, err)
			}
			return outBytes, true, nil
		default: // should not happen
			return nil, false, fmt.Errorf("unsupported format %v", format)
		}
	}, nil
}

func GenTp(dt string) (*template.Template, error) {
	return template.New("sink").Funcs(conf.FuncMap).Parse(dt)
}

// selectJson select fields from json bytes
func selectJson(bytes []byte, fields []string, transformed bool) ([]byte, bool, error) {
	if len(fields) == 0 {
		return bytes, transformed, nil
	}
	var m interface{}
	err := json.Unmarshal(bytes, &m)
	if err != nil {
		return bytes, transformed, err
	}
	switch m.(type) {
	case []interface{}:
		mm := m.([]interface{})
		outputs := make([]map[string]interface{}, len(mm))
		for i, v := range mm {
			if out, ok := v.(map[string]interface{}); !ok {
				return bytes, transformed, fmt.Errorf("fail to decode json, unsupported type %v", mm)
			} else {
				outputs[i] = selectMap(out, fields)
			}
		}
		jsonBytes, err := json.Marshal(outputs)
		return jsonBytes, true, err
	case []map[string]interface{}:
		mm := m.([]map[string]interface{})
		outputs := make([]map[string]interface{}, len(mm))
		for i, v := range mm {
			outputs[i] = selectMap(v, fields)
		}
		jsonBytes, err := json.Marshal(outputs)
		return jsonBytes, true, err
	case map[string]interface{}:
		mm := m.(map[string]interface{})
		jsonBytes, err := json.Marshal(selectMap(mm, fields))
		return jsonBytes, true, err
	default:
		return bytes, transformed, fmt.Errorf("fail to decode json, unsupported type %v", m)
	}
}

// selectMap select fields from input map
func selectMap(input map[string]interface{}, fields []string) map[string]interface{} {
	if len(fields) == 0 {
		return input
	}
	output := make(map[string]interface{})
	for _, field := range fields {
		if v, ok := input[field]; ok {
			output[field] = v
		}
	}
	// if no field is selected, return the whole map
	if len(output) == 0 {
		return input
	}
	return output

}
