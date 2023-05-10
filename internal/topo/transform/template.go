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
	"text/template"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/converter"
	"github.com/lf-edge/ekuiper/internal/converter/delimited"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/message"
)

// TransFunc is the function to transform data

// The second parameter indicates whether to select fields based on the fields property.
// If it is false, then after the dataTemplate, output the result directly.
// If it is true, then after the dataTemplate, select the fields based on the fields property.

type TransFunc func(interface{}, bool) ([]byte, bool, error)

func GenTransform(dt string, format string, schemaId string, delimiter string, fields []string) (TransFunc, error) {
	var (
		tp  *template.Template = nil
		c   message.Converter
		err error
	)
	switch format {
	case message.FormatProtobuf, message.FormatCustom:
		c, err = converter.GetOrCreateConverter(&ast.Options{FORMAT: format, SCHEMAID: schemaId})
		if err != nil {
			return nil, err
		}
	case message.FormatDelimited:
		c, err = converter.GetOrCreateConverter(&ast.Options{FORMAT: format, DELIMITER: delimiter})
		if err != nil {
			return nil, err
		}
		c.(*delimited.Converter).SetColumns(fields)
	case message.FormatJson:
		c, err = converter.GetOrCreateConverter(&ast.Options{FORMAT: format})
		if err != nil {
			return nil, err
		}
	}

	if dt != "" {
		temp, err := template.New("sink").Funcs(conf.FuncMap).Parse(dt)
		if err != nil {
			return nil, err
		}
		tp = temp
	}
	return func(d interface{}, s bool) ([]byte, bool, error) {
		var (
			bs          []byte
			transformed bool
			selected    bool
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
		// just for sinks like tdengine and sql.
		if !s {
			if transformed {
				return bs, true, nil
			}
			outBytes, err := json.Marshal(d)
			return outBytes, false, err
		} else {
			// Consider that if only the dataTemplate is needed, and the data after trans cannot be converted into map[string]interface
			var m interface{}
			var err error
			if transformed {
				m, err = SelectMap(bs, fields)
			} else {
				m, err = SelectMap(d, fields)
			}
			if err != nil && err.Error() != "fields cannot be empty" {
				return nil, false, fmt.Errorf("fail to decode data %s after applying dataTemplate for error %v", string(bs), err)
			} else if err == nil {
				d = m
				selected = true
			}
		}

		switch format {
		case message.FormatJson:
			if transformed && !selected {
				return bs, true, nil
			}
			outBytes, err := c.Encode(d)
			return outBytes, transformed || selected, err
		case message.FormatProtobuf, message.FormatCustom, message.FormatDelimited:
			if transformed && !selected {
				m := make(map[string]interface{})
				err := json.Unmarshal(bs, &m)
				if err != nil {
					return nil, false, fmt.Errorf("fail to decode data %s after applying dataTemplate for error %v", string(bs), err)
				}
				d = m
			}
			outBytes, err := c.Encode(d)
			return outBytes, transformed || selected, err
		default: // should not happen
			return nil, false, fmt.Errorf("unsupported format %v", format)
		}
	}, nil
}

func GenTp(dt string) (*template.Template, error) {
	return template.New("sink").Funcs(conf.FuncMap).Parse(dt)
}

// SelectMap select fields from input map or array of map.
// If you do not need to convert data to []byte, you can use this function directly. Otherwise, use TransFunc.
func SelectMap(input interface{}, fields []string) (interface{}, error) {
	if len(fields) == 0 {
		return input, fmt.Errorf("fields cannot be empty")
	}

	if _, ok := input.([]byte); ok {
		var m map[string]interface{}
		err := json.Unmarshal(input.([]byte), &m)
		if err != nil {
			return input, fmt.Errorf("fail to decode data %s for error %v", string(input.([]byte)), err)
		}
		input = m
	}

	outputs := make([]map[string]interface{}, 0)
	switch input.(type) {
	case map[string]interface{}:
		output := make(map[string]interface{})
		for _, field := range fields {
			output[field] = input.(map[string]interface{})[field]
		}
		return output, nil
	case []interface{}:
		for _, v := range input.([]interface{}) {
			output := make(map[string]interface{})
			if out, ok := v.(map[string]interface{}); !ok {
				return input, fmt.Errorf("unsupported type %v", input)
			} else {
				for _, field := range fields {
					output[field] = out[field]
				}
				outputs = append(outputs, output)
			}
		}
		return outputs, nil
	case []map[string]interface{}:
		for _, v := range input.([]map[string]interface{}) {
			output := make(map[string]interface{})
			for _, field := range fields {
				output[field] = v[field]
			}
			outputs = append(outputs, output)
		}
		return outputs, nil
	default:
		return input, fmt.Errorf("unsupported type %v", input)
	}
}
