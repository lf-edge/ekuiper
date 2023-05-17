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
type TransFunc func(interface{}) ([]byte, bool, error)

func GenTransform(dt string, format string, schemaId string, delimiter string, dataField string, fields []string) (TransFunc, error) {
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
	return func(d interface{}) ([]byte, bool, error) {
		var (
			bs          []byte
			transformed bool
			selected    bool
			m           interface{}
			e           error
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

		if transformed {
			m, selected, e = TransItem(bs, dataField, fields)
		} else {
			m, selected, e = TransItem(d, dataField, fields)
		}
		if e != nil {
			return nil, false, fmt.Errorf("fail to TransItem data %v for error %v", d, e)
		}
		if selected {
			d = m
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

// If you do not need to convert data to []byte, you can use this function directly. Otherwise, use TransFunc.
func TransItem(input interface{}, dataField string, fields []string) (interface{}, bool, error) {
	if dataField == "" && len(fields) == 0 {
		return input, false, nil
	}
	if _, ok := input.([]byte); ok {
		var m interface{}
		err := json.Unmarshal(input.([]byte), &m)
		if err != nil {
			return input, false, fmt.Errorf("fail to decode data %s for error %v", string(input.([]byte)), err)
		}
		input = m
	}

	if dataField != "" {
		switch input.(type) {
		case map[string]interface{}:
			input = input.(map[string]interface{})[dataField]
		case []interface{}:
			if len(input.([]interface{})) == 0 {
				return nil, false, nil
			}
			input = input.([]interface{})[0].(map[string]interface{})[dataField]
		case []map[string]interface{}:
			if len(input.([]map[string]interface{})) == 0 {
				return nil, false, nil
			}
			input = input.([]map[string]interface{})[0][dataField]
		default:
			return nil, false, fmt.Errorf("fail to decode data %v", input)
		}
	}

	m, err := selectMap(input, fields)
	if err != nil && err.Error() != "fields cannot be empty" {
		return nil, false, fmt.Errorf("fail to decode data %v for error %v", input, err)
	} else {
		return m, true, nil
	}
}

// selectMap select fields from input map or array of map.
func selectMap(input interface{}, fields []string) (interface{}, error) {
	if len(fields) == 0 {
		return input, fmt.Errorf("fields cannot be empty")
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
