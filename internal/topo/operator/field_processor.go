// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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

package operator

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

// Only run when strict validation mode is on, fields is defined and is not binary
// Do not convert types
type defaultFieldProcessor struct {
	streamFields    map[string]*ast.JsonStreamField
	timestampFormat string
}

func (p *defaultFieldProcessor) validateAndConvert(tuple *xsql.Tuple) error {
	_, err := p.validateAndConvertMessage(p.streamFields, tuple.Message)
	return err
}

func (p *defaultFieldProcessor) validateAndConvertMessage(schema map[string]*ast.JsonStreamField, message xsql.Message) (map[string]interface{}, error) {
	for name, sf := range schema {
		v, ok := message.Value(name, "")
		if !ok {
			return nil, fmt.Errorf("field %s is not found", name)
		}
		if nv, err := p.validateAndConvertField(sf, v); err != nil {
			return nil, fmt.Errorf("field %s type mismatch: %v", name, err)
		} else {
			message[name] = nv
		}
	}
	return message, nil
}

// Validate and convert field value to the type defined in schema
func (p *defaultFieldProcessor) validateAndConvertField(sf *ast.JsonStreamField, t interface{}) (interface{}, error) {
	v := reflect.ValueOf(t)
	jtype := v.Kind()
	switch sf.Type {
	case (ast.BIGINT).String():
		if jtype == reflect.Int64 {
			return t, nil
		}
		return cast.ToInt64(t, cast.CONVERT_SAMEKIND)
	case (ast.FLOAT).String():
		if jtype == reflect.Float64 {
			return t, nil
		}
		return cast.ToFloat64(t, cast.CONVERT_SAMEKIND)
	case (ast.BOOLEAN).String():
		if jtype == reflect.Bool {
			return t, nil
		}
		return cast.ToBool(t, cast.CONVERT_SAMEKIND)
	case (ast.STRINGS).String():
		if jtype == reflect.String {
			return t, nil
		}
		return cast.ToString(t, cast.CONVERT_SAMEKIND)
	case (ast.DATETIME).String():
		return cast.InterfaceToTime(t, p.timestampFormat)
	case (ast.BYTEA).String():
		return cast.ToByteA(t, cast.CONVERT_SAMEKIND)
	case (ast.ARRAY).String():
		if t == nil {
			return []interface{}(nil), nil
		} else if jtype == reflect.Slice {
			a, ok := t.([]interface{})
			if !ok {
				return nil, fmt.Errorf("cannot convert %v to []interface{}", t)
			}
			for i, e := range a {
				ne, err := p.validateAndConvertField(sf.Items, e)
				if err != nil {
					return nil, fmt.Errorf("array element type mismatch: %v", err)
				}
				if ne != nil {
					a[i] = ne
				}
			}
			return a, nil
		} else {
			return nil, fmt.Errorf("expect array but got %v", t)
		}
	case (ast.STRUCT).String():
		var (
			nextJ map[string]interface{}
			ok    bool
		)
		if t == nil {
			return map[string]interface{}(nil), nil
		} else if jtype == reflect.Map {
			nextJ, ok = t.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("expect map but found %[1]T(%[1]v)", t)
			}
		} else if jtype == reflect.String {
			err := json.Unmarshal([]byte(t.(string)), &nextJ)
			if err != nil {
				return nil, fmt.Errorf("invalid data type for %s, expect map but found %[1]T(%[1]v)", t)
			}
		} else {
			return nil, fmt.Errorf("expect struct but found %[1]T(%[1]v)", t)
		}
		return p.validateAndConvertMessage(sf.Properties, nextJ)
	default:
		return nil, fmt.Errorf("unsupported type %s", sf.Type)
	}
}

func (p *defaultFieldProcessor) parseTime(s string) (time.Time, error) {
	if p.timestampFormat != "" {
		return cast.ParseTime(s, p.timestampFormat)
	} else {
		return time.Parse(cast.JSISO, s)
	}
}
