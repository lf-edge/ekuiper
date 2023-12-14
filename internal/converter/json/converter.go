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

package json

import (
	"encoding/json"
	"fmt"

	"github.com/valyala/fastjson"

	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/message"
)

type Converter struct{}

var converter = &Converter{}

func GetConverter() (message.Converter, error) {
	return converter, nil
}

func (c *Converter) Encode(d interface{}) ([]byte, error) {
	return json.Marshal(d)
}

func (c *Converter) Decode(b []byte) (interface{}, error) {
	var r0 interface{}
	err := json.Unmarshal(b, &r0)
	if err != nil {
		return nil, err
	}
	return r0, nil
}

type FastJsonConverter struct {
	isSchemaLess bool
	schema       map[string]*ast.JsonStreamField
}

func NewFastJsonConverter(schema map[string]*ast.JsonStreamField, isSchemaLess bool) *FastJsonConverter {
	return &FastJsonConverter{
		isSchemaLess: isSchemaLess,
		schema:       schema,
	}
}

func (c *FastJsonConverter) Encode(d interface{}) ([]byte, error) {
	return json.Marshal(d)
}

func (c *FastJsonConverter) Decode(b []byte) (interface{}, error) {
	return c.decodeWithSchema(b, c.schema)
}

func (f *FastJsonConverter) decodeWithSchema(b []byte, schema map[string]*ast.JsonStreamField) (interface{}, error) {
	var p fastjson.Parser
	v, err := p.ParseBytes(b)
	if err != nil {
		return nil, err
	}
	switch v.Type() {
	case fastjson.TypeArray:
		array, err := v.Array()
		if err != nil {
			return nil, err
		}
		ms := make([]map[string]interface{}, len(array))
		for i, v := range array {
			obj, err := v.Object()
			if err != nil {
				return nil, err
			}
			subMap, err := f.decodeObject(obj, schema)
			if err != nil {
				return nil, err
			}
			ms[i] = subMap
		}
		return ms, nil
	case fastjson.TypeObject:
		obj, err := v.Object()
		if err != nil {
			return nil, err
		}
		m, err := f.decodeObject(obj, schema)
		if err != nil {
			return nil, err
		}
		return m, nil
	}
	return nil, fmt.Errorf("only map[string]interface{} and []map[string]interface{} is supported")
}

func (f *FastJsonConverter) decodeArray(array []*fastjson.Value, field *ast.JsonStreamField) ([]interface{}, error) {
	if !f.isSchemaLess && field == nil {
		return nil, nil
	}
	vs := make([]interface{}, len(array))
	for i, item := range array {
		switch item.Type() {
		case fastjson.TypeNull:
			vs[i] = nil
		case fastjson.TypeObject:
			if field != nil && field.Type != "struct" {
				return nil, fmt.Errorf("array has wrong type:%v, expect:%v", fastjson.TypeObject.String(), field.Type)
			}
			childObj, err := item.Object()
			if err != nil {
				return nil, err
			}
			var props map[string]*ast.JsonStreamField
			if field != nil {
				props = field.Properties
			}
			subMap, err := f.decodeObject(childObj, props)
			if err != nil {
				return nil, err
			}
			vs[i] = subMap
		case fastjson.TypeArray:
			if field != nil && field.Type != "array" {
				return nil, fmt.Errorf("array has wrong type:%v, expect:%v", fastjson.TypeArray.String(), field.Type)
			}
			childArrays, err := item.Array()
			if err != nil {
				return nil, err
			}
			var items *ast.JsonStreamField
			if field != nil {
				items = field.Items
			}
			subList, err := f.decodeArray(childArrays, items)
			if err != nil {
				return nil, err
			}
			vs[i] = subList
		case fastjson.TypeString:
			v, err := extractStringValue("array", item, field)
			if err != nil {
				return nil, err
			}
			vs[i] = v
		case fastjson.TypeNumber:
			v, err := extractNumberValue("array", item, field)
			if err != nil {
				return nil, err
			}
			vs[i] = v
		case fastjson.TypeTrue, fastjson.TypeFalse:
			v, err := extractBooleanFromValue("array", item, field)
			if err != nil {
				return nil, err
			}
			vs[i] = v
		}
	}
	return vs, nil
}

func (f *FastJsonConverter) decodeObject(obj *fastjson.Object, schema map[string]*ast.JsonStreamField) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	for key, field := range schema {
		if obj.Get(key) == nil {
			continue
		}
		if !f.isSchemaLess && field == nil {
			continue
		}

		v := obj.Get(key)
		switch v.Type() {
		case fastjson.TypeNull:
			m[key] = nil
		case fastjson.TypeObject:
			if field == nil || field.Type == "struct" {
				childObj, err := obj.Get(key).Object()
				if err != nil {
					return nil, err
				}
				var props map[string]*ast.JsonStreamField
				if field != nil {
					props = field.Properties
				}
				childMap, err := f.decodeObject(childObj, props)
				if err != nil {
					return nil, err
				}
				m[key] = childMap
			} else {
				return nil, fmt.Errorf("%v has wrong type:%v, expect:%v", key, v.Type().String(), getType(field))
			}
		case fastjson.TypeArray:
			if field == nil || field.Type == "array" {
				childArray, err := obj.Get(key).Array()
				if err != nil {
					return nil, err
				}
				var items *ast.JsonStreamField
				if field != nil {
					items = field.Items
				}
				subList, err := f.decodeArray(childArray, items)
				if err != nil {
					return nil, err
				}
				m[key] = subList
			} else {
				return nil, fmt.Errorf("%v has wrong type:%v, expect:%v", key, v.Type().String(), getType(field))
			}
		case fastjson.TypeString:
			v, err := extractStringValue(key, obj.Get(key), field)
			if err != nil {
				return nil, err
			}
			m[key] = v
		case fastjson.TypeNumber:
			v, err := extractNumberValue(key, obj.Get(key), field)
			if err != nil {
				return nil, err
			}
			m[key] = v
		case fastjson.TypeTrue, fastjson.TypeFalse:
			v, err := extractBooleanFromValue(key, obj.Get(key), field)
			if err != nil {
				return nil, err
			}
			m[key] = v
		}
	}
	return m, nil
}

func extractNumberValue(name string, v *fastjson.Value, field *ast.JsonStreamField) (interface{}, error) {
	switch {
	case field == nil, field.Type == "float", field.Type == "datetime":
		f64, err := v.Float64()
		if err != nil {
			return nil, err
		}
		return f64, nil
	case field.Type == "bigint":
		i64, err := v.Int64()
		if err != nil {
			return nil, err
		}
		return i64, nil
	case field.Type == "string":
		f64, err := v.Float64()
		if err != nil {
			return nil, err
		}
		return cast.ToStringAlways(f64), nil
	case field.Type == "boolean":
		bv, err := getBooleanFromValue(v)
		if err != nil {
			return nil, err
		}
		return bv, nil
	default:
		return nil, fmt.Errorf("%v has wrong type:%v, expect:%v", name, fastjson.TypeNumber.String(), field.Type)
	}
}

func extractStringValue(name string, v *fastjson.Value, field *ast.JsonStreamField) (interface{}, error) {
	switch {
	case field == nil, field.Type == "string", field.Type == "datetime":
		bs, err := v.StringBytes()
		if err != nil {
			return nil, err
		}
		return string(bs), nil
	case field.Type == "bytea":
		s, err := v.StringBytes()
		if err != nil {
			return nil, err
		}
		return cast.ToByteA(string(s), cast.CONVERT_ALL)
	case field.Type == "boolean":
		return getBooleanFromValue(v)
	default:
		return nil, fmt.Errorf("%v has wrong type:%v, expect:%v", name, fastjson.TypeString.String(), field.Type)
	}
}

func extractBooleanFromValue(name string, v *fastjson.Value, field *ast.JsonStreamField) (interface{}, error) {
	if field == nil || field.Type == "boolean" {
		s, err := v.Bool()
		if err != nil {
			return nil, err
		}
		return s, nil
	} else {
		return nil, fmt.Errorf("%v has wrong type:%v, expect:%v", name, v.Type().String(), getType(field))
	}
}

func getBooleanFromValue(value *fastjson.Value) (interface{}, error) {
	typ := value.Type()
	switch typ {
	case fastjson.TypeNumber:
		f64, err := value.Float64()
		if err != nil {
			return false, err
		}
		return cast.ToBool(f64, cast.CONVERT_ALL)
	case fastjson.TypeString:
		s, err := value.StringBytes()
		if err != nil {
			return false, err
		}
		return cast.ToBool(string(s), cast.CONVERT_ALL)
	case fastjson.TypeTrue, fastjson.TypeFalse:
		b, err := value.Bool()
		if err != nil {
			return false, err
		}
		return b, nil
	case fastjson.TypeNull:
		return nil, nil
	}
	return false, fmt.Errorf("wrong type:%v, expect:boolean", typ)
}

func getType(t *ast.JsonStreamField) string {
	if t == nil {
		return "null"
	} else {
		return t.Type
	}
}
