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

type FastJsonConverter struct{}

var fastConverter = &FastJsonConverter{}

func (f *FastJsonConverter) DecodeWithSchema(b []byte, schema map[string]*ast.JsonStreamField) (interface{}, error) {
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
	vs := make([]interface{}, len(array))
	switch field.Type {
	case "bigint", "float":
		for i, item := range array {
			f64, err := item.Float64()
			if err != nil {
				return nil, err
			}
			vs[i] = f64
		}
	case "string", "bytea":
		for i, item := range array {
			s, err := item.StringBytes()
			if err != nil {
				return nil, err
			}
			vs[i] = string(s)
		}
	case "array":
		for i, item := range array {
			childArrays, err := item.Array()
			if err != nil {
				return nil, err
			}
			subList, err := f.decodeArray(childArrays, field.Items)
			if err != nil {
				return nil, err
			}
			vs[i] = subList
		}
	case "struct":
		for i, item := range array {
			childObj, err := item.Object()
			if err != nil {
				return nil, err
			}
			subMap, err := f.decodeObject(childObj, field.Properties)
			if err != nil {
				return nil, err
			}
			vs[i] = subMap
		}
	case "boolean":
		for i, item := range array {
			b, err := item.Bool()
			if err != nil {
				return nil, err
			}
			vs[i] = b
		}
	case "datetime":
		for i, item := range array {
			switch array[i].Type() {
			case fastjson.TypeNumber:
				f64, err := item.Float64()
				if err != nil {
					return nil, err
				}
				vs[i] = f64
			case fastjson.TypeString:
				s, err := item.StringBytes()
				if err != nil {
					return nil, err
				}
				vs[i] = string(s)
			}
		}

	default:
		return nil, fmt.Errorf("unknown filed type:%s", field.Type)
	}
	return vs, nil
}

func (f *FastJsonConverter) decodeObject(obj *fastjson.Object, schema map[string]*ast.JsonStreamField) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	for key, field := range schema {
		switch field.Type {
		case "bigint", "float":
			f64v, err := obj.Get(key).Float64()
			if err != nil {
				return nil, err
			}
			m[key] = f64v
		case "string", "bytea":
			s, err := obj.Get(key).StringBytes()
			if err != nil {
				return nil, err
			}
			m[key] = string(s)
		case "array":
			childArray, err := obj.Get(key).Array()
			if err != nil {
				return nil, err
			}
			subList, err := f.decodeArray(childArray, schema[key].Items)
			if err != nil {
				return nil, err
			}
			m[key] = subList
		case "struct":
			childObj, err := obj.Get(key).Object()
			if err != nil {
				return nil, err
			}
			childMap, err := f.decodeObject(childObj, schema[key].Properties)
			if err != nil {
				return nil, err
			}
			m[key] = childMap
		case "boolean":
			b, err := obj.Get(key).Bool()
			if err != nil {
				return nil, err
			}
			m[key] = b
		case "datetime":
			switch obj.Get(key).Type() {
			case fastjson.TypeString:
				s, err := obj.Get(key).StringBytes()
				if err != nil {
					return nil, err
				}
				m[key] = string(s)
			case fastjson.TypeNumber:
				f64v, err := obj.Get(key).Float64()
				if err != nil {
					return nil, err
				}
				m[key] = f64v
			}
		default:
			return nil, fmt.Errorf("unknown filed type:%s", field.Type)
		}
	}
	return m, nil
}
