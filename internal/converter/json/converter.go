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
	"sync"

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
	sync.RWMutex
	// ruleID -> schema
	schemaMap   map[string]map[string]*ast.JsonStreamField
	schema      map[string]*ast.JsonStreamField
	wildcardMap map[string]struct{}
}

func NewFastJsonConverter(key string, schema map[string]*ast.JsonStreamField) *FastJsonConverter {
	f := &FastJsonConverter{
		schemaMap:   make(map[string]map[string]*ast.JsonStreamField),
		schema:      schema,
		wildcardMap: make(map[string]struct{}),
	}
	f.schemaMap[key] = schema
	return f
}

func (c *FastJsonConverter) MergeSchema(key string, newSchema map[string]*ast.JsonStreamField, isWildcard bool) error {
	c.Lock()
	defer c.Unlock()
	_, ok := c.schemaMap[key]
	if ok {
		return nil
	}
	c.schemaMap[key] = newSchema
	if isWildcard {
		c.wildcardMap[key] = struct{}{}
	} else {
		mergedSchema, err := mergeSchema(c.schema, newSchema)
		if err != nil {
			return err
		}
		c.schema = mergedSchema
	}
	return nil
}

func (c *FastJsonConverter) DetachSchema(key string) error {
	var err error
	c.Lock()
	defer c.Unlock()
	_, ok := c.schemaMap[key]
	if ok {
		delete(c.wildcardMap, key)
		delete(c.schemaMap, key)
		newSchema := make(map[string]*ast.JsonStreamField)
		for _, schema := range c.schemaMap {
			newSchema, err = mergeSchema(newSchema, schema)
			if err != nil {
				return err
			}
		}
		c.schema = newSchema
	}
	return nil
}

func mergeSchema(originSchema, newSchema map[string]*ast.JsonStreamField) (map[string]*ast.JsonStreamField, error) {
	resultSchema := make(map[string]*ast.JsonStreamField)
	for ruleID, oldSchemaField := range originSchema {
		resultSchema[ruleID] = oldSchemaField
	}
	for ruleID, newSchemaField := range newSchema {
		oldSchemaField, ok := originSchema[ruleID]
		if ok {
			switch {
			case oldSchemaField != nil && newSchemaField != nil:
				if oldSchemaField.Type != newSchemaField.Type {
					return nil, fmt.Errorf("column field type %v between current[%v] and new[%v] are not equal", ruleID, oldSchemaField.Type, newSchemaField.Type)
				}
				switch oldSchemaField.Type {
				case "struct":
					subResultSchema, err := mergeSchema(oldSchemaField.Properties, newSchemaField.Properties)
					if err != nil {
						return nil, err
					}
					resultSchema[ruleID].Properties = subResultSchema
				case "array":
					if oldSchemaField.Items.Type != newSchemaField.Items.Type {
						return nil, fmt.Errorf("array column field type %v between current[%v] and new[%v] are not equal", ruleID, oldSchemaField.Items.Type, newSchemaField.Items.Type)
					}
					if oldSchemaField.Items.Type == "struct" {
						subResultSchema, err := mergeSchema(oldSchemaField.Items.Properties, newSchemaField.Items.Properties)
						if err != nil {
							return nil, err
						}
						resultSchema[ruleID].Items.Properties = subResultSchema
					}
				}
			case oldSchemaField != nil && newSchemaField == nil:
				return nil, fmt.Errorf("array column field type %v between current[%v] and new[%v] are not equal", ruleID, oldSchemaField.Items.Type, "any")
			case oldSchemaField == nil && newSchemaField != nil:
				return nil, fmt.Errorf("array column field type %v between current[%v] and new[%v] are not equal", ruleID, "any", newSchemaField.Items.Type)
			case oldSchemaField == nil && newSchemaField == nil:
			}
			continue
		}
		resultSchema[ruleID] = newSchemaField
	}
	return resultSchema, nil
}

func (c *FastJsonConverter) Encode(d interface{}) ([]byte, error) {
	return json.Marshal(d)
}

func (c *FastJsonConverter) Decode(b []byte) (interface{}, error) {
	c.RLock()
	defer c.RUnlock()
	if len(c.wildcardMap) > 0 {
		return converter.Decode(b)
	}
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
	vs := make([]interface{}, len(array))
	switch field.Type {
	case "bigint":
		for i, item := range array {
			typ := item.Type()
			switch typ {
			case fastjson.TypeNumber:
				i64, err := item.Int64()
				if err != nil {
					return nil, err
				}
				vs[i] = i64
			case fastjson.TypeNull:
				vs[i] = nil
			default:
				return nil, fmt.Errorf("array has wrong type:%v, expect:%v", typ.String(), field.Type)
			}
		}
	case "float":
		for i, item := range array {
			typ := item.Type()
			switch typ {
			case fastjson.TypeNumber:
				f64, err := item.Float64()
				if err != nil {
					return nil, err
				}
				vs[i] = f64
			case fastjson.TypeNull:
				vs[i] = nil
			default:
				return nil, fmt.Errorf("array has wrong type:%v, expect:%v", typ.String(), field.Type)
			}
		}
	case "string":
		for i, item := range array {
			typ := item.Type()
			switch typ {
			case fastjson.TypeString:
				s, err := item.StringBytes()
				if err != nil {
					return nil, err
				}
				vs[i] = string(s)
			case fastjson.TypeNumber:
				f64, err := item.Float64()
				if err != nil {
					return nil, err
				}
				vs[i] = cast.ToStringAlways(f64)
			case fastjson.TypeNull:
				vs[i] = nil
			default:
				return nil, fmt.Errorf("array has wrong type:%v, expect:%v", typ.String(), field.Type)
			}
		}
	case "bytea":
		for i, item := range array {
			typ := item.Type()
			switch typ {
			case fastjson.TypeString:
				s, err := item.StringBytes()
				if err != nil {
					return nil, err
				}
				vs[i], err = cast.ToByteA(string(s), cast.CONVERT_ALL)
				if err != nil {
					return nil, err
				}
			case fastjson.TypeNull:
				vs[i] = nil
			}
		}
	case "array":
		for i, item := range array {
			typ := item.Type()
			switch typ {
			case fastjson.TypeArray:
				childArrays, err := item.Array()
				if err != nil {
					return nil, err
				}
				subList, err := f.decodeArray(childArrays, field.Items)
				if err != nil {
					return nil, err
				}
				vs[i] = subList
			case fastjson.TypeNull:
				vs[i] = nil
			default:
				return nil, fmt.Errorf("array has wrong type:%v, expect:%v", typ.String(), field.Type)
			}
		}
	case "struct":
		for i, item := range array {
			typ := item.Type()
			switch typ {
			case fastjson.TypeObject:
				childObj, err := item.Object()
				if err != nil {
					return nil, err
				}
				subMap, err := f.decodeObject(childObj, field.Properties)
				if err != nil {
					return nil, err
				}
				vs[i] = subMap
			case fastjson.TypeNull:
				vs[i] = nil
			default:
				return nil, fmt.Errorf("array has wrong type:%v, expect:%v", typ.String(), field.Type)
			}
		}
	case "boolean":
		for i, item := range array {
			b, err := getBooleanFromValue(item)
			if err != nil {
				return nil, fmt.Errorf("parse array failed, err:%v", err)
			}
			vs[i] = b
		}
	case "datetime":
		for i, item := range array {
			typ := item.Type()
			switch typ {
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
			case fastjson.TypeNull:
				vs[i] = nil
			default:
				return nil, fmt.Errorf("array has wrong type:%v, expect:%v", typ.String(), field.Type)
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
		if obj.Get(key) == nil {
			continue
		}
		v := obj.Get(key)
		switch field.Type {
		case "bigint":
			typ := obj.Get(key).Type()
			switch typ {
			case fastjson.TypeNumber:
				i64, err := obj.Get(key).Int64()
				if err != nil {
					return nil, err
				}
				m[key] = i64
			case fastjson.TypeNull:
				m[key] = nil
			default:
				return nil, fmt.Errorf("%v has wrong type:%v, expect:%v", key, typ.String(), field.Type)
			}
		case "float":
			typ := obj.Get(key).Type()
			switch typ {
			case fastjson.TypeNumber:
				f64v, err := obj.Get(key).Float64()
				if err != nil {
					return nil, err
				}
				m[key] = f64v
			case fastjson.TypeNull:
				m[key] = nil
			default:
				return nil, fmt.Errorf("%v has wrong type:%v, expect:%v", key, typ.String(), field.Type)
			}
		case "string":
			typ := obj.Get(key).Type()
			switch typ {
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
			case fastjson.TypeNull:
				m[key] = nil
			default:
				return nil, fmt.Errorf("%v has wrong type:%v, expect:%v", key, typ.String(), field.Type)
			}
		case "bytea":
			typ := obj.Get(key).Type()
			switch typ {
			case fastjson.TypeString:
				s, err := obj.Get(key).StringBytes()
				if err != nil {
					return nil, err
				}
				m[key], err = cast.ToByteA(string(s), cast.CONVERT_ALL)
				if err != nil {
					return nil, err
				}
			case fastjson.TypeNull:
				m[key] = nil
			default:
				return nil, fmt.Errorf("%v has wrong type:%v, expect:%v", key, typ.String(), field.Type)
			}
		case "array":
			typ := obj.Get(key).Type()
			switch typ {
			case fastjson.TypeArray:
				childArray, err := obj.Get(key).Array()
				if err != nil {
					return nil, err
				}
				subList, err := f.decodeArray(childArray, schema[key].Items)
				if err != nil {
					return nil, err
				}
				m[key] = subList
			case fastjson.TypeNull:
				m[key] = nil
			default:
				return nil, fmt.Errorf("%v has wrong type:%v, expect:%v", key, typ.String(), field.Type)
			}
		case "struct":
			typ := obj.Get(key).Type()
			switch typ {
			case fastjson.TypeObject:
				childObj, err := obj.Get(key).Object()
				if err != nil {
					return nil, err
				}
				childMap, err := f.decodeObject(childObj, schema[key].Properties)
				if err != nil {
					return nil, err
				}
				m[key] = childMap
			case fastjson.TypeNull:
				m[key] = nil
			default:
				return nil, fmt.Errorf("%v has wrong type:%v, expect:%v", key, typ.String(), field.Type)
			}
		case "boolean":
			b, err := getBooleanFromValue(v)
			if err != nil {
				return nil, fmt.Errorf("parse %v failed, err:%v", key, err)
			}
			m[key] = b
		case "datetime":
			typ := obj.Get(key).Type()
			switch typ {
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
			case fastjson.TypeNull:
				m[key] = nil
			default:
				return nil, fmt.Errorf("%v has wrong type:%v, expect:%v", key, typ.String(), field.Type)
			}
		default:
			return nil, fmt.Errorf("unknown filed type:%s", field.Type)
		}
	}
	return m, nil
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
