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

package service

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
	// introduce annotations
	_ "google.golang.org/genproto/googleapis/api/annotations"

	kconf "github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/converter/protobuf"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

type descriptor interface {
	GetFunctions() []string
}

type protoDescriptor interface {
	ConvertParamsToMessage(method string, params []interface{}) (*dynamic.Message, error)
	ConvertReturnMessage(method string, returnVal *dynamic.Message) (interface{}, error)
	MethodDescriptor(method string) *desc.MethodDescriptor
	MessageFactory() *dynamic.MessageFactory
}

type jsonDescriptor interface {
	ConvertParamsToJson(method string, params []interface{}) ([]byte, error)
	ConvertReturnJson(method string, returnVal []byte) (interface{}, error)
}

type textDescriptor interface {
	ConvertParamsToText(method string, params []interface{}) ([]byte, error)
	ConvertReturnText(method string, returnVal []byte) (interface{}, error)
}

type interfaceDescriptor interface {
	ConvertParams(method string, params []interface{}) ([]interface{}, error)
	ConvertReturn(method string, returnVal interface{}) (interface{}, error)
}

type multiplexDescriptor interface {
	jsonDescriptor
	textDescriptor
	interfaceDescriptor
	httpMapping
}

var ( // Do not call these directly, use the get methods
	protoParser *protoparse.Parser
	// A buffer of descriptor for schemas
	reg = &sync.Map{}
)

func ProtoParser() *protoparse.Parser {
	once.Do(func() {
		dir := "data/services/schemas/"
		if kconf.IsTesting {
			dir = "service/test/schemas/"
		}
		schemaDir, _ := kconf.GetLoc(dir)
		protoParser = &protoparse.Parser{ImportPaths: []string{schemaDir}}
	})
	return protoParser
}

func parse(schema schema, file string) (descriptor, error) {
	info := &schemaInfo{
		SchemaType: schema,
		SchemaFile: file,
	}
	switch schema {
	case PROTOBUFF:
		if v, ok := reg.Load(info); ok {
			return v.(descriptor), nil
		}
		if fds, err := ProtoParser().ParseFiles(file); err != nil {
			return nil, err
		} else {
			result := &wrappedProtoDescriptor{
				FileDescriptor: fds[0],
				mf:             dynamic.NewMessageFactoryWithDefaults(),
				fc:             protobuf.GetFieldConverter(),
			}
			err := result.parseHttpOptions()
			if err != nil {
				return nil, err
			}
			reg.Store(info, result)
			return result, nil
		}
	default:
		return nil, fmt.Errorf("unsupported schema %s", schema)
	}
}

type wrappedProtoDescriptor struct {
	*desc.FileDescriptor
	methodOptions map[string]*httpOptions
	mf            *dynamic.MessageFactory
	fc            *protobuf.FieldConverter
}

// GetFunctions TODO support for duplicate names
func (d *wrappedProtoDescriptor) GetFunctions() (result []string) {
	for _, s := range d.GetServices() {
		for _, m := range s.GetMethods() {
			result = append(result, m.GetName())
		}
	}
	return
}

func (d *wrappedProtoDescriptor) MessageFactory() *dynamic.MessageFactory {
	return d.mf
}

// ConvertParams TODO support optional field, support enum type
// Parameter mapping for protobuf
// 1. If param length is 1, it can either a map contains all field or a field only.
// 2. If param length is more then 1, they will map to message fields in the order
func (d *wrappedProtoDescriptor) ConvertParams(method string, params []interface{}) ([]interface{}, error) {
	m := d.MethodDescriptor(method)
	if m == nil {
		return nil, fmt.Errorf("can't find method %s in proto", method)
	}
	im := m.GetInputType()
	return d.convertParams(im, params)
}

func (d *wrappedProtoDescriptor) ConvertParamsToMessage(method string, params []interface{}) (*dynamic.Message, error) {
	m := d.MethodDescriptor(method)
	if m == nil {
		return nil, fmt.Errorf("can't find method %s in proto", method)
	}
	im := m.GetInputType()
	message := d.mf.NewDynamicMessage(im)
	typedParams, err := d.convertParams(im, params)
	if err != nil {
		return nil, err
	}
	for i, typeParam := range typedParams {
		message.SetFieldByNumber(i+1, typeParam)
	}
	return message, nil
}

func (d *wrappedProtoDescriptor) ConvertParamsToJson(method string, params []interface{}) ([]byte, error) {
	// Deal with encoded json string. Just return the string
	if len(params) == 1 {
		m := d.MethodDescriptor(method)
		if m == nil {
			return nil, fmt.Errorf("can't find method %s in proto", method)
		}
		im := m.GetInputType()
		if im.GetFullyQualifiedName() == protobuf.WrapperString {
			ss, err := cast.ToString(params[0], cast.STRICT)
			if err != nil {
				return nil, err
			}
			return []byte(ss), nil
		}
	}

	if message, err := d.ConvertParamsToMessage(method, params); err != nil {
		return nil, err
	} else {
		return message.MarshalJSON()
	}
}

func (d *wrappedProtoDescriptor) ConvertParamsToText(method string, params []interface{}) ([]byte, error) {
	if message, err := d.ConvertParamsToMessage(method, params); err != nil {
		return nil, err
	} else {
		return message.MarshalText()
	}
}

func (d *wrappedProtoDescriptor) convertParams(im *desc.MessageDescriptor, params []interface{}) ([]interface{}, error) {
	fields := im.GetFields()
	var result []interface{}
	switch len(params) {
	case 0:
		if len(fields) == 0 {
			return result, nil
		} else {
			return nil, fmt.Errorf("require %d parameters but none", len(fields))
		}
	case 1:
		// If it is map, try unfold it
		// TODO custom error for non map or map name not match
		if r, err := d.unfoldMap(im, params[0]); err != nil {
			kconf.Log.Debugf("try unfold param for message %s fail: %v", im.GetName(), err)
		} else {
			return r, nil
		}
		// For non map params, treat it as special case of multiple params
		if len(fields) == 1 {
			param0, err := d.fc.EncodeField(fields[0], params[0])
			if err != nil {
				return nil, err
			}
			return append(result, param0), nil
		} else {
			return nil, fmt.Errorf("require %d parameters but only got 1", len(fields))
		}
	default:
		if len(fields) == len(params) {
			for i, field := range fields {
				param, err := d.fc.EncodeField(field, params[i])
				if err != nil {
					return nil, err
				}
				result = append(result, param)
			}
			return result, nil
		} else {
			return nil, fmt.Errorf("require %d parameters but only got %d", len(fields), len(params))
		}
	}
}

func (d *wrappedProtoDescriptor) ConvertReturn(method string, returnVal interface{}) (interface{}, error) {
	m := d.MethodDescriptor(method)
	t := m.GetOutputType()
	if _, ok := protobuf.WRAPPER_TYPES[t.GetFullyQualifiedName()]; ok {
		return d.fc.DecodeField(returnVal, t.FindFieldByNumber(1), cast.STRICT)
	} else { // MUST be a map
		if retMap, ok := returnVal.(map[string]interface{}); ok {
			return d.fc.DecodeMap(retMap, t, cast.CONVERT_SAMEKIND)
		} else {
			return nil, fmt.Errorf("fail to convert return val, must be a map but got %v", returnVal)
		}
	}
}

func (d *wrappedProtoDescriptor) ConvertReturnMessage(method string, returnVal *dynamic.Message) (interface{}, error) {
	m := d.MethodDescriptor(method)
	return d.fc.DecodeMessage(returnVal, m.GetOutputType()), nil
}

func (d *wrappedProtoDescriptor) ConvertReturnJson(method string, returnVal []byte) (interface{}, error) {
	r := make(map[string]interface{})
	err := json.Unmarshal(returnVal, &r)
	if err != nil {
		return nil, err
	}
	m := d.MethodDescriptor(method)
	return d.fc.DecodeMap(r, m.GetOutputType(), cast.CONVERT_SAMEKIND)
}

func (d *wrappedProtoDescriptor) ConvertReturnText(method string, returnVal []byte) (interface{}, error) {
	m := d.MethodDescriptor(method)
	t := m.GetOutputType()
	if _, ok := protobuf.WRAPPER_TYPES[t.GetFullyQualifiedName()]; ok {
		return d.fc.DecodeField(string(returnVal), t.FindFieldByNumber(1), cast.CONVERT_ALL)
	} else {
		return nil, fmt.Errorf("fail to convert return val to text, return type must be primitive type but got %s", t.GetName())
	}
}

func (d *wrappedProtoDescriptor) MethodDescriptor(name string) *desc.MethodDescriptor {
	var m *desc.MethodDescriptor
	for _, s := range d.GetServices() {
		m = s.FindMethodByName(name)
		if m != nil {
			break
		}
	}
	return m
}

func (d *wrappedProtoDescriptor) unfoldMap(ft *desc.MessageDescriptor, i interface{}) ([]interface{}, error) {
	fields := ft.GetFields()
	result := make([]interface{}, len(fields))
	if m, ok := xsql.ToMessage(i); ok {
		for _, field := range fields {
			v, ok := m.Value(field.GetName(), "")
			if !ok {
				return nil, fmt.Errorf("field %s not found", field.GetName())
			}
			fv, err := d.fc.EncodeField(field, v)
			if err != nil {
				return nil, err
			}
			result[field.GetNumber()-1] = fv
		}
	} else {
		return nil, fmt.Errorf("not a map")
	}
	return result, nil
}
