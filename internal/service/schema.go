// Copyright 2021 EMQ Technologies Co., Ltd.
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
	"github.com/golang/protobuf/proto"
	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
	kconf "github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/cast"
	_ "google.golang.org/genproto/googleapis/api/annotations"
	"sync"
)

const (
	wrapperBool   = "google.protobuf.BoolValue"
	wrapperBytes  = "google.protobuf.BytesValue"
	wrapperDouble = "google.protobuf.DoubleValue"
	wrapperFloat  = "google.protobuf.FloatValue"
	wrapperInt32  = "google.protobuf.Int32Value"
	wrapperInt64  = "google.protobuf.Int64Value"
	wrapperString = "google.protobuf.StringValue"
	wrapperUInt32 = "google.protobuf.UInt32Value"
	wrapperUInt64 = "google.protobuf.UInt64Value"
	wrapperVoid   = "google.protobuf.EMPTY"
)

var WRAPPER_TYPES = map[string]struct{}{
	wrapperBool:   {},
	wrapperBytes:  {},
	wrapperDouble: {},
	wrapperFloat:  {},
	wrapperInt32:  {},
	wrapperInt64:  {},
	wrapperString: {},
	wrapperUInt32: {},
	wrapperUInt64: {},
}

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

var ( //Do not call these directly, use the get methods
	protoParser *protoparse.Parser
	// A buffer of descriptor for schemas
	reg = &sync.Map{}
)

func ProtoParser() *protoparse.Parser {
	once.Do(func() {
		dir := "etc/services/schemas/"
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
}

//TODO support for duplicate names
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
		if im.GetFullyQualifiedName() == wrapperString {
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
			param0, err := d.encodeField(fields[0], params[0])
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
				param, err := d.encodeField(field, params[i])
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
	if _, ok := WRAPPER_TYPES[t.GetFullyQualifiedName()]; ok {
		return decodeField(returnVal, t.FindFieldByNumber(1), cast.STRICT)
	} else { // MUST be a map
		if retMap, ok := returnVal.(map[string]interface{}); ok {
			return decodeMap(retMap, t, cast.CONVERT_SAMEKIND)
		} else {
			return nil, fmt.Errorf("fail to convert return val, must be a map but got %v", returnVal)
		}
	}
}

func (d *wrappedProtoDescriptor) ConvertReturnMessage(method string, returnVal *dynamic.Message) (interface{}, error) {
	m := d.MethodDescriptor(method)
	return decodeMessage(returnVal, m.GetOutputType()), nil
}

func (d *wrappedProtoDescriptor) ConvertReturnJson(method string, returnVal []byte) (interface{}, error) {
	r := make(map[string]interface{})
	err := json.Unmarshal(returnVal, &r)
	if err != nil {
		return nil, err
	}
	m := d.MethodDescriptor(method)
	return decodeMap(r, m.GetOutputType(), cast.CONVERT_SAMEKIND)
}

func (d *wrappedProtoDescriptor) ConvertReturnText(method string, returnVal []byte) (interface{}, error) {
	m := d.MethodDescriptor(method)
	t := m.GetOutputType()
	if _, ok := WRAPPER_TYPES[t.GetFullyQualifiedName()]; ok {
		return decodeField(string(returnVal), t.FindFieldByNumber(1), cast.CONVERT_ALL)
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
			fv, err := d.encodeField(field, v)
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

func (d *wrappedProtoDescriptor) encodeMap(im *desc.MessageDescriptor, i interface{}) (*dynamic.Message, error) {
	result := d.mf.NewDynamicMessage(im)
	fields := im.GetFields()
	if m, ok := i.(map[string]interface{}); ok {
		for _, field := range fields {
			v, ok := m[field.GetName()]
			if !ok {
				return nil, fmt.Errorf("field %s not found", field.GetName())
			}
			fv, err := d.encodeField(field, v)
			if err != nil {
				return nil, err
			}
			result.SetFieldByName(field.GetName(), fv)
		}
	}
	return result, nil
}

func (d *wrappedProtoDescriptor) encodeField(field *desc.FieldDescriptor, v interface{}) (interface{}, error) {
	fn := field.GetName()
	ft := field.GetType()
	if field.IsRepeated() {
		var (
			result interface{}
			err    error
		)
		switch ft {
		case dpb.FieldDescriptorProto_TYPE_DOUBLE:
			result, err = cast.ToFloat64Slice(v, cast.STRICT)
		case dpb.FieldDescriptorProto_TYPE_FLOAT:
			result, err = cast.ToTypedSlice(v, func(input interface{}, sn cast.Strictness) (interface{}, error) {
				r, err := cast.ToFloat64(input, sn)
				if err != nil {
					return 0, nil
				} else {
					return float32(r), nil
				}
			}, "float", cast.STRICT)
		case dpb.FieldDescriptorProto_TYPE_INT32, dpb.FieldDescriptorProto_TYPE_SFIXED32, dpb.FieldDescriptorProto_TYPE_SINT32:
			result, err = cast.ToTypedSlice(v, func(input interface{}, sn cast.Strictness) (interface{}, error) {
				r, err := cast.ToInt(input, sn)
				if err != nil {
					return 0, nil
				} else {
					return int32(r), nil
				}
			}, "int", cast.STRICT)
		case dpb.FieldDescriptorProto_TYPE_INT64, dpb.FieldDescriptorProto_TYPE_SFIXED64, dpb.FieldDescriptorProto_TYPE_SINT64:
			result, err = cast.ToInt64Slice(v, cast.STRICT)
		case dpb.FieldDescriptorProto_TYPE_FIXED32, dpb.FieldDescriptorProto_TYPE_UINT32:
			result, err = cast.ToTypedSlice(v, func(input interface{}, sn cast.Strictness) (interface{}, error) {
				r, err := cast.ToUint64(input, sn)
				if err != nil {
					return 0, nil
				} else {
					return uint32(r), nil
				}
			}, "uint", cast.STRICT)
		case dpb.FieldDescriptorProto_TYPE_FIXED64, dpb.FieldDescriptorProto_TYPE_UINT64:
			result, err = cast.ToUint64Slice(v, cast.STRICT)
		case dpb.FieldDescriptorProto_TYPE_BOOL:
			result, err = cast.ToBoolSlice(v, cast.STRICT)
		case dpb.FieldDescriptorProto_TYPE_STRING:
			result, err = cast.ToStringSlice(v, cast.STRICT)
		case dpb.FieldDescriptorProto_TYPE_BYTES:
			result, err = cast.ToBytesSlice(v, cast.STRICT)
		case dpb.FieldDescriptorProto_TYPE_MESSAGE:
			result, err = cast.ToTypedSlice(v, func(input interface{}, sn cast.Strictness) (interface{}, error) {
				r, err := cast.ToStringMap(v)
				if err == nil {
					return d.encodeMap(field.GetMessageType(), r)
				} else {
					return nil, fmt.Errorf("invalid type for map type field '%s': %v", fn, err)
				}
			}, "map", cast.STRICT)
		default:
			return nil, fmt.Errorf("invalid type for field '%s'", fn)
		}
		if err != nil {
			err = fmt.Errorf("failed to encode field '%s':%v", fn, err)
		}
		return result, err
	} else {
		return d.encodeSingleField(field, v)
	}
}

func (d *wrappedProtoDescriptor) encodeSingleField(field *desc.FieldDescriptor, v interface{}) (interface{}, error) {
	fn := field.GetName()
	switch field.GetType() {
	case dpb.FieldDescriptorProto_TYPE_DOUBLE:
		r, err := cast.ToFloat64(v, cast.STRICT)
		if err == nil {
			return r, nil
		} else {
			return nil, fmt.Errorf("invalid type for float type field '%s': %v", fn, err)
		}
	case dpb.FieldDescriptorProto_TYPE_FLOAT:
		r, err := cast.ToFloat64(v, cast.STRICT)
		if err == nil {
			return float32(r), nil
		} else {
			return nil, fmt.Errorf("invalid type for float type field '%s': %v", fn, err)
		}
	case dpb.FieldDescriptorProto_TYPE_INT32, dpb.FieldDescriptorProto_TYPE_SFIXED32, dpb.FieldDescriptorProto_TYPE_SINT32:
		r, err := cast.ToInt(v, cast.STRICT)
		if err == nil {
			return int32(r), nil
		} else {
			return nil, fmt.Errorf("invalid type for int type field '%s': %v", fn, err)
		}
	case dpb.FieldDescriptorProto_TYPE_INT64, dpb.FieldDescriptorProto_TYPE_SFIXED64, dpb.FieldDescriptorProto_TYPE_SINT64:
		r, err := cast.ToInt64(v, cast.STRICT)
		if err == nil {
			return r, nil
		} else {
			return nil, fmt.Errorf("invalid type for int type field '%s': %v", fn, err)
		}
	case dpb.FieldDescriptorProto_TYPE_FIXED32, dpb.FieldDescriptorProto_TYPE_UINT32:
		r, err := cast.ToUint64(v, cast.STRICT)
		if err == nil {
			return uint32(r), nil
		} else {
			return nil, fmt.Errorf("invalid type for uint type field '%s': %v", fn, err)
		}
	case dpb.FieldDescriptorProto_TYPE_FIXED64, dpb.FieldDescriptorProto_TYPE_UINT64:
		r, err := cast.ToUint64(v, cast.STRICT)
		if err == nil {
			return r, nil
		} else {
			return nil, fmt.Errorf("invalid type for uint type field '%s': %v", fn, err)
		}
	case dpb.FieldDescriptorProto_TYPE_BOOL:
		r, err := cast.ToBool(v, cast.STRICT)
		if err == nil {
			return r, nil
		} else {
			return nil, fmt.Errorf("invalid type for bool type field '%s': %v", fn, err)
		}
	case dpb.FieldDescriptorProto_TYPE_STRING:
		r, err := cast.ToString(v, cast.STRICT)
		if err == nil {
			return r, nil
		} else {
			return nil, fmt.Errorf("invalid type for string type field '%s': %v", fn, err)
		}
	case dpb.FieldDescriptorProto_TYPE_BYTES:
		r, err := cast.ToBytes(v, cast.STRICT)
		if err == nil {
			return r, nil
		} else {
			return nil, fmt.Errorf("invalid type for bytes type field '%s': %v", fn, err)
		}
	case dpb.FieldDescriptorProto_TYPE_MESSAGE:
		r, err := cast.ToStringMap(v)
		if err == nil {
			return d.encodeMap(field.GetMessageType(), r)
		} else {
			return nil, fmt.Errorf("invalid type for map type field '%s': %v", fn, err)
		}
	default:
		return nil, fmt.Errorf("invalid type for field '%s'", fn)
	}
}

func decodeMessage(message *dynamic.Message, outputType *desc.MessageDescriptor) interface{} {
	if _, ok := WRAPPER_TYPES[outputType.GetFullyQualifiedName()]; ok {
		return message.GetFieldByNumber(1)
	} else if wrapperVoid == outputType.GetFullyQualifiedName() {
		return nil
	}
	result := make(map[string]interface{})
	for _, field := range outputType.GetFields() {
		decodeMessageField(message.GetField(field), field, result, cast.STRICT)
	}
	return result
}

func decodeMessageField(src interface{}, field *desc.FieldDescriptor, result map[string]interface{}, sn cast.Strictness) error {
	if f, err := decodeField(src, field, sn); err != nil {
		return err
	} else {
		result[field.GetName()] = f
		return nil
	}
}

func decodeField(src interface{}, field *desc.FieldDescriptor, sn cast.Strictness) (interface{}, error) {
	var (
		r interface{}
		e error
	)
	fn := field.GetName()
	switch field.GetType() {
	case dpb.FieldDescriptorProto_TYPE_DOUBLE, dpb.FieldDescriptorProto_TYPE_FLOAT:
		if field.IsRepeated() {
			r, e = cast.ToFloat64Slice(src, sn)
		} else {
			r, e = cast.ToFloat64(src, sn)
		}
	case dpb.FieldDescriptorProto_TYPE_INT32, dpb.FieldDescriptorProto_TYPE_SFIXED32, dpb.FieldDescriptorProto_TYPE_SINT32, dpb.FieldDescriptorProto_TYPE_INT64, dpb.FieldDescriptorProto_TYPE_SFIXED64, dpb.FieldDescriptorProto_TYPE_SINT64, dpb.FieldDescriptorProto_TYPE_FIXED32, dpb.FieldDescriptorProto_TYPE_UINT32, dpb.FieldDescriptorProto_TYPE_FIXED64, dpb.FieldDescriptorProto_TYPE_UINT64:
		if field.IsRepeated() {
			r, e = cast.ToInt64Slice(src, sn)
		} else {
			r, e = cast.ToInt64(src, sn)
		}
	case dpb.FieldDescriptorProto_TYPE_BOOL:
		if field.IsRepeated() {
			r, e = cast.ToBoolSlice(src, sn)
		} else {
			r, e = cast.ToBool(src, sn)
		}
	case dpb.FieldDescriptorProto_TYPE_STRING:
		if field.IsRepeated() {
			r, e = cast.ToStringSlice(src, sn)
		} else {
			r, e = cast.ToString(src, sn)
		}
	case dpb.FieldDescriptorProto_TYPE_BYTES:
		if field.IsRepeated() {
			r, e = cast.ToBytesSlice(src, sn)
		} else {
			r, e = cast.ToBytes(src, sn)
		}
	case dpb.FieldDescriptorProto_TYPE_MESSAGE:
		if field.IsRepeated() {
			r, e = cast.ToTypedSlice(src, func(input interface{}, ssn cast.Strictness) (interface{}, error) {
				return decodeSubMessage(input, field.GetMessageType(), ssn)
			}, "map", sn)
		} else {
			r, e = decodeSubMessage(src, field.GetMessageType(), sn)
		}
	default:
		return nil, fmt.Errorf("unsupported type for %s", fn)
	}
	if e != nil {
		e = fmt.Errorf("invalid type of return value for '%s': %v", fn, e)
	}
	return r, e
}

func decodeMap(src map[string]interface{}, ft *desc.MessageDescriptor, sn cast.Strictness) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	for _, field := range ft.GetFields() {
		val, ok := src[field.GetName()]
		if !ok {
			continue
		}
		err := decodeMessageField(val, field, result, sn)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func decodeSubMessage(input interface{}, ft *desc.MessageDescriptor, sn cast.Strictness) (interface{}, error) {
	var m = map[string]interface{}{}
	switch v := input.(type) {
	case map[interface{}]interface{}:
		for k, val := range v {
			m[cast.ToStringAlways(k)] = val
		}
		return decodeMap(m, ft, sn)
	case map[string]interface{}:
		return decodeMap(v, ft, sn)
	case proto.Message:
		message, err := dynamic.AsDynamicMessage(v)
		if err != nil {
			return nil, err
		}
		return decodeMessage(message, ft), nil
	case *dynamic.Message:
		return decodeMessage(v, ft), nil
	default:
		return nil, fmt.Errorf("cannot decode %[1]T(%[1]v) to map", input)
	}
}
