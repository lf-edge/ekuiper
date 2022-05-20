// Copyright 2022 EMQ Technologies Co., Ltd.
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

package protobuf

import (
	"fmt"
	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

var (
	fieldConverterIns = &FieldConverter{}
	mf                = dynamic.NewMessageFactoryWithDefaults()
)

type FieldConverter struct{}

func GetFieldConverter() *FieldConverter {
	return fieldConverterIns
}

func (fc *FieldConverter) encodeMap(im *desc.MessageDescriptor, i interface{}) (*dynamic.Message, error) {
	result := mf.NewDynamicMessage(im)
	fields := im.GetFields()
	if m, ok := i.(map[string]interface{}); ok {
		for _, field := range fields {
			v, ok := m[field.GetName()]
			if !ok {
				return nil, fmt.Errorf("field %s not found", field.GetName())
			}
			fv, err := fc.EncodeField(field, v)
			if err != nil {
				return nil, err
			}
			result.SetFieldByName(field.GetName(), fv)
		}
	}
	return result, nil
}

func (fc *FieldConverter) EncodeField(field *desc.FieldDescriptor, v interface{}) (interface{}, error) {
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
					return fc.encodeMap(field.GetMessageType(), r)
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
		return fc.encodeSingleField(field, v)
	}
}

func (fc *FieldConverter) encodeSingleField(field *desc.FieldDescriptor, v interface{}) (interface{}, error) {
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
			return fc.encodeMap(field.GetMessageType(), r)
		} else {
			return nil, fmt.Errorf("invalid type for map type field '%s': %v", fn, err)
		}
	default:
		return nil, fmt.Errorf("invalid type for field '%s'", fn)
	}
}
