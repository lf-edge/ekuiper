package services

import (
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/common"
	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
	"sync"
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

type interfaceDescriptor interface {
	ConvertParams(method string, params []interface{}) ([]interface{}, error)
	ConvertReturn(method string, returnVal interface{}) (interface{}, error)
}

var ( //Do not call these directly, use the get methods
	protoParser *protoparse.Parser
	// A buffer of descriptor for schemas
	reg = &sync.Map{}
)

func ProtoParser() *protoparse.Parser {
	once.Do(func() {
		protoParser = &protoparse.Parser{}
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
				methods:        make(map[string]*desc.MethodDescriptor),
				mf:             dynamic.NewMessageFactoryWithDefaults(),
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
	methods map[string]*desc.MethodDescriptor
	mf      *dynamic.MessageFactory
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
	return convertParams(im, params)
}

func (d *wrappedProtoDescriptor) ConvertParamsToMessage(method string, params []interface{}) (*dynamic.Message, error) {
	m := d.MethodDescriptor(method)
	if m == nil {
		return nil, fmt.Errorf("can't find method %s in proto", method)
	}
	im := m.GetInputType()
	message := d.mf.NewDynamicMessage(im)
	typedParams, err := convertParams(im, params)
	if err != nil {
		return nil, err
	}
	for i, typeParam := range typedParams {
		message.SetFieldByNumber(i+1, typeParam)
	}
	return message, nil
}

func (d *wrappedProtoDescriptor) ConvertParamsToJson(method string, params []interface{}) ([]byte, error) {
	if message, err := d.ConvertParamsToMessage(method, params); err != nil {
		return nil, err
	} else {
		return message.MarshalJSON()
	}
}

func convertParams(im *desc.MessageDescriptor, params []interface{}) ([]interface{}, error) {
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
		if r, err := unfoldMap(im, params[0]); err != nil {
			common.Log.Debugf("try unfold param for message %s fail: %v", im.GetName(), err)
		} else {
			return r, nil
		}
		// For non map params, treat it as special case of multiple params
		if len(fields) == 1 {
			param0, err := encodeField(fields[0], params[0])
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
				param, err := encodeField(field, params[i])
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
	// TODO map support for msgpack?
	return decodeField(returnVal, m.GetOutputType().FindFieldByNumber(1))
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
	return decodeMap(r, m.GetOutputType())
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

func unfoldMap(ft *desc.MessageDescriptor, i interface{}) ([]interface{}, error) {
	fields := ft.GetFields()
	result := make([]interface{}, len(fields))
	if m, ok := i.(map[string]interface{}); ok {
		for _, field := range fields {
			v, ok := m[field.GetName()]
			if !ok {
				return nil, fmt.Errorf("field %s not found", field.GetName())
			}
			fv, err := encodeField(field, v)
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

func encodeMap(fields []*desc.FieldDescriptor, i interface{}) (map[string]interface{}, error) {
	var result map[string]interface{}
	if m, ok := i.(map[string]interface{}); ok && len(m) == len(fields) {
		for _, field := range fields {
			v, ok := m[field.GetName()]
			if !ok {
				return nil, fmt.Errorf("field %s not found", field.GetName())
			}
			fv, err := encodeField(field, v)
			if err != nil {
				return nil, err
			}
			result[field.GetName()] = fv
		}
	}
	return result, nil
}

func encodeField(field *desc.FieldDescriptor, v interface{}) (interface{}, error) {
	fn := field.GetName()
	ft := field.GetType()
	if field.IsRepeated() {
		var (
			result interface{}
			err    error
		)
		switch ft {
		case dpb.FieldDescriptorProto_TYPE_DOUBLE:
			result, err = common.ToFloat64Slice(v, false)
		case dpb.FieldDescriptorProto_TYPE_FLOAT:
			result, err = common.ToTypedSlice(v, func(input interface{}, unstrict bool) (interface{}, error) {
				r, err := common.ToFloat64(input, unstrict)
				if err != nil {
					return 0, nil
				} else {
					return float32(r), nil
				}
			}, "float", false)
		case dpb.FieldDescriptorProto_TYPE_INT32, dpb.FieldDescriptorProto_TYPE_SFIXED32, dpb.FieldDescriptorProto_TYPE_SINT32:
			result, err = common.ToTypedSlice(v, func(input interface{}, unstrict bool) (interface{}, error) {
				r, err := common.ToInt(input, unstrict)
				if err != nil {
					return 0, nil
				} else {
					return int32(r), nil
				}
			}, "int", false)
		case dpb.FieldDescriptorProto_TYPE_INT64, dpb.FieldDescriptorProto_TYPE_SFIXED64, dpb.FieldDescriptorProto_TYPE_SINT64:
			result, err = common.ToInt64Slice(v, false)
		case dpb.FieldDescriptorProto_TYPE_FIXED32, dpb.FieldDescriptorProto_TYPE_UINT32:
			result, err = common.ToTypedSlice(v, func(input interface{}, unstrict bool) (interface{}, error) {
				r, err := common.ToUint64(input, unstrict)
				if err != nil {
					return 0, nil
				} else {
					return uint32(r), nil
				}
			}, "uint", false)
		case dpb.FieldDescriptorProto_TYPE_FIXED64, dpb.FieldDescriptorProto_TYPE_UINT64:
			result, err = common.ToUint64Slice(v, false)
		case dpb.FieldDescriptorProto_TYPE_BOOL:
			result, err = common.ToBoolSlice(v, false)
		case dpb.FieldDescriptorProto_TYPE_STRING:
			result, err = common.ToStringSlice(v, false)
		case dpb.FieldDescriptorProto_TYPE_BYTES:
			result, err = common.ToBytesSlice(v, false)
		case dpb.FieldDescriptorProto_TYPE_MESSAGE:
			result, err = common.ToTypedSlice(v, func(input interface{}, unstrict bool) (interface{}, error) {
				r, err := common.ToStringMap(v)
				if err == nil {
					return encodeMap(field.GetMessageType().GetFields(), r)
				} else {
					return nil, fmt.Errorf("invalid type for map type field '%s': %v", fn, err)
				}
			}, "bool", false)
		default:
			return nil, fmt.Errorf("invalid type for field '%s'", fn)
		}
		if err != nil {
			err = fmt.Errorf("faile to encode field '%s':%v", fn, err)
		}
		return result, err
	} else {
		return encodeSingleField(field, v)
	}
}

func encodeSingleField(field *desc.FieldDescriptor, v interface{}) (interface{}, error) {
	fn := field.GetName()
	switch field.GetType() {
	case dpb.FieldDescriptorProto_TYPE_DOUBLE:
		r, err := common.ToFloat64(v, false)
		if err == nil {
			return r, nil
		} else {
			return nil, fmt.Errorf("invalid type for float type field '%s': %v", fn, err)
		}
	case dpb.FieldDescriptorProto_TYPE_FLOAT:
		r, err := common.ToFloat64(v, false)
		if err == nil {
			return float32(r), nil
		} else {
			return nil, fmt.Errorf("invalid type for float type field '%s': %v", fn, err)
		}
	case dpb.FieldDescriptorProto_TYPE_INT32, dpb.FieldDescriptorProto_TYPE_SFIXED32, dpb.FieldDescriptorProto_TYPE_SINT32:
		r, err := common.ToInt(v, false)
		if err == nil {
			return int32(r), nil
		} else {
			return nil, fmt.Errorf("invalid type for int type field '%s': %v", fn, err)
		}
	case dpb.FieldDescriptorProto_TYPE_INT64, dpb.FieldDescriptorProto_TYPE_SFIXED64, dpb.FieldDescriptorProto_TYPE_SINT64:
		r, err := common.ToInt64(v, false)
		if err == nil {
			return r, nil
		} else {
			return nil, fmt.Errorf("invalid type for int type field '%s': %v", fn, err)
		}
	case dpb.FieldDescriptorProto_TYPE_FIXED32, dpb.FieldDescriptorProto_TYPE_UINT32:
		r, err := common.ToUint64(v, false)
		if err == nil {
			return uint32(r), nil
		} else {
			return nil, fmt.Errorf("invalid type for uint type field '%s': %v", fn, err)
		}
	case dpb.FieldDescriptorProto_TYPE_FIXED64, dpb.FieldDescriptorProto_TYPE_UINT64:
		r, err := common.ToUint64(v, false)
		if err == nil {
			return r, nil
		} else {
			return nil, fmt.Errorf("invalid type for uint type field '%s': %v", fn, err)
		}
	case dpb.FieldDescriptorProto_TYPE_BOOL:
		r, err := common.ToBool(v, false)
		if err == nil {
			return r, nil
		} else {
			return nil, fmt.Errorf("invalid type for bool type field '%s': %v", fn, err)
		}
	case dpb.FieldDescriptorProto_TYPE_STRING:
		r, err := common.ToString(v, false)
		if err == nil {
			return r, nil
		} else {
			return nil, fmt.Errorf("invalid type for string type field '%s': %v", fn, err)
		}
	case dpb.FieldDescriptorProto_TYPE_BYTES:
		r, err := common.ToBytes(v, false)
		if err == nil {
			return r, nil
		} else {
			return nil, fmt.Errorf("invalid type for bytes type field '%s': %v", fn, err)
		}
	case dpb.FieldDescriptorProto_TYPE_MESSAGE:
		r, err := common.ToStringMap(v)
		if err == nil {
			return encodeMap(field.GetMessageType().GetFields(), r)
		} else {
			return nil, fmt.Errorf("invalid type for map type field '%s': %v", fn, err)
		}
	default:
		return nil, fmt.Errorf("invalid type for field '%s'", fn)
	}
}

func decodeMessage(message *dynamic.Message, outputType *desc.MessageDescriptor) interface{} {
	result := make(map[string]interface{})
	for _, field := range outputType.GetFields() {
		decodeMessageField(message.GetField(field), field, result)
	}
	return result
}

func decodeMessageField(src interface{}, field *desc.FieldDescriptor, result map[string]interface{}) error {
	if f, err := decodeField(src, field); err != nil {
		return err
	} else {
		result[field.GetName()] = f
		return nil
	}
}

func decodeField(src interface{}, field *desc.FieldDescriptor) (interface{}, error) {
	var (
		r interface{}
		e error
	)
	fn := field.GetName()
	switch field.GetType() {
	case dpb.FieldDescriptorProto_TYPE_DOUBLE, dpb.FieldDescriptorProto_TYPE_FLOAT:
		if field.IsRepeated() {
			r, e = common.ToFloat64Slice(src, false)
		} else {
			r, e = common.ToFloat64(src, false)
		}
	case dpb.FieldDescriptorProto_TYPE_INT32, dpb.FieldDescriptorProto_TYPE_SFIXED32, dpb.FieldDescriptorProto_TYPE_SINT32, dpb.FieldDescriptorProto_TYPE_INT64, dpb.FieldDescriptorProto_TYPE_SFIXED64, dpb.FieldDescriptorProto_TYPE_SINT64, dpb.FieldDescriptorProto_TYPE_FIXED32, dpb.FieldDescriptorProto_TYPE_UINT32, dpb.FieldDescriptorProto_TYPE_FIXED64, dpb.FieldDescriptorProto_TYPE_UINT64:
		if field.IsRepeated() {
			r, e = common.ToInt64Slice(src, false)
		} else {
			r, e = common.ToInt64(src, false)
		}
	case dpb.FieldDescriptorProto_TYPE_BOOL:
		if field.IsRepeated() {
			r, e = common.ToBoolSlice(src, false)
		} else {
			r, e = common.ToBool(src, false)
		}
	case dpb.FieldDescriptorProto_TYPE_STRING:
		if field.IsRepeated() {
			r, e = common.ToStringSlice(src, false)
		} else {
			r, e = common.ToString(src, false)
		}
	case dpb.FieldDescriptorProto_TYPE_BYTES:
		if field.IsRepeated() {
			r, e = common.ToBytesSlice(src, false)
		} else {
			r, e = common.ToBytes(src, false)
		}
	case dpb.FieldDescriptorProto_TYPE_MESSAGE:
		if field.IsRepeated() {
			r, e = common.ToTypedSlice(src, func(input interface{}, unstrict bool) (interface{}, error) {
				if r, err := common.ToStringMap(input); err != nil {
					return nil, err
				} else {
					return decodeMap(r, field.GetMessageType())
				}
			}, "map", false)
		} else {
			if m, err := common.ToStringMap(src); err != nil {
				r, e = nil, err
			} else {
				r, e = decodeMap(m, field.GetMessageType())
			}
		}
	default:
		return nil, fmt.Errorf("unsupported type for %s", fn)
	}
	if e != nil {
		e = fmt.Errorf("invalid type of return value for '%s': %v", fn, e)
	}
	return r, e
}

func decodeMap(src map[string]interface{}, ft *desc.MessageDescriptor) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	for _, field := range ft.GetFields() {
		val, ok := src[field.GetName()]
		if !ok {
			continue
		}
		err := decodeMessageField(val, field, result)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}
