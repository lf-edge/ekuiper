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

//go:build schema || !core

package schema

import (
	"fmt"

	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"

	"github.com/lf-edge/ekuiper/internal/pkg/def"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/message"
)

var protoParser *protoparse.Parser

func init() {
	inferes[message.FormatProtobuf] = InferProtobuf
	protoParser = &protoparse.Parser{}
}

// InferProtobuf infers the schema from a protobuf file dynamically in case the schema file changed
func InferProtobuf(schemaFile string, messageName string) (ast.StreamFields, error) {
	ffs, err := GetSchemaFile(def.PROTOBUF, schemaFile)
	if err != nil {
		return nil, err
	}
	if fds, err := protoParser.ParseFiles(ffs.SchemaFile); err != nil {
		return nil, fmt.Errorf("parse schema file %s failed: %s", ffs.SchemaFile, err)
	} else {
		messageDescriptor := fds[0].FindMessage(messageName)
		if messageDescriptor == nil {
			return nil, fmt.Errorf("message type %s not found in schema file %s", messageName, schemaFile)
		}
		return convertMessage(messageDescriptor)
	}
}

func convertMessage(m *desc.MessageDescriptor) (ast.StreamFields, error) {
	mfs := m.GetFields()
	result := make(ast.StreamFields, 0, len(mfs))
	for _, f := range mfs {
		ff, err := convertField(f)
		if err != nil {
			return nil, err
		}
		result = append(result, ff)
	}
	return result, nil
}

func convertField(f *desc.FieldDescriptor) (ast.StreamField, error) {
	ff := ast.StreamField{
		Name: f.GetName(),
	}
	var (
		ft  ast.FieldType
		err error
	)
	ft, err = convertFieldType(f.GetType(), f)
	if err != nil {
		return ff, err
	}
	if f.IsRepeated() {
		switch t := ft.(type) {
		case *ast.BasicType:
			ft = &ast.ArrayType{
				Type: t.Type,
			}
		case *ast.RecType:
			ft = &ast.ArrayType{
				Type:      ast.STRUCT,
				FieldType: t,
			}
		case *ast.ArrayType:
			ft = &ast.ArrayType{
				Type:      ast.ARRAY,
				FieldType: t,
			}
		}
	}
	ff.FieldType = ft
	return ff, nil
}

func convertFieldType(tt dpb.FieldDescriptorProto_Type, f *desc.FieldDescriptor) (ast.FieldType, error) {
	var ft ast.FieldType
	switch tt {
	case dpb.FieldDescriptorProto_TYPE_DOUBLE,
		dpb.FieldDescriptorProto_TYPE_FLOAT:
		ft = &ast.BasicType{Type: ast.FLOAT}
	case dpb.FieldDescriptorProto_TYPE_INT32, dpb.FieldDescriptorProto_TYPE_SFIXED32, dpb.FieldDescriptorProto_TYPE_SINT32,
		dpb.FieldDescriptorProto_TYPE_INT64, dpb.FieldDescriptorProto_TYPE_SFIXED64, dpb.FieldDescriptorProto_TYPE_SINT64,
		dpb.FieldDescriptorProto_TYPE_FIXED32, dpb.FieldDescriptorProto_TYPE_UINT32,
		dpb.FieldDescriptorProto_TYPE_FIXED64, dpb.FieldDescriptorProto_TYPE_UINT64,
		dpb.FieldDescriptorProto_TYPE_ENUM:
		ft = &ast.BasicType{Type: ast.BIGINT}
	case dpb.FieldDescriptorProto_TYPE_BOOL:
		ft = &ast.BasicType{Type: ast.BOOLEAN}
	case dpb.FieldDescriptorProto_TYPE_STRING:
		ft = &ast.BasicType{Type: ast.STRINGS}
	case dpb.FieldDescriptorProto_TYPE_BYTES:
		ft = &ast.BasicType{Type: ast.BYTEA}
	case dpb.FieldDescriptorProto_TYPE_MESSAGE:
		sfs, err := convertMessage(f.GetMessageType())
		if err != nil {
			return nil, fmt.Errorf("invalid struct field type: %v", err)
		}
		ft = &ast.RecType{StreamFields: sfs}
	default:
		return nil, fmt.Errorf("invalid type for field '%s'", f.GetName())
	}
	return ft, nil
}
