//go:build schema || !core

package schema

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"            //nolint:staticcheck
	"github.com/jhump/protoreflect/desc/protoparse" //nolint:staticcheck
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

var protoParser *protoparse.Parser

func init() {
	etcDir, _ := conf.GetLoc("etc/schemas/protobuf/")
	dataDir, _ := conf.GetLoc("data/schemas/protobuf/")
	protoParser = &protoparse.Parser{ImportPaths: []string{etcDir, dataDir}}
}

type PbType struct{}

func (p *PbType) Scan(logger api.Logger, schemaDir string) (map[string]*modules.Files, error) {
	var newSchemas map[string]*modules.Files
	files, err := os.ReadDir(schemaDir)
	if err != nil {
		return nil, fmt.Errorf("cannot read schema directory: %s", err)
	} else {
		newSchemas = make(map[string]*modules.Files, len(files))
		for _, file := range files {
			// Subdirectory: treat as a single schema ID containing multiple .proto files
			if file.IsDir() {
				schemaId := file.Name()
				ffs, ok := newSchemas[schemaId]
				if !ok {
					ffs = &modules.Files{}
					newSchemas[schemaId] = ffs
				}
				// SchemaFile points to the directory itself
				ffs.SchemaFile = filepath.Join(schemaDir, file.Name())
				logger.Infof("schema directory %s/%s loaded", schemaDir, schemaId)
				continue
			}
			fileName := filepath.Base(file.Name())
			ext := filepath.Ext(fileName)
			schemaId := strings.TrimSuffix(fileName, filepath.Ext(fileName))
			ffs, ok := newSchemas[schemaId]
			if !ok {
				ffs = &modules.Files{}
				newSchemas[schemaId] = ffs
			}
			switch ext {
			case ".so":
				ffs.SoFile = filepath.Join(schemaDir, file.Name())
			case ".proto":
				ffs.SchemaFile = filepath.Join(schemaDir, file.Name())
			default:
				continue
			}
			logger.Infof("schema file %s/%s loaded", schemaDir, schemaId)
		}
	}
	return newSchemas, nil
}

func (p *PbType) Infer(_ api.Logger, filePath string, messageId string) (ast.StreamFields, error) {
	protoFiles, err := collectProtoFiles(filePath)
	if err != nil {
		return nil, fmt.Errorf("collect proto files from %s failed: %s", filePath, err)
	}
	fds, err := protoParser.ParseFiles(protoFiles...)
	if err != nil {
		return nil, fmt.Errorf("parse schema file(s) %s failed: %s", filePath, err)
	}
	for _, fd := range fds {
		messageDescriptor := fd.FindMessage(messageId)
		if messageDescriptor != nil {
			return convertMessage(messageDescriptor)
		}
	}
	return nil, fmt.Errorf("message type %s not found in schema path %s", messageId, filePath)
}

// collectProtoFiles returns a list of .proto file paths for the given path.
// If the path is a directory, it returns dir-relative paths (e.g. "multidir/msg_a.proto").
// If it is a single file, it returns the path as-is.
func collectProtoFiles(path string) ([]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return []string{path}, nil
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var result []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".proto") {
			result = append(result, filepath.Join(path, e.Name()))
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("no .proto files found in directory %s", path)
	}
	return result, nil
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

var _ modules.SchemaTypeDef = &PbType{}
