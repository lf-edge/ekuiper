package schema

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jhump/protoreflect/desc/protoparse" //nolint:staticcheck
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

func TestInferProtobuf(t *testing.T) {
	pt := &PbType{}
	// Test infer
	result, err := pt.Infer(nil, "test/test1.proto", "Person")
	if err != nil {
		t.Errorf("InferProtobuf error: %v", err)
		return
	}
	expected := ast.StreamFields{
		{Name: "name", FieldType: &ast.BasicType{Type: ast.STRINGS}},
		{Name: "id", FieldType: &ast.BasicType{Type: ast.BIGINT}},
		{Name: "email", FieldType: &ast.BasicType{Type: ast.STRINGS}},
		{Name: "code", FieldType: &ast.ArrayType{
			Type: ast.STRUCT,
			FieldType: &ast.RecType{StreamFields: []ast.StreamField{
				{Name: "doubles", FieldType: &ast.ArrayType{Type: ast.FLOAT}},
			}},
		}},
	}
	require.Equal(t, expected, result)
}

func TestInferProtobufWithEmbedType(t *testing.T) {
	pt := &PbType{}
	// Test infer
	result, err := pt.Infer(nil, "test/test3.proto", "DrivingData")
	if err != nil {
		t.Errorf("InferProtobuf error: %v", err)
		return
	}
	expected := ast.StreamFields{
		{Name: "drvg_mod", FieldType: &ast.BasicType{Type: ast.BIGINT}},
		{Name: "average_speed", FieldType: &ast.BasicType{Type: ast.FLOAT}},
		{Name: "brk_pedal_sts", FieldType: &ast.RecType{StreamFields: []ast.StreamField{
			{Name: "valid", FieldType: &ast.BasicType{Type: ast.BIGINT}},
		}}},
		{Name: "drvg_mod_history", FieldType: &ast.ArrayType{Type: ast.BIGINT}},
	}
	if !assert.Equal(t, expected, result) {
		t.Errorf("InferProtobuf result is not expected, got %v, expected %v", result, expected)
	}
}

// ---- Directory-based Scan tests ----

type mockLogger struct{}

func (m *mockLogger) Debug(args ...interface{})                 {}
func (m *mockLogger) Info(args ...interface{})                  {}
func (m *mockLogger) Warn(args ...interface{})                  {}
func (m *mockLogger) Error(args ...interface{})                 {}
func (m *mockLogger) Debugln(args ...interface{})               {}
func (m *mockLogger) Infoln(args ...interface{})                {}
func (m *mockLogger) Warnln(args ...interface{})                {}
func (m *mockLogger) Errorln(args ...interface{})               {}
func (m *mockLogger) Debugf(format string, args ...interface{}) {}
func (m *mockLogger) Infof(format string, args ...interface{})  {}
func (m *mockLogger) Warnf(format string, args ...interface{})  {}
func (m *mockLogger) Errorf(format string, args ...interface{}) {}

func TestScan_WithSubdirectory(t *testing.T) {
	pt := &PbType{}
	schemas, err := pt.Scan(&mockLogger{}, "test")
	require.NoError(t, err)
	// Should find "multidir" as a subdirectory-based schema ID
	assert.Contains(t, schemas, "multidir")
	assert.NotEmpty(t, schemas["multidir"].SchemaFile)
	// Should also find regular .proto files
	assert.Contains(t, schemas, "test1")
}

// ---- Directory-based Infer tests ----

func TestInfer_FromDirectory(t *testing.T) {
	pt := &PbType{}
	// Infer a message from directory containing multiple proto files
	result, err := pt.Infer(nil, "test/multidir", "SensorData")
	require.NoError(t, err)
	expected := ast.StreamFields{
		{Name: "temperature", FieldType: &ast.BasicType{Type: ast.FLOAT}},
		{Name: "humidity", FieldType: &ast.BasicType{Type: ast.BIGINT}},
	}
	assert.Equal(t, expected, result)
}

func TestInfer_FromDirectory_SecondFile(t *testing.T) {
	pt := &PbType{}
	// Infer a message defined in the second proto file
	result, err := pt.Infer(nil, "test/multidir", "VehicleStatus")
	require.NoError(t, err)
	// VehicleStatus has: speed (int32), vin (string), battery (BatteryInfo)
	assert.Len(t, result, 3)
	assert.Equal(t, "speed", result[0].Name)
	assert.Equal(t, "vin", result[1].Name)
	assert.Equal(t, "battery", result[2].Name)
}

func TestInfer_FromDirectory_MessageNotFound(t *testing.T) {
	pt := &PbType{}
	_, err := pt.Infer(nil, "test/multidir", "NonExistentMessage")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestInfer_AbsolutePaths(t *testing.T) {
	root := createAbsolutePathTestSchemas(t)
	originalParser := protoParser
	protoParser = &protoparse.Parser{ImportPaths: []string{root}}
	t.Cleanup(func() { protoParser = originalParser })

	tests := []struct {
		name        string
		schemaPath  string
		messageName string
	}{
		{
			name:        "single file",
			schemaPath:  filepath.Join(root, "single.proto"),
			messageName: "SingleMessage",
		},
		{
			name:        "directory",
			schemaPath:  filepath.Join(root, "bundle"),
			messageName: "SecondMessage",
		},
		{
			name:        "proto import",
			schemaPath:  filepath.Join(root, "importing.proto"),
			messageName: "ImportingMessage",
		},
	}
	pt := &PbType{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields, err := pt.Infer(nil, tt.schemaPath, tt.messageName)
			require.NoError(t, err)
			require.Len(t, fields, 1)
		})
	}
}

func createAbsolutePathTestSchemas(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "bundle"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "support"), 0o755))
	files := map[string]string{
		"single.proto":         `syntax = "proto3"; message SingleMessage { string value = 1; }`,
		"bundle/first.proto":   `syntax = "proto3"; message FirstMessage { string value = 1; }`,
		"bundle/second.proto":  `syntax = "proto3"; message SecondMessage { int32 value = 1; }`,
		"support/common.proto": `syntax = "proto3"; message CommonMessage { string value = 1; }`,
		"importing.proto": `syntax = "proto3";
import "support/common.proto";
message ImportingMessage { CommonMessage common = 1; }`,
	}
	for name, content := range files {
		require.NoError(t, os.WriteFile(filepath.Join(root, filepath.FromSlash(name)), []byte(content), 0o600))
	}
	return root
}
