package schema

import (
	"path/filepath"
	"testing"

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

// ---- collectProtoFiles tests ----

func TestCollectProtoFiles_SingleFile(t *testing.T) {
	result, err := collectProtoFiles("test/test1.proto")
	require.NoError(t, err)
	assert.Equal(t, []string{"test/test1.proto"}, result)
}

func TestCollectProtoFiles_Directory(t *testing.T) {
	result, err := collectProtoFiles("test/multidir")
	require.NoError(t, err)
	assert.Len(t, result, 2)
	assert.Contains(t, result, filepath.Join("test/multidir", "msg_a.proto"))
	assert.Contains(t, result, filepath.Join("test/multidir", "msg_b.proto"))
}

func TestCollectProtoFiles_EmptyDir(t *testing.T) {
	emptyDir := t.TempDir()
	_, err := collectProtoFiles(emptyDir)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no .proto files found")
}

func TestCollectProtoFiles_NotExist(t *testing.T) {
	_, err := collectProtoFiles("test/nonexistent")
	assert.Error(t, err)
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
