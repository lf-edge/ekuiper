package schema

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func TestInferProtobuf(t *testing.T) {
	// Move test schema file to etc dir
	etcDir, err := conf.GetDataLoc()
	if err != nil {
		t.Fatal(err)
	}
	etcDir = filepath.Join(etcDir, "schemas", "protobuf")
	err = os.MkdirAll(etcDir, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	// Copy init.proto
	bytesRead, err := os.ReadFile("test/test1.proto")
	if err != nil {
		t.Fatal(err)
	}
	err = os.WriteFile(filepath.Join(etcDir, "test1.proto"), bytesRead, 0o755)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = os.RemoveAll(etcDir)
		if err != nil {
			t.Fatal(err)
		}
	}()
	pt := &PbType{}
	modules.RegisterSchemaType(modules.PROTOBUF, pt)
	err = InitRegistry()
	if err != nil {
		t.Errorf("InitRegistry error: %v", err)
		return
	}
	// Test infer
	result, err := pt.Infer(nil, "test1.Person")
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
	// Move test schema file to etc dir
	etcDir, err := conf.GetDataLoc()
	if err != nil {
		t.Fatal(err)
	}
	etcDir = filepath.Join(etcDir, "schemas", "protobuf")
	err = os.MkdirAll(etcDir, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	// Copy init.proto
	bytesRead, err := os.ReadFile("test/test3.proto")
	if err != nil {
		t.Fatal(err)
	}
	err = os.WriteFile(filepath.Join(etcDir, "test3.proto"), bytesRead, 0o755)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = os.RemoveAll(etcDir)
		if err != nil {
			t.Fatal(err)
		}
	}()
	pt := &PbType{}
	modules.RegisterSchemaType(modules.PROTOBUF, pt)
	err = InitRegistry()
	if err != nil {
		t.Errorf("InitRegistry error: %v", err)
		return
	}
	// Test infer
	result, err := pt.Infer(nil, "test3.DrivingData")
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
