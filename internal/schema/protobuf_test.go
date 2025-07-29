package schema

import (
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
