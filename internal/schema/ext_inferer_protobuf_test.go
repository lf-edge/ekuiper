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

//go:build schema || !core

package schema

import (
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/testx"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestInferProtobuf(t *testing.T) {
	testx.InitEnv()
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
	//Copy init.proto
	bytesRead, err := os.ReadFile("test/test1.proto")
	if err != nil {
		t.Fatal(err)
	}
	err = os.WriteFile(filepath.Join(etcDir, "test1.proto"), bytesRead, 0755)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = os.RemoveAll(etcDir)
		if err != nil {
			t.Fatal(err)
		}
	}()
	err = InitRegistry()
	if err != nil {
		t.Errorf("InitRegistry error: %v", err)
		return
	}
	// Test infer
	result, err := InferProtobuf("test1", "Person")
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
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("InferProtobuf result is not expected, got %v, expected %v", result, expected)
	}
}
