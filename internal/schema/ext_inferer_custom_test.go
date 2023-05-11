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

package schema

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/testx"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

func init() {
	testx.InitEnv()
}

func TestInferCustom(t *testing.T) {
	// Prepare test schema file
	dataDir, err := conf.GetDataLoc()
	if err != nil {
		t.Fatal(err)
	}
	etcDir := filepath.Join(dataDir, "schemas", "custom")
	err = os.MkdirAll(etcDir, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = os.RemoveAll(etcDir)
		if err != nil {
			t.Fatal(err)
		}
	}()
	// build the so file into data/test prior to running the test
	// Copy the helloworld.so
	bytesRead, err := os.ReadFile(filepath.Join(dataDir, "myFormat.so"))
	if err != nil {
		t.Fatal(err)
	}
	err = os.WriteFile(filepath.Join(etcDir, "myFormat.so"), bytesRead, 0o755)
	if err != nil {
		t.Fatal(err)
	}
	InitRegistry()

	// Test
	result, err := InferCustom("myFormat", "Sample")
	if err != nil {
		t.Errorf("Infer custom format error: %v", err)
		return
	}
	expected := ast.StreamFields{
		{Name: "id", FieldType: &ast.BasicType{Type: ast.BIGINT}},
		{Name: "name", FieldType: &ast.BasicType{Type: ast.STRINGS}},
		{Name: "age", FieldType: &ast.BasicType{Type: ast.BIGINT}},
		{Name: "hobbies", FieldType: &ast.RecType{
			StreamFields: []ast.StreamField{
				{Name: "indoor", FieldType: &ast.ArrayType{Type: ast.STRINGS}},
				{Name: "outdoor", FieldType: &ast.ArrayType{Type: ast.STRINGS}},
			},
		}},
	}
	if len(result) != len(expected) {
		t.Errorf("InferProtobuf result is not expected, got %v, expected %v", result, expected)
	}
}
