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

package schema

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/testx"
)

func TestRegistry(t *testing.T) {
	testx.InitEnv()
	// Move test schema file to etc dir
	etcDir, err := conf.GetConfLoc()
	if err != nil {
		t.Fatal(err)
	}
	etcDir = filepath.Join(etcDir, "schemas", "protobuf")
	err = os.MkdirAll(etcDir, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	//Copy init.proto
	bytesRead, err := os.ReadFile("test/init.proto")
	if err != nil {
		t.Fatal(err)
	}
	err = os.WriteFile(filepath.Join(etcDir, "init.proto"), bytesRead, 0755)
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
	s := httptest.NewServer(
		http.FileServer(http.Dir("test")),
	)
	defer s.Close()
	endpoint := s.URL
	// Create 1 by file
	schema1 := &Info{
		Name:     "test1",
		Type:     "protobuf",
		FilePath: endpoint + "/test1.proto",
	}
	err = Register(schema1)
	if err != nil {
		t.Errorf("Register schema1 error: %v", err)
		return
	}
	// Get 1
	expectedSchema := &Info{
		Type:     "protobuf",
		Name:     "test1",
		Content:  "syntax = \"proto2\";message Person {required string name = 1;optional int32 id = 2;optional string email = 3;repeated ListOfDoubles code = 4;}message ListOfDoubles {repeated double doubles=1;}",
		FilePath: filepath.Join(etcDir, "test1.proto"),
	}
	gottenSchema, err := GetSchema("protobuf", "test1")
	if !reflect.DeepEqual(gottenSchema, expectedSchema) {
		t.Errorf("Get test1 unmatch: Expect\n%v\nbut got\n%v", *expectedSchema, *gottenSchema)
		return
	}
	// Create 2 by content
	schema2 := &Info{
		Name:    "test2",
		Type:    "protobuf",
		Content: "message Book{\n  required string name = 1;}",
	}
	err = Register(schema2)
	if err != nil {
		t.Errorf("Register schema2 error: %v", err)
		return
	}
	// Update 2 by file
	updatedSchema2 := &Info{
		Name:     "test2",
		Type:     "protobuf",
		FilePath: endpoint + "/test2.proto",
	}
	err = CreateOrUpdateSchema(updatedSchema2)
	if err != nil {
		t.Errorf("Update Schema2 error: %v", err)
		return
	}
	// List & check file
	regSchemas, err := GetAllForType("protobuf")
	expectedSchemas := []string{
		"init", "test1", "test2",
	}
	if !reflect.DeepEqual(len(regSchemas), len(expectedSchemas)) {
		t.Errorf("Expect\n%v\nbut got\n%v", expectedSchemas, regSchemas)
		return
	}
	checkFile(etcDir, expectedSchemas, t)
	// Delete 2
	err = DeleteSchema("protobuf", "test2")
	if err != nil {
		t.Errorf("Delete Schema2 error: %v", err)
		return
	}
	// Update 1 by content
	updatedSchema1 := &Info{
		Name:    "test1",
		Type:    "protobuf",
		Content: "message Person{required string name = 1;required int32 id = 2;optional string email = 3;}",
	}
	err = CreateOrUpdateSchema(updatedSchema1)
	if err != nil {
		t.Errorf("Update Schema1 error: %v", err)
		return
	}
	// List & check file
	regSchemas, err = GetAllForType("protobuf")
	expectedSchemas = []string{
		"init", "test1",
	}
	if !reflect.DeepEqual(len(regSchemas), len(expectedSchemas)) {
		t.Errorf("Expect\n%v\nbut got\n%v", expectedSchemas, regSchemas)
		return
	}
	checkFile(etcDir, expectedSchemas, t)
	// Delete 1
	err = DeleteSchema("protobuf", "test1")
	if err != nil {
		t.Errorf("Delete Schema1 error: %v", err)
		return
	}
	// List & check file
	regSchemas, err = GetAllForType("protobuf")
	expectedSchemas = []string{
		"init",
	}
	if !reflect.DeepEqual(regSchemas, expectedSchemas) {
		t.Errorf("Expect\n%v\nbut got\n%v", expectedSchemas, regSchemas)
		return
	}
	checkFile(etcDir, expectedSchemas, t)
}

func checkFile(etcDir string, schemas []string, t *testing.T) {
	files, err := os.ReadDir(etcDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != len(schemas) {
		t.Errorf("Expect %d files but got %d", len(schemas), len(files))
		return
	}
	for _, file := range files {
		fileName := filepath.Base(file.Name())
		found := false
		for _, schema := range schemas {
			if fileName == schema+".proto" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expect %s but got %s", schemas, fileName)
			return
		}
	}
}
