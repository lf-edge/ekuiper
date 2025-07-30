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
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func init() {
	testx.InitEnv("schema")
}

func TestProtoRegistry(t *testing.T) {
	// Move test schema file to etc dir
	etcDir, err := conf.GetDataLoc()
	require.NoError(t, err)
	etcDir = filepath.Join(etcDir, "schemas", "protobuf")
	err = os.MkdirAll(etcDir, os.ModePerm)
	require.NoError(t, err)
	// Copy init.proto
	bytesRead, err := os.ReadFile("test/init.proto")
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(etcDir, "init.proto"), bytesRead, 0o755)
	require.NoError(t, err)
	defer func() {
		err = os.RemoveAll(etcDir)
		require.NoError(t, err)
	}()
	pt := &PbType{}
	modules.RegisterSchemaType(modules.PROTOBUF, pt)
	err = InitRegistry()
	require.NoError(t, err)
	s := httptest.NewServer(
		http.FileServer(http.Dir("test")),
	)
	defer s.Close()
	endpoint := s.URL
	// Create 1 by file
	schema1 := &Info{
		Name:     "test1",
		Type:     "protobuf",
		FilePath: endpoint + "/test1.zip",
	}
	err = Register(schema1)
	require.NoError(t, err)
	// Get 1
	expectedSchema := &Info{
		Type:     "protobuf",
		Name:     "test1",
		Content:  "syntax = \"proto2\";message Person {required string name = 1;optional int32 id = 2;optional string email = 3;}message ListOfDoubles {repeated double doubles = 1;}",
		FilePath: filepath.Join(etcDir, "test1.proto"),
	}
	gottenSchema, err := GetSchema("protobuf", "test1")
	assert.Equal(t, expectedSchema, gottenSchema)
	expectedFiles := []string{
		"init.proto", "test1.proto", "test1",
	}
	checkFile(etcDir, expectedFiles, t)
	// Update 1 by file
	schema1 = &Info{
		Name:     "test1",
		Type:     "protobuf",
		FilePath: endpoint + "/test1.proto",
	}
	err = CreateOrUpdateSchema(schema1)
	if err != nil {
		t.Errorf("Update schema1 error: %v", err)
		return
	}
	// Get 1
	expectedSchema = &Info{
		Type:     "protobuf",
		Name:     "test1",
		Content:  "syntax = \"proto2\";message Person {required string name = 1;optional int32 id = 2;optional string email = 3;repeated ListOfDoubles code = 4;}message ListOfDoubles {repeated double doubles = 1;}",
		FilePath: filepath.Join(etcDir, "test1.proto"),
	}
	gottenSchema, err = GetSchema("protobuf", "test1")
	if !reflect.DeepEqual(gottenSchema, expectedSchema) {
		t.Errorf("Get test1 unmatch: Expect\n%v\nbut got\n%v", *expectedSchema, *gottenSchema)
		return
	}
	expectedFiles = []string{
		"init.proto", "test1.proto",
	}
	checkFile(etcDir, expectedFiles, t)
	// Create 1 with invalid zip (no named file)
	schema1 = &Info{
		Name:     "test",
		Type:     "protobuf",
		FilePath: endpoint + "/test1.zip",
	}
	err = Register(schema1)
	assert.EqualError(t, err, "schema file test.proto not found inside the zip")
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
		SoPath:   endpoint + "/fake.so",
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
	expectedFiles = []string{
		"init.proto", "test1.proto", "test2.proto", "test2.so",
	}
	checkFile(etcDir, expectedFiles, t)
	// Delete 2
	err = DeleteSchema("protobuf", "test2")
	require.NoError(t, err)
	// Update 1 by content
	updatedSchema1 := &Info{
		Name:    "test1",
		Type:    "protobuf",
		Content: "message Person{required string name = 1;required int32 id = 2;optional string email = 3;}",
	}
	err = CreateOrUpdateSchema(updatedSchema1)
	require.NoError(t, err)
	// List & check file
	regSchemas, err = GetAllForType("protobuf")
	expectedSchemas = []string{
		"init", "test1",
	}
	assert.Equal(t, len(regSchemas), len(expectedSchemas))
	expectedFiles = []string{
		"init.proto", "test1.proto",
	}
	checkFile(etcDir, expectedFiles, t)
	// Update schema
	schema1 = &Info{
		Name:     "test1",
		Type:     "protobuf",
		FilePath: endpoint + "/test1.zip",
	}
	err = CreateOrUpdateSchema(schema1)
	require.NoError(t, err)
	// Get 1
	expectedSchema = &Info{
		Type:     "protobuf",
		Name:     "test1",
		Content:  "syntax = \"proto2\";message Person {required string name = 1;optional int32 id = 2;optional string email = 3;}message ListOfDoubles {repeated double doubles = 1;}",
		FilePath: filepath.Join(etcDir, "test1.proto"),
	}
	gottenSchema, err = GetSchema("protobuf", "test1")
	assert.Equal(t, expectedSchema, gottenSchema)
	expectedFiles = []string{
		"init.proto", "test1.proto", "test1",
	}
	checkFile(etcDir, expectedFiles, t)
	// Delete 1
	err = DeleteSchema("protobuf", "test1")
	require.NoError(t, err)
	// List & check file
	regSchemas, err = GetAllForType("protobuf")
	expectedSchemas = []string{
		"init",
	}
	assert.Equal(t, regSchemas, expectedSchemas)
	expectedFiles = []string{
		"init.proto",
	}
	checkFile(etcDir, expectedFiles, t)
}

func TestCustomRegistry(t *testing.T) {
	// Move test schema file to etc dir
	etcDir, err := conf.GetDataLoc()
	if err != nil {
		t.Fatal(err)
	}
	etcDir = filepath.Join(etcDir, "schemas", "custom")
	err = os.MkdirAll(etcDir, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	// Copy fake.so as init
	bytesRead, err := os.ReadFile("test/fake.so")
	if err != nil {
		t.Fatal(err)
	}
	err = os.WriteFile(filepath.Join(etcDir, "init.so"), bytesRead, 0o755)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = os.RemoveAll(etcDir)
		if err != nil {
			t.Fatal(err)
		}
	}()
	modules.RegisterSchemaType(modules.CUSTOM, &CustomType{})
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
		Name:   "test1",
		Type:   "custom",
		SoPath: endpoint + "/fake.so",
	}
	err = Register(schema1)
	if err != nil {
		t.Errorf("Register schema1 error: %v", err)
		return
	}
	// Get 1
	expectedSchema := &Info{
		Type:   "custom",
		Name:   "test1",
		SoPath: filepath.Join(etcDir, "test1.so"),
	}
	gottenSchema, err := GetSchema("custom", "test1")
	if !reflect.DeepEqual(gottenSchema, expectedSchema) {
		t.Errorf("Get test1 unmatch: Expect\n%v\nbut got\n%v", *expectedSchema, *gottenSchema)
		return
	}
	// Update 1 by file
	updatedSchema2 := &Info{
		Name:   "test1",
		Type:   "custom",
		SoPath: endpoint + "/fake.so",
	}
	err = CreateOrUpdateSchema(updatedSchema2)
	if err != nil {
		t.Errorf("Update Schema2 error: %v", err)
		return
	}
	// List & check file
	regSchemas, err := GetAllForType("custom")
	expectedSchemas := []string{
		"init", "test1",
	}
	if !reflect.DeepEqual(len(regSchemas), len(expectedSchemas)) {
		t.Errorf("Expect\n%v\nbut got\n%v", expectedSchemas, regSchemas)
		return
	}
	expectedFiles := []string{
		"init.so", "test1.so",
	}
	checkFile(etcDir, expectedFiles, t)
	// Delete 2
	err = DeleteSchema("custom", "init")
	if err != nil {
		t.Errorf("Delete Schema2 error: %v", err)
		return
	}
	// List & check file
	regSchemas, err = GetAllForType("custom")
	expectedSchemas = []string{
		"test1",
	}
	if !reflect.DeepEqual(len(regSchemas), len(expectedSchemas)) {
		t.Errorf("Expect\n%v\nbut got\n%v", expectedSchemas, regSchemas)
		return
	}
	expectedFiles = []string{
		"test1.so",
	}
	checkFile(etcDir, expectedFiles, t)
}

func checkFile(etcDir string, schemas []string, t *testing.T) {
	files, err := os.ReadDir(etcDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != len(schemas) {
		fmt.Printf("files: %v\nschemas: %v\n", files, schemas)
		t.Errorf("Expect %d files but got %d", len(schemas), len(files))
		return
	}
	for _, file := range files {
		fileName := filepath.Base(file.Name())
		found := false
		for _, schema := range schemas {
			if fileName == schema {
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

func TestInvalidInfo(t *testing.T) {
	tests := []struct {
		info *Info
		err  string
	}{
		{
			info: &Info{
				Name: "../test1",
				Type: "custom",
			},
			err: "schema name ../test1 is invalid",
		},
		{
			info: &Info{
				Name: "test1",
				Type: "custom/../../test",
			},
			err: "schema type custom/../../test is invalid",
		},
		{
			info: &Info{
				Name: "test1",
				Type: "invalid",
			},
			err: "schema type invalid not found",
		},
	}
	InitRegistry()
	for _, tt := range tests {
		err := CreateOrUpdateSchema(tt.info)
		assert.EqualError(t, err, tt.err)
	}
}
