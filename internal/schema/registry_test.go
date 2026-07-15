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
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/pkg/kv"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

type failingSetKV struct {
	kv.KeyValue
}

func (failingSetKV) Set(string, interface{}) error {
	return fmt.Errorf("injected schema DB write failure")
}

func init() {
	testx.InitEnv("schema")
	conf.Config.Basic.EnablePrivateNet = true
}

func TestPartialImpoart(t *testing.T) {
	etcDir, err := conf.GetDataLoc()
	require.NoError(t, err)
	etcDir = filepath.Join(etcDir, "schemas", "protobuf")
	err = os.MkdirAll(etcDir, os.ModePerm)
	require.NoError(t, err)
	pt := &PbType{}
	modules.RegisterSchemaType(modules.PROTOBUF, pt, ".proto")
	err = InitRegistry()
	require.NoError(t, err)
	errMap := SchemaPartialImport(context.Background(), map[string]string{"protobuf_test111": `{"type":"protobuf","name":"test111","content":"message Book {required string a = 1; oneof b {string c = 3;string d = 4; }}","file":"","soFile":""}`})
	require.Equal(t, 0, len(errMap))
	result := GetAllSchema()
	require.Equal(t, map[string]string{"protobuf_test111": `{"type":"protobuf","name":"test111","content":"message Book {required string a = 1; oneof b {string c = 3;string d = 4; }}","file":"","soFile":""}`}, result)
	DeleteSchema(modules.PROTOBUF, "test111")
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
	err = os.WriteFile(filepath.Join(etcDir, "init@v1.2.proto"), bytesRead, 0o755)
	require.NoError(t, err)
	defer func() {
		err = os.RemoveAll(etcDir)
		require.NoError(t, err)
	}()
	pt := &PbType{}
	modules.RegisterSchemaType(modules.PROTOBUF, pt, ".proto")
	err = InitRegistry()
	require.NoError(t, err)
	s := httptest.NewServer(
		http.FileServer(http.Dir("test")),
	)
	defer s.Close()
	endpoint := s.URL
	// Check default
	expectedSchema := &Info{
		Type:     "protobuf",
		Name:     "init",
		Content:  "syntax = \"proto3\";\n\nmessage HelloRequest {\n  string name = 1;\n}",
		Version:  "v1.2",
		FilePath: filepath.Join(etcDir, "init@v1.2.proto"),
	}
	gottenSchema, err := GetSchema("protobuf", "init")
	assert.Equal(t, expectedSchema, gottenSchema)
	expectedFiles := []string{
		"init@v1.2.proto",
	}
	checkFile(etcDir, expectedFiles, t)
	// Create 1 by file
	schema1 := &Info{
		Name:     "test1",
		Type:     "protobuf",
		Version:  "1756347354",
		FilePath: endpoint + "/test1.zip",
	}
	err = Register(schema1)
	require.NoError(t, err)
	// Get 1
	expectedSchema = &Info{
		Type:     "protobuf",
		Name:     "test1",
		Content:  "syntax = \"proto2\";message Person {required string name = 1;optional int32 id = 2;optional string email = 3;}message ListOfDoubles {repeated double doubles = 1;}",
		Version:  "1756347354",
		FilePath: filepath.Join(etcDir, "test1@1756347354.proto"),
	}
	gottenSchema, err = GetSchema("protobuf", "test1")
	assert.Equal(t, expectedSchema, gottenSchema)
	expectedFiles = []string{
		"init@v1.2.proto", "test1@1756347354.proto", "test1",
	}
	checkFile(etcDir, expectedFiles, t)
	// Update 1 by file
	schema1 = &Info{
		Name:     "test1",
		Type:     "protobuf",
		FilePath: endpoint + "/test1.proto",
	}
	err = CreateOrUpdateSchema(schema1)
	require.EqualError(t, err, "schema protobuf.test1 already registered with a newer version 1756347354")
	// Update 1 by file
	schema1 = &Info{
		Name:     "test1",
		Type:     "protobuf",
		FilePath: endpoint + "/test1.proto",
		Version:  "1756348354",
	}
	err = CreateOrUpdateSchema(schema1)
	require.NoError(t, err)
	// Get 1
	expectedSchema = &Info{
		Type:     "protobuf",
		Name:     "test1",
		Content:  "syntax = \"proto2\";message Person {required string name = 1;optional int32 id = 2;optional string email = 3;repeated ListOfDoubles code = 4;}message ListOfDoubles {repeated double doubles = 1;}",
		Version:  "1756348354",
		FilePath: filepath.Join(etcDir, "test1@1756348354.proto"),
	}
	gottenSchema, err = GetSchema("protobuf", "test1")
	require.Equal(t, expectedSchema, gottenSchema)
	expectedFiles = []string{
		"init@v1.2.proto", "test1@1756348354.proto",
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
		"init@v1.2.proto", "test1@1756348354.proto", "test2.proto", "test2.so",
	}
	checkFile(etcDir, expectedFiles, t)
	// Delete 2
	err = DeleteSchema("protobuf", "test2")
	require.NoError(t, err)
	// Update 1 by content
	updatedSchema1 := &Info{
		Name:    "test1",
		Type:    "protobuf",
		Version: "1756348355",
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
		"init@v1.2.proto", "test1@1756348355.proto",
	}
	checkFile(etcDir, expectedFiles, t)
	// Update schema
	schema1 = &Info{
		Name:     "test1",
		Type:     "protobuf",
		FilePath: endpoint + "/test1.zip",
		Version:  "1756349354",
	}
	err = CreateOrUpdateSchema(schema1)
	require.NoError(t, err)
	// Get 1
	expectedSchema = &Info{
		Type:     "protobuf",
		Name:     "test1",
		Content:  "syntax = \"proto2\";message Person {required string name = 1;optional int32 id = 2;optional string email = 3;}message ListOfDoubles {repeated double doubles = 1;}",
		Version:  "1756349354",
		FilePath: filepath.Join(etcDir, "test1@1756349354.proto"),
	}
	gottenSchema, err = GetSchema("protobuf", "test1")
	assert.Equal(t, expectedSchema, gottenSchema)
	expectedFiles = []string{
		"init@v1.2.proto", "test1@1756349354.proto", "test1",
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
		"init@v1.2.proto",
	}
	checkFile(etcDir, expectedFiles, t)
}

func TestCustomRegistry(t *testing.T) {
	// Move test schema file to etc dir
	etcDir, err := conf.GetConfLoc()
	require.NoError(t, err)
	etcDir = filepath.Join(etcDir, "schemas", "custom")
	err = os.MkdirAll(etcDir, os.ModePerm)
	require.NoError(t, err)
	dataDir, err := conf.GetDataLoc()
	require.NoError(t, err)
	dataDir = filepath.Join(dataDir, "schemas", "custom")
	err = os.MkdirAll(dataDir, os.ModePerm)
	require.NoError(t, err)
	// Copy fake.so as init
	bytesRead, err := os.ReadFile("test/fake.so")
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(etcDir, "init.so"), bytesRead, 0o755)
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(etcDir, "test1.so"), bytesRead, 0o755)
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(etcDir, "test2@1@2.so"), bytesRead, 0o755)
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(dataDir, "init@1234.so"), bytesRead, 0o755)
	require.NoError(t, err)
	defer func() {
		err = os.RemoveAll(dataDir)
		if err != nil {
			t.Fatal(err)
		}
	}()
	modules.RegisterSchemaType(modules.CUSTOM, &CustomType{}, ".so")
	err = InitRegistry()
	require.NoError(t, err)
	regSchemas, err := GetAllForType("custom")
	expectedSchemas := []string{
		"init", "test1",
	}
	sort.Strings(regSchemas)
	require.Equal(t, expectedSchemas, regSchemas)
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
	require.EqualError(t, err, "schema custom.test1 already registered")
	// Get 1
	err = CreateOrUpdateSchema(schema1)
	expectedSchema := &Info{
		Type:   "custom",
		Name:   "test1",
		SoPath: filepath.Join(dataDir, "test1.so"),
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
	regSchemas, err = GetAllForType("custom")
	expectedSchemas = []string{
		"init", "test1",
	}
	if !reflect.DeepEqual(len(regSchemas), len(expectedSchemas)) {
		t.Errorf("Expect\n%v\nbut got\n%v", expectedSchemas, regSchemas)
		return
	}
	expectedFiles := []string{
		"init@1234.so", "test1.so",
	}
	checkFile(dataDir, expectedFiles, t)
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
	checkFile(dataDir, expectedFiles, t)
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

func TestUpsert(t *testing.T) {
	modules.RegisterSchemaType(modules.PROTOBUF, &PbType{}, ".proto")
	require.NoError(t, InitRegistry())
	const name = "upsert_concurrent_test"
	_ = DeleteSchema(modules.PROTOBUF, name)
	defer func() {
		_ = DeleteSchema(modules.PROTOBUF, name)
	}()

	dataDir, err := conf.GetDataLoc()
	require.NoError(t, err)
	tempDir := filepath.Join(dataDir, "uploads", "schemas")
	require.NoError(t, os.MkdirAll(tempDir, 0o755))
	paths := make([]string, 2)
	contents := []string{
		"message First { required string name = 1; }",
		"message Second { required int32 id = 1; }",
	}
	for i, content := range contents {
		file, createErr := os.CreateTemp(tempDir, ".upsert-test-*.proto")
		require.NoError(t, createErr)
		paths[i] = file.Name()
		_, writeErr := file.WriteString(content)
		require.NoError(t, writeErr)
		require.NoError(t, file.Close())
		defer os.Remove(paths[i])
	}

	createdResults := make(chan bool, 2)
	errorResults := make(chan error, 2)
	var wg sync.WaitGroup
	for _, path := range paths {
		wg.Add(1)
		go func(path string) {
			defer wg.Done()
			created, upsertErr := Upsert(&Info{
				Type:     modules.PROTOBUF,
				Name:     name,
				FilePath: localFileURL(path),
			})
			createdResults <- created
			errorResults <- upsertErr
		}(path)
	}
	wg.Wait()
	close(createdResults)
	close(errorResults)
	for upsertErr := range errorResults {
		require.NoError(t, upsertErr)
	}
	createdCount := 0
	for created := range createdResults {
		if created {
			createdCount++
		}
	}
	require.Equal(t, 1, createdCount)

	gotten, err := GetSchema(modules.PROTOBUF, name)
	require.NoError(t, err)
	require.Contains(t, contents, gotten.Content)
	_, script := GetSchemaInstallScript(modules.PROTOBUF + "_" + name)
	require.NotContains(t, script, ".upsert-test-")
	require.True(t, strings.Contains(script, "/schemas/protobuf/") || strings.Contains(script, "\\schemas\\protobuf\\"))
}

func TestUpsertRollsBackFilesWhenPersistenceFails(t *testing.T) {
	modules.RegisterSchemaType(modules.PROTOBUF, &PbType{}, ".proto")
	require.NoError(t, InitRegistry())

	dataDir, err := conf.GetDataLoc()
	require.NoError(t, err)
	schemaDir := filepath.Join(dataDir, "schemas", modules.PROTOBUF)

	t.Run("create", func(t *testing.T) {
		const name = "upsert_db_failure_create"
		_ = DeleteSchema(modules.PROTOBUF, name)
		defer func() { _ = DeleteSchema(modules.PROTOBUF, name) }()

		originalDB := schemaDb
		schemaDb = failingSetKV{KeyValue: originalDB}
		defer func() { schemaDb = originalDB }()

		created, upsertErr := Upsert(&Info{
			Type:    modules.PROTOBUF,
			Name:    name,
			Content: "message Created { required string value = 1; }",
		})
		require.False(t, created)
		require.EqualError(t, upsertErr, "injected schema DB write failure")
		_, getErr := GetSchema(modules.PROTOBUF, name)
		require.Error(t, getErr)
		require.NoFileExists(t, filepath.Join(schemaDir, name+".proto"))
	})

	t.Run("update", func(t *testing.T) {
		const name = "upsert_db_failure_update"
		_ = DeleteSchema(modules.PROTOBUF, name)
		defer func() { _ = DeleteSchema(modules.PROTOBUF, name) }()

		const originalContent = "message Original { required string value = 1; }"
		created, upsertErr := Upsert(&Info{
			Type:    modules.PROTOBUF,
			Name:    name,
			Content: originalContent,
		})
		require.NoError(t, upsertErr)
		require.True(t, created)
		_, originalScript := GetSchemaInstallScript(modules.PROTOBUF + "_" + name)

		originalDB := schemaDb
		schemaDb = failingSetKV{KeyValue: originalDB}
		defer func() { schemaDb = originalDB }()

		created, upsertErr = Upsert(&Info{
			Type:    modules.PROTOBUF,
			Name:    name,
			Content: "message Updated { required int32 value = 1; }",
		})
		require.False(t, created)
		require.EqualError(t, upsertErr, "injected schema DB write failure")
		gotten, getErr := GetSchema(modules.PROTOBUF, name)
		require.NoError(t, getErr)
		require.Equal(t, originalContent, gotten.Content)
		_, currentScript := GetSchemaInstallScript(modules.PROTOBUF + "_" + name)
		require.Equal(t, originalScript, currentScript)
	})
}

func TestFileReplacementTransaction(t *testing.T) {
	t.Run("rolls back a partial installation", func(t *testing.T) {
		dir := t.TempDir()
		targetFile := filepath.Join(dir, "schema.proto")
		require.NoError(t, os.WriteFile(targetFile, []byte("old schema"), 0o600))
		sourceFile := filepath.Join(dir, "new.proto")
		require.NoError(t, os.WriteFile(sourceFile, []byte("new schema"), 0o600))

		targetDir := filepath.Join(dir, "supporting")
		require.NoError(t, os.Mkdir(targetDir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(targetDir, "old.txt"), []byte("old support"), 0o600))

		replacements := []fileReplacement{
			{source: sourceFile, target: targetFile},
			{target: targetDir},
			{source: filepath.Join(dir, "missing.proto"), target: filepath.Join(dir, "third.proto")},
		}
		err := commitFileReplacements(replacements, filepath.Join(dir, "backup"))
		require.Error(t, err)
		content, err := os.ReadFile(targetFile)
		require.NoError(t, err)
		require.Equal(t, "old schema", string(content))
		support, err := os.ReadFile(filepath.Join(targetDir, "old.txt"))
		require.NoError(t, err)
		require.Equal(t, "old support", string(support))
		require.NoFileExists(t, replacements[2].target)
	})

	t.Run("handles empty replacement list", func(t *testing.T) {
		require.NoError(t, commitFileReplacements(nil, filepath.Join(t.TempDir(), "backup")))
	})

	t.Run("reports backup directory error", func(t *testing.T) {
		dir := t.TempDir()
		backupPath := filepath.Join(dir, "backup")
		require.NoError(t, os.WriteFile(backupPath, []byte("not a directory"), 0o600))
		err := commitFileReplacements([]fileReplacement{{target: filepath.Join(dir, "target")}}, backupPath)
		require.Error(t, err)
	})

	t.Run("reports rollback error", func(t *testing.T) {
		dir := t.TempDir()
		err := rollbackFileReplacements([]fileReplacement{{
			target:    filepath.Join(dir, "target"),
			backup:    filepath.Join(dir, "missing-backup"),
			hadTarget: true,
		}})
		require.Error(t, err)
	})
}
