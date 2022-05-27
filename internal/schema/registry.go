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
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/def"
	"github.com/lf-edge/ekuiper/internal/pkg/httpx"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Initialize in the server startup
var registry *Registry

// Registry is a global registry for schemas
// It stores the schema ids and the ref to its file content in memory
// The schema definition is stored in the file system and will only be loaded once used
type Registry struct {
	sync.RWMutex
	// The map of schema files for all types
	schemas map[def.SchemaType]map[string]string
}

// Registry provide the method to add, update, get and parse and delete schemas

// InitRegistry initialize the registry, only called once by the server
func InitRegistry() error {
	registry = &Registry{
		schemas: make(map[def.SchemaType]map[string]string, len(def.SchemaTypes)),
	}
	etcDir, err := conf.GetConfLoc()
	if err != nil {
		return fmt.Errorf("cannot find etc folder: %s", err)
	}
	for _, schemaType := range def.SchemaTypes {
		schemaDir := filepath.Join(etcDir, "schemas", string(schemaType))
		var newSchemas map[string]string
		files, err := ioutil.ReadDir(schemaDir)
		if err != nil {
			conf.Log.Warnf("cannot read schema directory: %s", err)
			newSchemas = make(map[string]string)
		} else {
			newSchemas = make(map[string]string, len(files))
			for _, file := range files {
				fileName := filepath.Base(file.Name())
				schemaId := strings.TrimSuffix(fileName, filepath.Ext(fileName))
				newSchemas[schemaId] = filepath.Join(schemaDir, file.Name())
			}
		}
		registry.schemas[schemaType] = newSchemas
	}
	return nil
}

func GetAllForType(schemaType def.SchemaType) ([]string, error) {
	registry.RLock()
	defer registry.RUnlock()
	if _, ok := registry.schemas[schemaType]; !ok {
		return nil, fmt.Errorf("schema type %s not found", schemaType)
	}
	result := make([]string, 0, len(registry.schemas[schemaType]))
	for k := range registry.schemas[schemaType] {
		result = append(result, k)
	}
	return result, nil
}

func Register(info *Info) error {
	if _, ok := registry.schemas[info.Type]; !ok {
		return fmt.Errorf("schema type %s not found", info.Type)
	}
	if _, ok := registry.schemas[info.Type][info.Name]; ok {
		return fmt.Errorf("schema %s.%s already registered", info.Type, info.Name)
	}
	return CreateOrUpdateSchema(info)
}

func CreateOrUpdateSchema(info *Info) error {
	if _, ok := registry.schemas[info.Type]; !ok {
		return fmt.Errorf("schema type %s not found", info.Type)
	}
	etcDir, _ := conf.GetConfLoc()
	etcDir = filepath.Join(etcDir, "schemas", string(info.Type))
	if err := os.MkdirAll(etcDir, os.ModePerm); err != nil {
		return err
	}
	schemaFile := filepath.Join(etcDir, info.Name+schemaExt[info.Type])
	if _, err := os.Stat(schemaFile); os.IsNotExist(err) {
		file, err := os.Create(schemaFile)
		if err != nil {
			return err
		}
		defer file.Close()
	}
	if info.Content != "" {
		err := os.WriteFile(schemaFile, []byte(info.Content), 0666)
		if err != nil {
			return err
		}
	} else {
		err := httpx.DownloadFile(schemaFile, info.FilePath)
		if err != nil {
			return err
		}
	}

	registry.schemas[info.Type][info.Name] = schemaFile
	return nil
}

func GetSchema(schemaType def.SchemaType, name string) (*Info, error) {
	schemaFile, err := GetSchemaFile(schemaType, name)
	if err != nil {
		return nil, err
	}
	content, err := ioutil.ReadFile(schemaFile)
	if err != nil {
		return nil, fmt.Errorf("cannot read schema file %s: %s", schemaFile, err)
	}
	return &Info{
		Type:     schemaType,
		Name:     name,
		Content:  string(content),
		FilePath: schemaFile,
	}, nil
}

func GetSchemaFile(schemaType def.SchemaType, name string) (string, error) {
	registry.RLock()
	defer registry.RUnlock()
	if _, ok := registry.schemas[schemaType]; !ok {
		return "", fmt.Errorf("schema type %s not found", schemaType)
	}
	if _, ok := registry.schemas[schemaType][name]; !ok {
		return "", fmt.Errorf("schema %s.%s not found", schemaType, name)
	}
	schemaFile := registry.schemas[schemaType][name]
	return schemaFile, nil
}

func DeleteSchema(schemaType def.SchemaType, name string) error {
	registry.Lock()
	defer registry.Unlock()
	if _, ok := registry.schemas[schemaType]; !ok {
		return fmt.Errorf("schema type %s not found", schemaType)
	}
	if _, ok := registry.schemas[schemaType][name]; !ok {
		return fmt.Errorf("schema %s.%s not found", schemaType, name)
	}
	schemaFile := registry.schemas[schemaType][name]
	err := os.Remove(schemaFile)
	if err != nil {
		conf.Log.Errorf("cannot delete schema file %s: %s", schemaFile, err)
	}
	delete(registry.schemas[schemaType], name)
	return nil
}
