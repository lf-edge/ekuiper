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
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/pkg/store"
	"github.com/lf-edge/ekuiper/pkg/kv"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/def"
	"github.com/lf-edge/ekuiper/internal/pkg/httpx"
)

// Initialize in the server startup
var registry *Registry
var schemaDb kv.KeyValue
var schemaStatusDb kv.KeyValue

type Files struct {
	SchemaFile string
	SoFile     string
}

// Registry is a global registry for schemas
// It stores the schema ids and the ref to its file content in memory
// The schema definition is stored in the file system and will only be loaded once used
type Registry struct {
	sync.RWMutex
	// The map of schema files for all types
	schemas map[def.SchemaType]map[string]*Files
}

// Registry provide the method to add, update, get and parse and delete schemas

// InitRegistry initialize the registry, only called once by the server
func InitRegistry() error {
	registry = &Registry{
		schemas: make(map[def.SchemaType]map[string]*Files, len(def.SchemaTypes)),
	}
	dataDir, err := conf.GetDataLoc()
	if err != nil {
		return fmt.Errorf("cannot find etc folder: %s", err)
	}
	err, schemaDb = store.GetKV("schema")
	if err != nil {
		return fmt.Errorf("cannot open schema db: %s", err)
	}
	err, schemaStatusDb = store.GetKV("schemaStatus")
	if err != nil {
		return fmt.Errorf("cannot open schemaStatus db: %s", err)
	}
	if hasInstallFlag() {
		schemaInstallWhenReboot()
		clearInstallFlag()
	} else {
		for _, schemaType := range def.SchemaTypes {
			schemaDir := filepath.Join(dataDir, "schemas", string(schemaType))
			var newSchemas map[string]*Files
			files, err := os.ReadDir(schemaDir)
			if err != nil {
				conf.Log.Warnf("cannot read schema directory: %s", err)
				newSchemas = make(map[string]*Files)
			} else {
				newSchemas = make(map[string]*Files, len(files))
				for _, file := range files {
					fileName := filepath.Base(file.Name())
					ext := filepath.Ext(fileName)
					schemaId := strings.TrimSuffix(fileName, filepath.Ext(fileName))
					ffs, ok := newSchemas[schemaId]
					if !ok {
						ffs = &Files{}
						newSchemas[schemaId] = ffs
					}
					switch ext {
					case ".so":
						ffs.SoFile = filepath.Join(schemaDir, file.Name())
					default:
						ffs.SchemaFile = filepath.Join(schemaDir, file.Name())
					}
					conf.Log.Infof("schema file %s.%s loaded", schemaType, schemaId)
				}
			}
			registry.schemas[schemaType] = newSchemas
		}
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
	dataDir, _ := conf.GetDataLoc()
	etcDir := filepath.Join(dataDir, "schemas", string(info.Type))
	if err := os.MkdirAll(etcDir, os.ModePerm); err != nil {
		return err
	}
	ffs := &Files{}
	if info.Content != "" || info.FilePath != "" {
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
		ffs.SchemaFile = schemaFile
	}

	if info.SoPath != "" {
		soFile := filepath.Join(etcDir, info.Name+".so")
		err := httpx.DownloadFile(soFile, info.SoPath)
		if err != nil {
			return err
		}
		ffs.SoFile = soFile
	}

	registry.schemas[info.Type][info.Name] = ffs
	return nil
}

func GetSchema(schemaType def.SchemaType, name string) (*Info, error) {
	schemaFile, err := GetSchemaFile(schemaType, name)
	if err != nil {
		return nil, err
	}
	if schemaFile.SchemaFile != "" {
		content, err := os.ReadFile(schemaFile.SchemaFile)
		if err != nil {
			return nil, fmt.Errorf("cannot read schema file %s: %s", schemaFile, err)
		}
		return &Info{
			Type:     schemaType,
			Name:     name,
			Content:  string(content),
			FilePath: schemaFile.SchemaFile,
			SoPath:   schemaFile.SoFile,
		}, nil
	} else {
		return &Info{
			Type:   schemaType,
			Name:   name,
			SoPath: schemaFile.SoFile,
		}, nil
	}

}

func GetSchemaFile(schemaType def.SchemaType, name string) (*Files, error) {
	registry.RLock()
	defer registry.RUnlock()
	if _, ok := registry.schemas[schemaType]; !ok {
		return nil, fmt.Errorf("schema type %s not found in registry", schemaType)
	}
	if _, ok := registry.schemas[schemaType][name]; !ok {
		return nil, fmt.Errorf("schema type %s, file %s not found", schemaType, name)
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
	if schemaFile.SchemaFile != "" {
		err := os.Remove(schemaFile.SchemaFile)
		if err != nil {
			conf.Log.Errorf("cannot delete schema file %s: %s", schemaFile.SchemaFile, err)
		}
	}
	if schemaFile.SoFile != "" {
		err := os.Remove(schemaFile.SoFile)
		if err != nil {
			conf.Log.Errorf("cannot delete schema so file %s: %s", schemaFile.SoFile, err)
		}
	}
	delete(registry.schemas[schemaType], name)
	return nil
}

const BOOT_INSTALL = "$boot_install"

func GetAllSchema() map[string]string {
	all, err := schemaDb.All()
	if err != nil {
		return nil
	}
	delete(all, BOOT_INSTALL)
	return all
}

func GetAllSchemaStatus() map[string]string {
	all, err := schemaStatusDb.All()
	if err != nil {
		return nil
	}
	return all
}

func UninstallAllSchema() {
	schemaMaps, err := schemaDb.All()
	if err != nil {
		return
	}
	for key, value := range schemaMaps {
		info := &Info{}
		_ = json.Unmarshal([]byte(value), info)
		_ = DeleteSchema(info.Type, key)
	}
}

func hasInstallFlag() bool {
	var val = ""
	found, _ := schemaDb.Get(BOOT_INSTALL, &val)
	return found
}

func clearInstallFlag() {
	_ = schemaDb.Delete(BOOT_INSTALL)
}

func ImportSchema(schema map[string]string) error {
	if len(schema) == 0 {
		return nil
	}
	for k, v := range schema {
		err := schemaDb.Set(k, v)
		if err != nil {
			return err
		}
	}
	//set the flag to install the plugins when eKuiper reboot
	return schemaDb.Set(BOOT_INSTALL, BOOT_INSTALL)
}

func schemaInstallWhenReboot() {
	allPlgs, err := schemaDb.All()
	if err != nil {
		return
	}

	delete(allPlgs, BOOT_INSTALL)
	_ = schemaStatusDb.Clean()

	for k, v := range allPlgs {
		info := &Info{}
		err := json.Unmarshal([]byte(v), info)
		if err != nil {
			_ = schemaStatusDb.Set(k, err.Error())
			continue
		}
		err = CreateOrUpdateSchema(info)
		if err != nil {
			_ = schemaStatusDb.Set(k, err.Error())
			continue
		}
	}
}
