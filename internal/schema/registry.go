// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/google/uuid"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/filex"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/httpx"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/kv"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

// Initialize in the server startup
var (
	registry       *Registry
	schemaDb       kv.KeyValue
	schemaStatusDb kv.KeyValue
)

// Registry is a global registry for schemas
// It stores the schema ids and the ref to its file content in memory
// The schema definition is stored in the file system and will only be loaded once used
type Registry struct {
	sync.RWMutex
	// The map of schema files for all types
	schemas map[string]map[string]*modules.Files
}

// Registry provide the method to add, update, get and parse and delete schemas

// InitRegistry initialize the registry, only called once by the server
func InitRegistry() error {
	registry = &Registry{
		schemas: make(map[string]map[string]*modules.Files, len(modules.SchemaTypeDefs)),
	}
	// TODO shall we allow to delete etc schema?
	etcDir, err := conf.GetConfLoc()
	if err != nil {
		return fmt.Errorf("cannot find etc folder: %s", err)
	}
	dataDir, err := conf.GetDataLoc()
	if err != nil {
		return fmt.Errorf("cannot find data folder: %s", err)
	}
	schemaDb, err = store.GetKV("schema")
	if err != nil {
		return fmt.Errorf("cannot open schema db: %s", err)
	}
	schemaStatusDb, err = store.GetKV("schemaStatus")
	if err != nil {
		return fmt.Errorf("cannot open schemaStatus db: %s", err)
	}
	for schemaType, st := range modules.SchemaTypeDefs {
		// Read from etcDir firstly, then read from dataDir
		// Compare version and leave the newer version
		schemaDir := filepath.Join(etcDir, "schemas", schemaType)
		etcSchemas, err := st.Def.Scan(conf.Log, schemaDir)
		if err != nil {
			conf.Log.Warnf("cannot read schema directory: %s", err)
			etcSchemas = map[string]*modules.Files{}
		}
		for n, s := range etcSchemas {
			result := strings.Split(n, "@")
			if len(result) == 2 {
				s.Version = result[1]
				etcSchemas[result[0]] = s
				delete(etcSchemas, n)
			} else if len(result) > 2 {
				conf.Log.Warnf("schema definition '%s' has malform version name", n)
				delete(etcSchemas, n)
			}
		}
		schemaDir = filepath.Join(dataDir, "schemas", schemaType)
		newSchemas, err := st.Def.Scan(conf.Log, schemaDir)
		if err != nil {
			conf.Log.Warnf("cannot read schema directory: %s", err)
			newSchemas = map[string]*modules.Files{}
		}
		for n, s := range newSchemas {
			result := strings.Split(n, "@")
			if len(result) == 2 {
				s.Version = result[1]
				newSchemas[result[0]] = s
				delete(newSchemas, n)
			} else if len(result) > 2 {
				conf.Log.Warnf("schema definition '%s' has malform version name", n)
				delete(newSchemas, n)
			}
		}
		// merge schemas
		for n, s := range etcSchemas {
			ss, ok := newSchemas[n]
			if ok {
				if s.Version > ss.Version {
					conf.Log.Infof("schema definition '%s' in etc has version %s greater than %s", n, s.Version, ss.Version)
					newSchemas[n] = s
				}
			} else {
				newSchemas[n] = s
			}
		}
		registry.schemas[schemaType] = newSchemas
	}
	if hasInstallFlag() {
		schemaInstallWhenReboot()
		clearInstallFlag()
	}
	return nil
}

func GetAllForType(schemaType string) ([]string, error) {
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
	err := func() error {
		registry.RLock()
		defer registry.RUnlock()
		if _, ok := registry.schemas[info.Type]; !ok {
			return fmt.Errorf("schema type %s not found", info.Type)
		}
		if _, ok := registry.schemas[info.Type][info.Name]; ok {
			return fmt.Errorf("schema %s.%s already registered", info.Type, info.Name)
		}
		return nil
	}()
	if err != nil {
		return err
	}
	err = CreateOrUpdateSchema(info)
	if err != nil {
		return err
	}
	return nil
}

func CreateOrUpdateSchema(info *Info) error {
	registry.Lock()
	defer registry.Unlock()
	if strings.Contains(info.Type, "/") || strings.Contains(info.Type, "\\") || strings.Contains(info.Type, "..") {
		return fmt.Errorf("schema type %s is invalid", info.Type)
	}
	if strings.Contains(info.Name, "/") || strings.Contains(info.Name, "\\") || strings.Contains(info.Name, "..") {
		return fmt.Errorf("schema name %s is invalid", info.Name)
	}
	if strings.Contains(info.Version, "/") || strings.Contains(info.Version, "\\") || strings.Contains(info.Version, "..") {
		return fmt.Errorf("schema version %s is invalid", info.Version)
	}
	st, ok := modules.SchemaTypeDefs[info.Type]
	if !ok {
		return fmt.Errorf("schema type %s not found", info.Type)
	}
	// compare version
	if _, ok := registry.schemas[info.Type]; !ok {
		return fmt.Errorf("schema type %s not found", info.Type)
	}
	offs, ok := registry.schemas[info.Type][info.Name]
	if ok {
		if offs.Version != "" && offs.Version > info.Version {
			return fmt.Errorf("schema %s.%s already registered with a newer version %s", info.Type, info.Name, offs.Version)
		}
	}
	dataDir, _ := conf.GetDataLoc()
	etcDir := filepath.Join(dataDir, "schemas", info.Type)
	// make sure info.Type does not escape from root
	if err := os.MkdirAll(etcDir, os.ModePerm); err != nil {
		conf.Log.Warnf("failed to create directory %s: %v", info.Type, err)
	}
	ffs := &modules.Files{}
	// If file path is a .zip, it must have the name.type file and a folder of the same name to hold the supporting files. Other files will all be ignored.
	// Otherwise, save the file in the upper folder
	if info.Content != "" || info.FilePath != "" {
		supportingDir := filepath.Join(etcDir, info.Name)
		err := os.RemoveAll(filepath.Join(etcDir, info.Name))
		if err != nil {
			conf.Log.Errorf("cannot delete schema supporting files %s: %s", supportingDir, err)
		}

		schemaFileName := info.Name + st.Ext
		targetName := schemaFileName
		if info.Version != "" {
			targetName = fmt.Sprintf("%s@%s%s", info.Name, info.Version, st.Ext)
		}
		schemaFile := filepath.Join(etcDir, targetName)
		if filepath.Ext(info.FilePath) == ".zip" {
			conf.Log.Infof("unzipping schema file %s", info.FilePath)
			tmpFile := filepath.Join(etcDir, uuid.New().String()+".zip")
			err := httpx.DownloadFile(tmpFile, info.FilePath)
			if err != nil {
				return err
			}
			defer os.Remove(tmpFile)
			reader, err := zip.OpenReader(tmpFile)
			if err != nil {
				return err
			}
			defer reader.Close()
			found := false
			for _, file := range reader.File {
				fileName := file.Name
				// Check if it's the exact file we want
				if fileName == schemaFileName {
					err = filex.UnzipTo(file, etcDir, targetName)
					found = true
				} else if fileName == info.Name && file.FileInfo().IsDir() {
					err = filex.UnzipTo(file, etcDir, info.Name)
				} else if strings.HasPrefix(fileName, info.Name+"/") {
					err = filex.UnzipTo(file, etcDir, fileName)
				} else {
					// Skip files that don't match our criteria
					continue
				}
				if err != nil {
					return err
				}
			}
			if !found {
				return fmt.Errorf("schema file %s not found inside the zip", schemaFileName)
			}
		} else {
			if _, err := os.Stat(schemaFile); os.IsNotExist(err) {
				file, err := os.Create(schemaFile)
				if err != nil {
					return err
				}
				defer file.Close()
			}
			if info.Content != "" {
				err := os.WriteFile(schemaFile, cast.StringToBytes(info.Content), 0o666)
				if err != nil {
					return err
				}
			} else {
				err := httpx.DownloadFile(schemaFile, info.FilePath)
				if err != nil {
					return err
				}
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
	ffs.Version = info.Version
	registry.schemas[info.Type][info.Name] = ffs
	storeSchemaInstallScript(info)
	// clean up old ffs
	if offs != nil && info.Version != "" {
		if offs.SchemaFile != "" {
			err := os.Remove(offs.SchemaFile)
			if err != nil {
				conf.Log.Errorf("cannot delete old schema file %s: %s", offs.SchemaFile, err)
			}
		}
	}
	return nil
}

func GetSchema(schemaType string, name string) (*Info, error) {
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
			Version:  schemaFile.Version,
		}, nil
	} else {
		return &Info{
			Type:    schemaType,
			Name:    name,
			SoPath:  schemaFile.SoFile,
			Version: schemaFile.Version,
		}, nil
	}
}

// GetSchemaFile return main schema file if schema id is defined; otherwise return the original schema id (possibly the file path)
func GetSchemaFile(schemaType string, name string) (*modules.Files, error) {
	registry.RLock()
	defer registry.RUnlock()
	if _, ok := registry.schemas[schemaType]; !ok {
		return nil, fmt.Errorf("schema type %s not found in registry", schemaType)
	}
	if _, ok := registry.schemas[schemaType][name]; !ok {
		return nil, fmt.Errorf("schema type %s id %s not found in registry", schemaType, name)
	}
	schemaFile := registry.schemas[schemaType][name]
	return schemaFile, nil
}

func DeleteSchema(schemaType string, name string) error {
	registry.Lock()
	defer registry.Unlock()
	if _, ok := registry.schemas[schemaType]; !ok {
		return fmt.Errorf("schema type %s not found", schemaType)
	}
	if _, ok := registry.schemas[schemaType][name]; !ok {
		return fmt.Errorf("schema %s.%s not found", schemaType, name)
	}
	schemaFile := registry.schemas[schemaType][name]
	err := doDelete(name, schemaFile)
	if err != nil {
		return err
	}
	delete(registry.schemas[schemaType], name)
	removeSchemaInstallScript(schemaType, name)
	return nil
}

func doDelete(name string, schemaFile *modules.Files) error {
	// If the schema is a folder, delete the folder otherwise delete the single file
	if schemaFile.SchemaFile != "" {
		err := os.Remove(schemaFile.SchemaFile)
		if err != nil {
			conf.Log.Errorf("cannot delete schema file %s: %s", schemaFile.SchemaFile, err)
		}
		supportingDir := filepath.Join(filepath.Dir(schemaFile.SchemaFile), name)
		if ff, _ := os.Stat(supportingDir); ff != nil && ff.IsDir() {
			err = os.RemoveAll(supportingDir)
			if err != nil {
				conf.Log.Errorf("cannot delete schema supporting files %s: %s", supportingDir, err)
			}
		}
	}
	if schemaFile.SoFile != "" {
		err := os.Remove(schemaFile.SoFile)
		if err != nil {
			conf.Log.Errorf("cannot delete schema so file %s: %s", schemaFile.SoFile, err)
		}
	}
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
		_ = json.Unmarshal(cast.StringToBytes(value), info)
		_ = DeleteSchema(info.Type, key)
	}
}

func hasInstallFlag() bool {
	val := ""
	found, _ := schemaDb.Get(BOOT_INSTALL, &val)
	return found
}

func clearInstallFlag() {
	_ = schemaDb.Delete(BOOT_INSTALL)
}

func ImportSchema(ctx context.Context, schema map[string]string) map[string]string {
	if len(schema) == 0 {
		return nil
	}
	errMap := map[string]string{}
	for k, v := range schema {
		select {
		case <-ctx.Done():
			return errMap
		default:
		}
		err := schemaDb.Set(k, v)
		if err != nil {
			errMap[k] = err.Error()
		}
	}
	// set the flag to install the plugins when eKuiper reboot
	err := schemaDb.Set(BOOT_INSTALL, BOOT_INSTALL)
	if err != nil {
		errMap["flag"] = err.Error()
	}
	return errMap
}

// SchemaPartialImport compare the schema to be installed and the one in database
// if not exist in database, install;
// if existed, ignore
func SchemaPartialImport(ctx context.Context, schemas map[string]string) map[string]string {
	errMap := map[string]string{}
	for k, v := range schemas {
		select {
		case <-ctx.Done():
			return errMap
		default:
		}
		schemaScript := ""
		found, _ := schemaDb.Get(k, &schemaScript)
		if !found {
			err := schemaRegisterForImport(k, v)
			if err != nil {
				errMap[k] = err.Error()
			}
		}
	}
	return errMap
}

func schemaRegisterForImport(k, v string) error {
	info := &Info{}
	err := json.Unmarshal(cast.StringToBytes(v), info)
	if err != nil {
		return err
	}
	err = CreateOrUpdateSchema(info)
	if err != nil {
		return err
	}
	return nil
}

func schemaInstallWhenReboot() {
	allPlgs, err := schemaDb.All()
	if err != nil {
		return
	}

	delete(allPlgs, BOOT_INSTALL)
	_ = schemaStatusDb.Clean()

	for k, v := range allPlgs {
		err := schemaRegisterForImport(k, v)
		if err != nil {
			_ = schemaStatusDb.Set(k, err.Error())
		}
	}
}

func storeSchemaInstallScript(info *Info) {
	key := info.Type + "_" + info.Name
	val := info.InstallScript()
	_ = schemaDb.Set(key, val)
}

func removeSchemaInstallScript(schemaType string, name string) {
	key := schemaType + "_" + name
	_ = schemaDb.Delete(key)
}

func GetSchemaInstallScript(key string) (string, string) {
	var script string
	schemaDb.Get(key, &script)
	return key, script
}
