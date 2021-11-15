// Copyright 2021 EMQ Technologies Co., Ltd.
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

package service

import (
	"archive/zip"
	"fmt"
	kconf "github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/filex"
	"github.com/lf-edge/ekuiper/internal/pkg/httpx"
	"github.com/lf-edge/ekuiper/internal/pkg/store"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/kv"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

var (
	once      sync.Once
	mutex     sync.Mutex
	singleton *Manager //Do not call this directly, use GetServiceManager
)

type Manager struct {
	executorPool *sync.Map // The pool of executors
	loaded       bool
	serviceBuf   *sync.Map
	functionBuf  *sync.Map

	etcDir     string
	serviceKV  kv.KeyValue
	functionKV kv.KeyValue
}

func InitManager() (*Manager, error) {
	mutex.Lock()
	defer mutex.Unlock()
	if singleton == nil {
		dir := "etc/services"
		if kconf.IsTesting {
			dir = "service/test"
		}
		etcDir, err := kconf.GetLoc(dir)
		if err != nil {
			return nil, fmt.Errorf("cannot find etc/services folder: %s", err)
		}
		err, sdb := store.GetKV("services")
		if err != nil {
			return nil, fmt.Errorf("cannot open service db: %s", err)
		}
		err, fdb := store.GetKV("serviceFuncs")
		if err != nil {
			return nil, fmt.Errorf("cannot open function db: %s", err)
		}
		singleton = &Manager{
			executorPool: &sync.Map{},
			serviceBuf:   &sync.Map{},
			functionBuf:  &sync.Map{},

			etcDir:     etcDir,
			serviceKV:  sdb,
			functionKV: fdb,
		}
	}
	if !singleton.loaded && !kconf.IsTesting { // To boost the testing perf
		err := singleton.InitByFiles()
		return singleton, err
	}
	return singleton, nil
}

func GetManager() *Manager {
	return singleton
}

/**
 * This function will parse the service definition json files in etc/services.
 * It will validate all json files and their schemaFiles. If invalid, it just prints
 * an error log and ignore. So it is possible that only valid service definition are
 * parsed and available.
 *
 * NOT threadsafe, must run in lock
 */
func (m *Manager) InitByFiles() error {
	kconf.Log.Debugf("init service manager")
	files, err := ioutil.ReadDir(m.etcDir)
	if nil != err {
		return err
	}
	// Parse schemas in batch. So we have 2 loops. First loop to collect files and the second to save the result.
	for _, file := range files {
		baseName := filepath.Base(file.Name())
		if filepath.Ext(baseName) == ".json" {
			err := m.initFile(baseName)
			if err != nil {
				kconf.Log.Errorf("%v", err)
				continue
			}
		}
	}
	m.loaded = true
	return nil
}

func (m *Manager) initFile(baseName string) error {
	serviceConf := &conf{}
	err := filex.ReadJsonUnmarshal(filepath.Join(m.etcDir, baseName), serviceConf)
	if err != nil {
		return fmt.Errorf("parse services file %s failed: %v", baseName, err)
	}
	//TODO validate serviceConf
	serviceName := baseName[0 : len(baseName)-5]
	info := &serviceInfo{
		About:      serviceConf.About,
		Interfaces: make(map[string]*interfaceInfo),
	}
	for name, binding := range serviceConf.Interfaces {
		desc, err := parse(binding.SchemaType, binding.SchemaFile)
		if err != nil {
			return fmt.Errorf("Fail to parse schema file %s: %v", binding.SchemaFile, err)
		}

		// setting function alias
		aliasMap := make(map[string]string)
		for _, finfo := range binding.Functions {
			aliasMap[finfo.ServiceName] = finfo.Name
		}

		methods := desc.GetFunctions()
		functions := make([]string, len(methods))
		for i, f := range methods {
			fname := f
			if a, ok := aliasMap[f]; ok {
				fname = a
			}
			functions[i] = fname
		}
		info.Interfaces[name] = &interfaceInfo{
			Desc:     binding.Description,
			Addr:     binding.Address,
			Protocol: binding.Protocol,
			Schema: &schemaInfo{
				SchemaType: binding.SchemaType,
				SchemaFile: binding.SchemaFile,
			},
			Functions: functions,
			Options:   binding.Options,
		}
		for i, f := range functions {
			err := m.functionKV.Set(f, &functionContainer{
				ServiceName:   serviceName,
				InterfaceName: name,
				MethodName:    methods[i],
			})
			if err != nil {
				kconf.Log.Errorf("fail to save the function mapping for %s, the function is not available: %v", f, err)
			}
		}
	}
	err = m.serviceKV.Set(serviceName, info)
	if err != nil {
		return fmt.Errorf("fail to save the parsing result: %v", err)
	}
	return nil
}

// Start Implement FunctionFactory

func (m *Manager) HasFunctionSet(_ string) bool {
	return false
}

func (m *Manager) Function(name string) (api.Function, error) {
	f, ok := m.getFunction(name)
	if !ok {
		return nil, fmt.Errorf("service function %s not found", name)
	}
	s, ok := m.getService(f.ServiceName)
	if !ok {
		return nil, fmt.Errorf("service function %s's service %s not found", name, f.ServiceName)
	}
	i, ok := s.Interfaces[f.InterfaceName]
	if !ok {
		return nil, fmt.Errorf("service function %s's interface %s not found", name, f.InterfaceName)
	}
	// executor is gotten from pool, so all externalFuncs with the same interface share the same executor instance
	e, err := m.getExecutor(f.InterfaceName, i)
	if err != nil {
		return nil, fmt.Errorf("fail to initiate the executor for %s: %v", f.InterfaceName, err)
	}
	return &ExternalFunc{exe: e, methodName: f.MethodName}, nil
}

func (m *Manager) ConvName(funcName string) (string, bool) {
	_, ok := m.getFunction(funcName)
	return funcName, ok
}

// End Implement FunctionFactory

func (m *Manager) HasService(name string) bool {
	_, ok := m.getService(name)
	kconf.Log.Debugf("found external service %s? %v ", name, ok)
	return ok
}

func (m *Manager) getFunction(name string) (*functionContainer, bool) {
	var r *functionContainer
	if t, ok := m.functionBuf.Load(name); ok {
		r = t.(*functionContainer)
		return r, ok
	} else {
		r = &functionContainer{}
		ok, err := m.functionKV.Get(name, r)
		if err != nil {
			kconf.Log.Errorf("failed to get service function %s from kv: %v", name, err)
			return nil, false
		}
		if ok {
			m.functionBuf.Store(name, r)
		}
		return r, ok
	}
}

func (m *Manager) getService(name string) (*serviceInfo, bool) {
	var r *serviceInfo
	if t, ok := m.serviceBuf.Load(name); ok {
		r = t.(*serviceInfo)
		return r, ok
	} else {
		r = &serviceInfo{}
		ok, err := m.serviceKV.Get(name, r)
		if err != nil {
			kconf.Log.Errorf("failed to get service %s from kv: %v", name, err)
			return nil, false
		}
		if ok {
			m.serviceBuf.Store(name, r)
		}
		return r, ok
	}
}

// Each interface maps to an executor
func (m *Manager) getExecutor(name string, info *interfaceInfo) (executor, error) {
	e, ok := m.executorPool.Load(name)
	if !ok {
		ne, err := NewExecutor(info)
		if err != nil {
			return nil, err
		}
		e, _ = m.executorPool.LoadOrStore(name, ne)
	}
	return e.(executor), nil
}

func (m *Manager) deleteServiceFuncs(service string) error {
	if s, ok := m.getService(service); ok {
		for _, i := range s.Interfaces {
			for _, f := range i.Functions {
				_ = m.deleteFunc(service, f)
			}
		}
	}
	return nil
}

func (m *Manager) deleteFunc(service, name string) error {
	f, err := m.GetFunction(name)
	if err != nil {
		return err
	}
	if f.ServiceName == service {
		m.functionBuf.Delete(name)
		m.functionKV.Delete(name)
	}
	return nil
}

// ** CRUD of the service files **

type ServiceCreationRequest struct {
	Name string `json:"name"`
	File string `json:"file"`
}

func (m *Manager) List() ([]string, error) {
	return m.serviceKV.Keys()
}

func (m *Manager) Create(r *ServiceCreationRequest) error {
	name, uri := r.Name, r.File
	if ok, _ := m.serviceKV.Get(name, &serviceInfo{}); ok {
		return fmt.Errorf("service %s exist", name)
	}
	if !httpx.IsValidUrl(uri) || !strings.HasSuffix(uri, ".zip") {
		return fmt.Errorf("invalid file path %s", uri)
	}
	zipPath := path.Join(m.etcDir, name+".zip")
	//clean up: delete zip file and unzip files in error
	defer os.Remove(zipPath)
	//download
	err := httpx.DownloadFile(zipPath, uri)
	if err != nil {
		return fmt.Errorf("fail to download file %s: %s", uri, err)
	}
	//unzip and copy to destination
	err = m.unzip(name, zipPath)
	if err != nil {
		return err
	}
	// init file to serviceKV
	return m.initFile(name + ".json")
}

func (m *Manager) Delete(name string) error {
	name = strings.Trim(name, " ")
	if name == "" {
		return fmt.Errorf("invalid name %s: should not be empty", name)
	}
	m.deleteServiceFuncs(name)
	m.serviceBuf.Delete(name)
	err := m.serviceKV.Delete(name)
	if err != nil {
		return err
	}
	path := path.Join(m.etcDir, name+".json")
	err = os.Remove(path)
	if err != nil {
		kconf.Log.Errorf("remove service json fails: %v", err)
	}
	return nil
}

func (m *Manager) Get(name string) (*serviceInfo, error) {
	name = strings.Trim(name, " ")
	if name == "" {
		return nil, fmt.Errorf("invalid name %s: should not be empty", name)
	}
	r, ok := m.getService(name)
	if !ok {
		return nil, fmt.Errorf("can't get the service %s", name)
	}
	return r, nil
}

func (m *Manager) Update(req *ServiceCreationRequest) error {
	err := m.Delete(req.Name)
	if err != nil {
		return err
	}
	return m.Create(req)
}

func (m *Manager) unzip(name, src string) error {
	r, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer r.Close()
	baseName := strings.ToLower(name + ".json")
	// Try unzip
	found := false
	for _, file := range r.File {
		if strings.ToLower(file.Name) == baseName {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("cannot find the json descriptor file %s for service", baseName)
	}
	// unzip
	for _, file := range r.File {
		err := filex.UnzipTo(file, path.Join(m.etcDir, file.Name))
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) ListFunctions() ([]string, error) {
	return m.functionKV.Keys()
}

func (m *Manager) GetFunction(name string) (*functionContainer, error) {
	name = strings.Trim(name, " ")
	if name == "" {
		return nil, fmt.Errorf("invalid name %s: should not be empty", name)
	}
	r, ok := m.getFunction(name)
	if !ok {
		return nil, fmt.Errorf("can't get the service function %s", name)
	}
	return r, nil
}
