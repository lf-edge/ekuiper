package services

import (
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/common/kv"
	"io/ioutil"
	"path"
	"path/filepath"
	"sync"
)

var (
	once      sync.Once
	mutex     sync.Mutex
	singleton *Manager //Do not call this directly, use NewServiceManager
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

func NewServiceManager() (*Manager, error) {
	mutex.Lock()
	defer mutex.Unlock()
	if singleton == nil {
		etcDir, err := common.GetLoc("/etc/services")
		if err != nil {
			return nil, fmt.Errorf("cannot find etc/services folder: %s", err)
		}
		dbDir, err := common.GetDataLoc()
		if err != nil {
			return nil, fmt.Errorf("cannot find db folder: %s", err)
		}
		sdb := kv.GetDefaultKVStore(path.Join(dbDir, "services"))
		fdb := kv.GetDefaultKVStore(path.Join(dbDir, "serviceFuncs"))
		err = sdb.Open()
		if err != nil {
			return nil, fmt.Errorf("cannot open service db: %s", err)
		}
		err = fdb.Open()
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
	if !singleton.loaded {
		err := singleton.initByFiles()
		return singleton, err
	}
	return singleton, nil
}

/**
 * This function will parse the service definition json files in etc/services.
 * It will validate all json files and their schemaFiles. If invalid, it just prints
 * an error log and ignore. So it is possible that only valid service definition are
 * parsed and available.
 *
 * NOT threadsafe, must run in lock
 */
func (m *Manager) initByFiles() error {
	common.Log.Debugf("init service manager")
	files, err := ioutil.ReadDir(m.etcDir)
	if nil != err {
		return err
	}
	// Parse schemas in batch. So we have 2 loops. First loop to collect files and the second to save the result.
	for _, file := range files {
		baseName := filepath.Base(file.Name())
		if filepath.Ext(baseName) == ".json" {
			serviceConf := &conf{}
			err := common.ReadJsonUnmarshal(filepath.Join(m.etcDir, baseName), serviceConf)
			if err != nil {
				common.Log.Errorf("Parse services file %s failed: %v", baseName, err)
				continue
			}
			//TODO validate serviceConf
			serviceName := baseName[0 : len(baseName)-5]
			info := &serviceInfo{
				About:      serviceConf.About,
				Interfaces: make(map[string]*interfaceInfo),
			}
			for _, binding := range serviceConf.Interfaces {
				binding.SchemaFile = path.Join(m.etcDir, "schemas", binding.SchemaFile)
				desc, err := parse(binding.SchemaType, binding.SchemaFile)
				if err != nil {
					common.Log.Errorf("Fail to parse schema file %s: %v", binding.SchemaFile, err)
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
				info.Interfaces[binding.Name] = &interfaceInfo{
					Desc:     binding.Description,
					Addr:     binding.Address,
					Protocol: binding.Protocol,
					Schema: &schemaInfo{
						SchemaType: binding.SchemaType,
						SchemaFile: binding.SchemaFile,
					},
					Functions: functions,
				}
				err = m.serviceKV.Set(serviceName, info)
				if err != nil {
					return fmt.Errorf("fail to save the parsing result: %v", err)
				}
				for i, f := range functions {
					err := m.functionKV.Set(f, &functionContainer{
						ServiceName:   serviceName,
						InterfaceName: binding.Name,
						MethodName:    methods[i],
					})
					if err != nil {
						common.Log.Errorf("fail to save the function mapping for %s, the function is not available: %v", f, err)
					}
				}
			}
		}
	}
	m.loaded = true
	return nil
}

func (m *Manager) HasFunction(name string) bool {
	_, ok := m.getFunction(name)
	common.Log.Debugf("found external function %s? %v ", name, ok)
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
			common.Log.Errorf("failed to get service function %s from kv: %v", name, err)
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
			common.Log.Errorf("failed to get service %s from kv: %v", name, err)
			return nil, false
		}
		if ok {
			m.serviceBuf.Store(name, r)
		}
		return r, ok
	}
}

func ValidateFunction(name string) {

}

func (m *Manager) InvokeFunction(name string, params []interface{}) (interface{}, bool) {
	f, ok := m.getFunction(name)
	if !ok {
		return fmt.Errorf("service function %s not found", name), false
	}
	s, ok := m.getService(f.ServiceName)
	if !ok {
		return fmt.Errorf("service function %s's service %s not found", name, f.ServiceName), false
	}
	i, ok := s.Interfaces[f.InterfaceName]
	if !ok {
		return fmt.Errorf("service function %s's interface %s not found", name, f.InterfaceName), false
	}
	e, err := m.getExecutor(f.InterfaceName, i)
	if err != nil {
		return fmt.Errorf("fail to initiate the executor for %s: %v", f.InterfaceName, err), false
	}
	if r, err := e.InvokeFunction(f.MethodName, params); err != nil {
		return err, false
	} else {
		return r, true
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
