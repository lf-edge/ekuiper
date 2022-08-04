package wasm

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/meta"
	"github.com/lf-edge/ekuiper/internal/pkg/filex"
	"github.com/lf-edge/ekuiper/internal/plugin"
	"github.com/lf-edge/ekuiper/internal/plugin/wasm/runtime"
	"io/ioutil"
	"path"
	"path/filepath"
	"sync"
)

var manager *Manager

type Manager struct {
	pluginDir string
	etcDir    string
	reg       *registry
}

func InitManager() (*Manager, error) {
	pluginDir, err := conf.GetPluginsLoc()
	if err != nil {
		fmt.Println("[internal][wasm] cannot find plugins folder:", err)
		return nil, fmt.Errorf("cannot find plugins folder: %s", err)
	}
	etcDir, err := conf.GetConfLoc()
	if err != nil {
		fmt.Println("[internal][plugin][wasm] cannot find etc folder: ", err)
		return nil, fmt.Errorf("cannot find etc folder: %s", err)
	}
	fmt.Println("[internal][plugin][wasm][InitManager] etcDir: ", etcDir) // etcDir : "etc"
	registry := &registry{
		RWMutex:   sync.RWMutex{},
		plugins:   make(map[string]*PluginInfo),
		functions: make(map[string]string),
	}
	// Read plugin info from file system
	pluginDir = filepath.Join(pluginDir, "wasm")
	fmt.Println("[internal][plugin][wasm][InitManager] pluginDir: ", pluginDir) // "plugins"
	m := &Manager{
		pluginDir: pluginDir,
		etcDir:    etcDir,
		reg:       registry,
	}
	err = m.syncRegistry()
	if err != nil {
		fmt.Println("[internal][plugin][wasm][InitManager] syncRegistry err: ", err)
		return nil, err
	}
	manager = m
	return m, nil
}

func MockManager(plugins map[string]*PluginInfo) (*Manager, error) {
	registry := &registry{
		RWMutex:   sync.RWMutex{},
		plugins:   make(map[string]*PluginInfo),
		functions: make(map[string]string),
	}
	for name, pi := range plugins {
		err := pi.Validate(name)
		if err != nil {
			return nil, err
		}
		registry.Set(name, pi)
	}
	return &Manager{reg: registry}, nil
}

func (m *Manager) syncRegistry() error {
	fmt.Println("[internal][plugin][wasm][syncRegistry] start :")
	files, err := ioutil.ReadDir(m.pluginDir)
	if err != nil {
		return fmt.Errorf("read path '%s' error: %v", m.pluginDir, err)
	}
	fmt.Println("[internal][plugin][wasm][syncRegistry] files: ", files)
	// files:  [0xc000143520]
	for _, file := range files {
		if file.IsDir() { // is a Dir
			//err := m.parsePlugin(file.Name())
			//if err != nil {
			//	conf.Log.Warn(err)
			//}
			filename := file.Name()
			fmt.Println("[internal][plugin][wasm][syncRegistry] filename: ", filename)
			err := m.parsePlugin(filename) // fibonacci
			if err != nil {
				conf.Log.Warn(err)
			}
		} else {
			conf.Log.Warnf("find file `%s`, wasm plugin must be a directory", file.Name())
		}
	}
	return nil
}

func (m *Manager) parsePlugin(name string) error {
	pi, err := m.parsePluginJson(name) // fibonacci
	if err != nil {
		return err
	}
	return m.doRegistry(name, pi, true)
}

func (m *Manager) parsePluginJson(name string) (*PluginInfo, error) {
	jsonPath := filepath.Join(m.pluginDir, name, name+".json")
	pi := &PluginInfo{PluginMeta: runtime.PluginMeta{Name: name}}
	//fmt.Println("[internal][plugin][wasm][parsePluginJson] pi: ", pi)
	err := filex.ReadJsonUnmarshal(jsonPath, pi)
	//fmt.Println("[internal][plugin][wasm][parsePluginJson] jsonPath: ", jsonPath)
	fmt.Println("[internal][plugin][wasm][parsePluginJson] pi: ", pi)
	if err != nil {
		// json file `/home/erfenjiao/ekuiper/plugins/wasm/fibonacci/fibonacci.json`
		return nil, fmt.Errorf("cannot read json file `%s` when loading wasm plugins: %v", jsonPath, err)
	}
	if err := pi.Validate(name); err != nil {
		return nil, err
	}
	if result, ok := m.reg.Get(pi.Name); ok {
		fmt.Println("result: ", result)
		return nil, fmt.Errorf("Wasm plugin %s already exists", pi.Name)
	}
	return pi, nil
}

//func (m *Manager) Register(p plugin.Plugin) error {
//	name, uri, shellParas := p.GetName(), p.GetFile(), p.GetShellParas()
//	fmt.Println("[internal][plugin][wasm][Register] name: ", name)
//	fmt.Println("[internal][plugin][wasm][Register] uri: ", uri)
//	fmt.Println("[internal][plugin][wasm][Register] shellParas: ", shellParas)
//	if name == " " {
//		return fmt.Errorf("invalid name %s: should not be empty", name)
//	}
//	if !httpx.IsValidUrl(uri) || !strings.HasSuffix(uri, ".zip") {
//		return fmt.Errorf("invalid uri %s", uri)
//	}
//
//	if _, ok := m.reg.Get(name); ok {
//		return fmt.Errorf("invalid name %s: duplicate", name)
//	}
//
//}

func (m *Manager) doRegistry(name string, pi *PluginInfo, isInit bool) error {
	exeAbs := filepath.Clean(filepath.Join(m.pluginDir, name, pi.Executable))
	fmt.Println("[internal][plugin][wasm][doRegistry] exeAbs: ", exeAbs)
	//if _, err := os.Stat(exeAbs); err != nil {
	//	return fmt.Errorf("cannot find executable `%s` when loading wasm plugins: %v", exeAbs, err)
	//}
	pi.Executable = exeAbs
	// name: /home/erfenjiao/ekuiper/plugins/wasm/...(file)
	m.reg.Set(name, pi)

	if !isInit {
		for _, s := range pi.Functions {
			if err := meta.ReadFuncMetaFile(path.Join(m.etcDir, plugin.PluginTypes[plugin.FUNCTION], s+`.json`), true); nil != err {
				conf.Log.Errorf("read function json file:%v", err)
			}
		}
	}
	fmt.Println("pi: ", pi)
	// pi:  &{{fibonacci v1.0.0 go /home/erfenjiao/ekuiper/plugins/wasm/fibonacci/fib /home/erfenjiao/ekuiper/plugins/wasm/fib/fibonacci.wasm wasmedge} [fib]}
	conf.Log.Infof("Installed wasm plugin %s successfully", name)
	return nil
}

func (m *Manager) GetPluginMeta(pt plugin.PluginType, symbolName string) (*runtime.PluginMeta, bool) {
	pname, ok := m.reg.GetSymbol(pt, symbolName)
	if !ok {
		return nil, false
	}
	pinfo, ok := m.reg.Get(pname)
	if !ok {
		return nil, false
	}
	return &pinfo.PluginMeta, true
}

func (m *Manager) GetPluginInfo(pluginName string) (*PluginInfo, bool) {
	pinfo, ok := m.reg.Get(pluginName)
	if !ok {
		return nil, false
	}
	return pinfo, true
}

// registry install

// delete
//func (m *Manager) Delete(name string) error {
//	pinfo, ok := m.reg.Get(name)
//	if !ok {
//		return fmt.Errorf("wasm plugin %s is not found", name)
//	}
//	m.reg.Delete(name)
//	//delete files and unintall plugin metas
//	for _, s := range pinfo.Functions {
//
//	}
//}

func (m *Manager) List() []*PluginInfo {
	return m.reg.List()
}
