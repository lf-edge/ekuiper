// Copyright erfenjiao, 630166475@qq.com.
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

package wasm

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/filex"
	"github.com/lf-edge/ekuiper/internal/pkg/httpx"
	"github.com/lf-edge/ekuiper/internal/plugin"
	"github.com/lf-edge/ekuiper/internal/plugin/wasm/runtime"
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
	// fmt.Println("[internal][plugin][wasm][InitManager] etcDir: ", etcDir)
	registry := &registry{
		RWMutex:   sync.RWMutex{},
		plugins:   make(map[string]*PluginInfo),
		functions: make(map[string]string),
	}
	// Read plugin info from file system
	pluginDir = filepath.Join(pluginDir, "wasm")
	fmt.Println("[internal][plugin][wasm][InitManager] pluginDir: ", pluginDir)
	m := &Manager{
		pluginDir: pluginDir,
		etcDir:    etcDir,
		reg:       registry,
	}
	err = m.syncRegistry()
	if err != nil {
		// fmt.Println("[internal][plugin][wasm][InitManager] syncRegistry err: ", err)
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
		wasmPath := filepath.Join(name + ".wasm")
		fmt.Println("wasmPath:", wasmPath)
		pi.WasmFile = wasmPath
		registry.plugins[name] = pi
		for _, s := range pi.Functions {
			registry.functions[s] = name
		}
	}
	return &Manager{reg: registry}, nil
}

func (m *Manager) syncRegistry() error {
	files, err := ioutil.ReadDir(m.pluginDir)
	if err != nil {
		return fmt.Errorf("read path '%s' error: %v", m.pluginDir, err)
	}
	for _, file := range files {
		if file.IsDir() { // is a Dir
			err := m.parsePlugin(file.Name())
			if err != nil {
				conf.Log.Warn(err)
			}
			filename := file.Name()
			err = m.parsePlugin(filename)
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
	pi, err := m.parsePluginJson(name)
	if err != nil {
		return err
	}
	return m.doRegistry(name, pi, true)
}

func (m *Manager) parsePluginJson(name string) (*PluginInfo, error) {
	jsonPath := filepath.Join(m.pluginDir, name, name+".json")
	wasmPath := filepath.Join(m.pluginDir, name, name+".wasm")
	pi := &PluginInfo{PluginMeta: runtime.PluginMeta{Name: name}}
	pi.WasmFile = wasmPath
	err := filex.ReadJsonUnmarshal(jsonPath, pi)
	if err != nil {
		return nil, fmt.Errorf("cannot read json file `%s` when loading wasm plugins: %v", jsonPath, err)
	}
	if err := pi.Validate(name); err != nil {
		return nil, err
	}
	if result, ok := m.reg.Get(pi.Name); ok {
		return nil, fmt.Errorf("wasm plugin %s already exists. result : %s", pi.Name, result)
	}
	return pi, nil
}

func (m *Manager) Register(p plugin.Plugin) error {
	name, uri, shellParas := p.GetName(), p.GetFile(), p.GetShellParas()
	name = strings.Trim(name, " ")
	if name == " " {
		return fmt.Errorf("invalid name %s: should not be empty", name)
	}

	if !httpx.IsValidUrl(uri) || !strings.HasSuffix(uri, ".zip") {
		return fmt.Errorf("invalid uri %s", uri)
	}

	fmt.Println("name :", name)
	if _, ok := m.reg.Get(name); ok {
		return fmt.Errorf("invalid name %s: duplicate", name)
	}
	zipPath := path.Join(m.pluginDir, name+".zip")
	// clean up: delete zip file and unzip files in error
	defer os.Remove(zipPath)
	// download
	err := httpx.DownloadFile(zipPath, uri)
	if err != nil {
		return fmt.Errorf("fail to download file %s: %s", uri, err)
	}
	// unzip and copy to destination
	err = m.install(name, zipPath, shellParas)
	if err != nil { // Revert for any errors
		return fmt.Errorf("fail to install plugin: %s", err)
	}
	return nil
}

func (m *Manager) doRegistry(name string, pi *PluginInfo, isInit bool) error {
	m.reg.Set(name, pi)

	//if !isInit {
	//	for _, s := range pi.Functions {
	//		if err := meta.ReadFuncMetaFile(path.Join(m.etcDir, plugin.PluginTypes[plugin.FUNCTION], s+`.json`), true); nil != err {
	//			conf.Log.Errorf("read function json file:%v", err)
	//		}
	//	}
	//}
	conf.Log.Infof("[doRegistry] Installed wasm plugin %s successfully", name)
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

func (m *Manager) install(name, src string, shellParas []string) (resultErr error) {
	var (
		jsonName = name + ".json"
		// wasmName     = name + ".wasm"
		pluginTarget = filepath.Join(m.pluginDir, name)
		// The map of install files. Used to check if all required files are installed and for reverting
		installedMap  = make(map[string]string)
		requiredFiles = []string{jsonName}
	)
	defer func() {
		// remove all installed files if err happens
		if resultErr != nil {
			for _, p := range installedMap {
				_ = os.Remove(p)
			}
			_ = os.Remove(pluginTarget)
		}
	}()
	r, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer r.Close()
	var pi *PluginInfo
	// Parse json file
	for _, file := range r.File {
		if file.Name == jsonName {
			jf, err := file.Open()
			if err != nil {
				err = fmt.Errorf("invalid json file %s: %s", jsonName, err)
				return err
			}
			pi = &PluginInfo{PluginMeta: runtime.PluginMeta{Name: name}}
			allBytes, err := ioutil.ReadAll(jf)
			if err != nil {
				return err
			}
			err = json.Unmarshal(allBytes, pi)
			if err != nil {
				return err
			}
		}
	}
	//if pi.WasmFile == "" {
	//	return fmt.Errorf("missing or invalid wasm file %s", pi.WasmFile)
	//}
	if pi == nil {
		return fmt.Errorf("missing or invalid json file %s", jsonName)
	}
	if err = pi.Validate(name); err != nil {
		return err
	}
	if _, ok := m.reg.Get(pi.Name); ok {
		return fmt.Errorf("wasm plugin %s already exists", pi.Name)
	}

	// file copying
	d := filepath.Clean(pluginTarget)
	if _, err := os.Stat(d); os.IsNotExist(err) {
		err = os.MkdirAll(d, 0o755)
		if err != nil {
			return err
		}
	}

	needInstall := false
	target := ""
	for _, file := range r.File {
		fileName := file.Name
		if strings.HasPrefix(fileName, "sources/") || strings.HasPrefix(fileName, "sinks/") || strings.HasPrefix(fileName, "functions/") {
			target = path.Join(m.etcDir, fileName)
		} else {
			target = path.Join(pluginTarget, fileName)
			if fileName == "install.sh" {
				needInstall = true
			}
		}
		err = filex.UnzipTo(file, target)
		if err != nil {
			return err
		}
		if !file.FileInfo().IsDir() {
			installedMap[fileName] = target
		}
	}

	// Check if all files installed
	for _, rf := range requiredFiles {
		if _, ok := installedMap[rf]; !ok {
			return fmt.Errorf("missing %s", rf)
		}
	}

	if needInstall {
		// run install script if there is
		spath := path.Join(pluginTarget, "install.sh")
		shellParas = append(shellParas, spath)
		if 1 != len(shellParas) {
			copy(shellParas[1:], shellParas[0:])
			shellParas[0] = spath
		}
		cmd := exec.Command("/bin/sh", shellParas...)
		conf.Log.Infof("run install script %s", strings.Join(shellParas, " "))
		var outb, errb bytes.Buffer
		cmd.Stdout = &outb
		cmd.Stderr = &errb
		err = cmd.Run()
		if err != nil {
			return fmt.Errorf(`err:%v stdout:%s stderr:%s`, err, outb.String(), errb.String())
		} else {
			conf.Log.Infof(`install script ouput: %s`, outb.String())
		}
	}
	return m.doRegistry(name, pi, false)
}

// delete

func (m *Manager) Delete(name string) error {
	pinfo, ok := m.reg.Get(name)
	if !ok {
		return fmt.Errorf("wasm plugin %s is not found", name)
	}
	// unregister the plugin
	m.reg.Delete(name)
	// delete files and uninstall metas
	for _, s := range pinfo.Functions {
		p := path.Join(m.etcDir, plugin.PluginTypes[plugin.FUNCTION], s+".json")
		os.Remove(p)
	}
	_ = os.RemoveAll(path.Join(m.pluginDir, name))
	return nil
}

func (m *Manager) List() []*PluginInfo {
	return m.reg.List()
}
