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

package portable

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/meta"
	"github.com/lf-edge/ekuiper/internal/pkg/filex"
	"github.com/lf-edge/ekuiper/internal/pkg/httpx"
	"github.com/lf-edge/ekuiper/internal/plugin"
	"github.com/lf-edge/ekuiper/internal/plugin/portable/runtime"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

var manager *Manager

type Manager struct {
	pluginDir string
	etcDir    string
	reg       *registry // can be replaced with kv
}

// InitManager must only be called once
func InitManager() (*Manager, error) {
	pluginDir, err := conf.GetPluginsLoc()
	if err != nil {
		return nil, fmt.Errorf("cannot find plugins folder: %s", err)
	}
	etcDir, err := conf.GetConfLoc()
	if err != nil {
		return nil, fmt.Errorf("cannot find etc folder: %s", err)
	}
	registry := &registry{
		RWMutex:   sync.RWMutex{},
		plugins:   make(map[string]*PluginInfo),
		sources:   make(map[string]string),
		sinks:     make(map[string]string),
		functions: make(map[string]string),
	}
	// Read plugin info from file system
	pluginDir = filepath.Join(pluginDir, "portable")
	m := &Manager{
		pluginDir: pluginDir,
		etcDir:    etcDir,
		reg:       registry,
	}
	err = m.syncRegistry()
	if err != nil {
		return nil, err
	}
	manager = m
	return m, nil
}

func GetManager() *Manager {
	return manager
}

func (m *Manager) syncRegistry() error {
	files, err := ioutil.ReadDir(m.pluginDir)
	if err != nil {
		return fmt.Errorf("read path '%s' error: %v", m.pluginDir, err)
	}
	for _, file := range files {
		if file.IsDir() {
			err := m.parsePlugin(file.Name())
			if err != nil {
				conf.Log.Warn(err)
			}
		} else {
			conf.Log.Warnf("find file `%s`, portable plugin must be a directory", file.Name())
		}
	}
	return nil
}

func (m *Manager) parsePlugin(name string) error {
	pi, err := m.parsePluginJson(name)
	if err != nil {
		return err
	}
	return m.doRegister(name, pi, true)
}

func (m *Manager) doRegister(name string, pi *PluginInfo, isInit bool) error {
	exeAbs := filepath.Clean(filepath.Join(m.pluginDir, name, pi.Executable))
	if _, err := os.Stat(exeAbs); err != nil {
		return fmt.Errorf("cannot find executable `%s` when loading portable plugins: %v", exeAbs, err)
	}
	pi.Executable = exeAbs
	m.reg.Set(name, pi)

	if !isInit {
		for _, s := range pi.Sources {
			if err := meta.ReadSourceMetaFile(path.Join(m.etcDir, plugin.PluginTypes[plugin.SOURCE], s+`.json`), true); nil != err {
				conf.Log.Errorf("read source json file:%v", err)
			}
		}
		for _, s := range pi.Sinks {
			if err := meta.ReadSinkMetaFile(path.Join(m.etcDir, plugin.PluginTypes[plugin.SINK], s+`.json`), true); nil != err {
				conf.Log.Errorf("read sink json file:%v", err)
			}
		}
		for _, s := range pi.Functions {
			if err := meta.ReadFuncMetaFile(path.Join(m.etcDir, plugin.PluginTypes[plugin.FUNCTION], s+`.json`), true); nil != err {
				conf.Log.Errorf("read function json file:%v", err)
			}
		}
	}

	conf.Log.Infof("Installed portable plugin %s successfully", name)
	return nil
}

func (m *Manager) parsePluginJson(name string) (*PluginInfo, error) {
	jsonPath := filepath.Join(m.pluginDir, name, name+".json")
	pi := &PluginInfo{PluginMeta: runtime.PluginMeta{Name: name}}
	err := filex.ReadJsonUnmarshal(jsonPath, pi)
	if err != nil {
		return nil, fmt.Errorf("cannot read json file `%s` when loading portable plugins: %v", jsonPath, err)
	}
	if err := pi.Validate(name); err != nil {
		return nil, err
	}
	if _, ok := m.reg.Get(pi.Name); ok {
		return nil, fmt.Errorf("portable plugin %s already exists", pi.Name)
	}
	return pi, nil
}

func (m *Manager) Register(p plugin.Plugin) error {
	name, uri, shellParas := p.GetName(), p.GetFile(), p.GetShellParas()
	name = strings.Trim(name, " ")
	if name == "" {
		return fmt.Errorf("invalid name %s: should not be empty", name)
	}
	if !httpx.IsValidUrl(uri) || !strings.HasSuffix(uri, ".zip") {
		return fmt.Errorf("invalid uri %s", uri)
	}

	if _, ok := m.reg.Get(name); ok {
		return fmt.Errorf("invalid name %s: duplicate", name)
	}

	zipPath := path.Join(m.pluginDir, name+".zip")
	//clean up: delete zip file and unzip files in error
	defer os.Remove(zipPath)
	//download
	err := httpx.DownloadFile(zipPath, uri)
	if err != nil {
		return fmt.Errorf("fail to download file %s: %s", uri, err)
	}
	//unzip and copy to destination
	err = m.install(name, zipPath, shellParas)
	if err != nil { //Revert for any errors
		return fmt.Errorf("fail to install plugin: %s", err)
	}
	return nil
}

func (m *Manager) install(name, src string, shellParas []string) (resultErr error) {
	var (
		jsonName     = name + ".json"
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
	if pi == nil {
		return fmt.Errorf("missing or invalid json file %s", jsonName)
	}
	if err = pi.Validate(name); err != nil {
		return err
	}
	if _, ok := m.reg.Get(pi.Name); ok {
		return fmt.Errorf("portable plugin %s already exists", pi.Name)
	}

	requiredFiles = append(requiredFiles, pi.Executable)
	for _, src := range pi.Sources {
		requiredFiles = append(requiredFiles, fmt.Sprintf("sources/%s.yaml", src))
	}

	// file copying
	d := filepath.Clean(pluginTarget)
	if _, err := os.Stat(d); os.IsNotExist(err) {
		err = os.MkdirAll(d, 0755)
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
		//run install script if there is
		spath := path.Join(pluginTarget, "install.sh")
		shellParas = append(shellParas, spath)
		if 1 != len(shellParas) {
			copy(shellParas[1:], shellParas[0:])
			shellParas[0] = spath
		}
		cmd := exec.Command("/bin/sh", shellParas...)
		var outb, errb bytes.Buffer
		cmd.Stdout = &outb
		cmd.Stderr = &errb
		err = cmd.Run()
		if err != nil {
			return err
		}
	}
	return m.doRegister(name, pi, false)
}

func (m *Manager) List() []*PluginInfo {
	return m.reg.List()
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

func (m *Manager) Delete(name string) error {
	pinfo, ok := m.reg.Get(name)
	if !ok {
		return fmt.Errorf("portable plugin %s is not found", name)
	}
	// unregister the plugin
	m.reg.Delete(name)
	// delete files and uninstall metas
	for _, s := range pinfo.Sources {
		p := path.Join(m.etcDir, plugin.PluginTypes[plugin.SOURCE], s+".yaml")
		os.Remove(p)
		p = path.Join(m.etcDir, plugin.PluginTypes[plugin.SOURCE], s+".json")
		os.Remove(p)
		meta.UninstallSource(s)
	}
	for _, s := range pinfo.Sinks {
		p := path.Join(m.etcDir, plugin.PluginTypes[plugin.SINK], s+".json")
		os.Remove(p)
		meta.UninstallSink(s)
	}
	for _, s := range pinfo.Functions {
		p := path.Join(m.etcDir, plugin.PluginTypes[plugin.FUNCTION], s+".json")
		os.Remove(p)
		meta.UninstallFunc(s)
	}
	_ = os.RemoveAll(path.Join(m.pluginDir, name))
	return nil
}
