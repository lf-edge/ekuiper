// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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

// Manage the loading of both native and portable plugins

package native

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"plugin"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/binder"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/meta"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/filex"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/httpx"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	plugin2 "github.com/lf-edge/ekuiper/v2/internal/plugin"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/kv"
)

// Manager Initialized in the binder
var (
	manager *Manager
	_       binder.SourceFactory = manager
	_       binder.SinkFactory   = manager
	_       binder.FuncFactory   = manager
)

const DELETED = "$deleted"

// Manager is appended only because plugin cannot delete or reload. To delete a plugin, restart the server to reindex
type Manager struct {
	sync.RWMutex
	// 3 maps for source/sink/function. In each map, key is the plugin name, value is the version
	plugins []map[string]string
	// A map from function name to its plugin file name. It is constructed during initialization by reading kv info. All functions must have at least an entry, even the function resizes in a one function plugin.
	symbols map[string]string
	// loaded symbols in current runtime
	runtime map[string]*plugin.Plugin
	// dirs
	pluginDir     string
	pluginConfDir string
	pluginDataDir string
	// the access to func symbols db
	funcSymbolsDb kv.KeyValue
	// the access to plugin install script db
	plgInstallDb kv.KeyValue
	// the access to plugin install status db
	plgStatusDb kv.KeyValue
}

// InitManager must only be called once
func InitManager() (*Manager, error) {
	pluginDir, err := conf.GetPluginsLoc()
	if err != nil {
		return nil, fmt.Errorf("cannot find plugins folder: %s", err)
	}
	etcDir, err := conf.GetConfLoc()
	if err != nil {
		return nil, fmt.Errorf("cannot find data folder: %s", err)
	}
	dataDir, err := conf.GetDataLoc()
	if err != nil {
		return nil, fmt.Errorf("cannot find data folder: %s", err)
	}
	func_db, err := store.GetKV("pluginFuncs")
	if err != nil {
		return nil, fmt.Errorf("error when opening funcSymbolsdb: %v", err)
	}
	plg_db, err := store.GetKV("nativePlugin")
	if err != nil {
		return nil, fmt.Errorf("error when opening nativePlugin: %v", err)
	}
	plg_status_db, err := store.GetKV("nativePluginStatus")
	if err != nil {
		return nil, fmt.Errorf("error when opening nativePluginStatus: %v", err)
	}
	registry := &Manager{symbols: make(map[string]string), funcSymbolsDb: func_db, plgInstallDb: plg_db, plgStatusDb: plg_status_db, pluginDir: pluginDir, pluginConfDir: etcDir, pluginDataDir: dataDir, runtime: make(map[string]*plugin.Plugin)}
	manager = registry

	plugins := make([]map[string]string, 3)
	for i := range plugins {
		names, err := findAll(plugin2.PluginType(i), pluginDir)
		if err != nil {
			return nil, fmt.Errorf("fail to find existing plugins: %s", err)
		}
		plugins[i] = names
	}
	registry.plugins = plugins

	for pf := range plugins[plugin2.FUNCTION] {
		l := make([]string, 0)
		if ok, err := func_db.Get(pf, &l); ok {
			_ = registry.storeSymbols(pf, l)
		} else if err != nil {
			return nil, fmt.Errorf("error when querying kv: %s", err)
		} else {
			_ = registry.storeSymbols(pf, []string{pf})
		}
	}
	if manager.hasInstallFlag() {
		manager.pluginInstallWhenReboot()
		manager.clearInstallFlag()
	}
	return registry, nil
}

func findAll(t plugin2.PluginType, pluginDir string) (result map[string]string, err error) {
	result = make(map[string]string)
	dir := path.Join(pluginDir, plugin2.PluginTypes[t])
	files, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	for _, file := range files {
		baseName := filepath.Base(file.Name())
		if strings.HasSuffix(baseName, ".so") {
			n, v := parseName(baseName)
			// load the plugins when ekuiper set up
			if !conf.IsTesting {
				if _, err := manager.loadRuntime(t, n, path.Join(dir, baseName), ""); err != nil {
					continue
				}
			}
			result[n] = v
		}
	}
	return
}

func (rr *Manager) get(t plugin2.PluginType, name string) (string, bool) {
	rr.RLock()
	result := rr.plugins[t]
	rr.RUnlock()
	r, ok := result[name]
	return r, ok
}

func (rr *Manager) store(t plugin2.PluginType, name string, version string) {
	rr.Lock()
	rr.plugins[t][name] = version
	rr.Unlock()
}

func (rr *Manager) storeSymbols(name string, symbols []string) error {
	rr.Lock()
	defer rr.Unlock()
	for _, s := range symbols {
		if _, ok := rr.symbols[s]; ok {
			return fmt.Errorf("function name %s already exists", s)
		} else {
			rr.symbols[s] = name
		}
	}

	return nil
}

func (rr *Manager) removeSymbols(symbols []string) {
	rr.Lock()
	for _, s := range symbols {
		delete(rr.symbols, s)
	}
	rr.Unlock()
}

// API for management

func (rr *Manager) List(t plugin2.PluginType) []string {
	rr.RLock()
	result := rr.plugins[t]
	rr.RUnlock()
	keys := make([]string, 0, len(result))
	for k := range result {
		keys = append(keys, k)
	}
	return keys
}

func (rr *Manager) ListSymbols() []string {
	rr.RLock()
	result := rr.symbols
	rr.RUnlock()
	keys := make([]string, 0, len(result))
	for k := range result {
		keys = append(keys, k)
	}
	return keys
}

func (rr *Manager) GetPluginVersionBySymbol(t plugin2.PluginType, symbolName string) (string, bool) {
	switch t {
	case plugin2.FUNCTION:
		rr.RLock()
		result := rr.plugins[t]
		name, ok := rr.symbols[symbolName]
		rr.RUnlock()
		if ok {
			r, nok := result[name]
			return r, nok
		} else {
			return "", false
		}
	default:
		return rr.get(t, symbolName)
	}
}

func (rr *Manager) GetPluginBySymbol(t plugin2.PluginType, symbolName string) (string, bool) {
	switch t {
	case plugin2.FUNCTION:
		rr.RLock()
		defer rr.RUnlock()
		name, ok := rr.symbols[symbolName]
		return name, ok
	default:
		return symbolName, true
	}
}

func (rr *Manager) storePluginInstallScript(name string, t plugin2.PluginType, j plugin2.Plugin) {
	key := plugin2.PluginTypes[t] + "_" + name
	val := string(j.GetInstallScripts())
	_ = rr.plgInstallDb.Set(key, val)
}

func (rr *Manager) removePluginInstallScript(name string, t plugin2.PluginType) {
	key := plugin2.PluginTypes[t] + "_" + name
	_ = rr.plgInstallDb.Delete(key)
}

func (rr *Manager) Register(t plugin2.PluginType, j plugin2.Plugin) error {
	name, uri, shellParas := j.GetName(), j.GetFile(), j.GetShellParas()
	// Validation
	name = strings.Trim(name, " ")
	if name == "" {
		return fmt.Errorf("invalid name %s: should not be empty", name)
	}
	if !httpx.IsValidUrl(uri) || !strings.HasSuffix(uri, ".zip") {
		return fmt.Errorf("invalid uri %s", uri)
	}

	if v, ok := rr.get(t, name); ok {
		if v == DELETED {
			conf.Log.Debugf("update the plugin %s is marked as deleted but eKuiper is not restarted for the change to take effect yet", name)
		} else {
			return fmt.Errorf("invalid name %s: duplicate", name)
		}
	}

	var err error
	zipPath := path.Join(rr.pluginDir, name+".zip")

	// clean up: delete zip file and unzip files in error
	defer func(name string) { _ = os.Remove(name) }(zipPath)
	// download
	err = httpx.DownloadFile(zipPath, uri)
	if err != nil {
		return fmt.Errorf("fail to download file %s: %s", uri, err)
	}

	if t == plugin2.FUNCTION {
		if len(j.GetSymbols()) > 0 {
			err = rr.funcSymbolsDb.Set(name, j.GetSymbols())
			if err != nil {
				return err
			}
			err = rr.storeSymbols(name, j.GetSymbols())
		} else {
			err = rr.storeSymbols(name, []string{name})
		}
	}
	if err != nil {
		return err
	}

	// unzip and copy to destination
	version, err := rr.install(t, name, zipPath, shellParas)
	if err == nil && len(j.GetSymbols()) > 0 {
		err = rr.funcSymbolsDb.Set(name, j.GetSymbols())
	}
	if err != nil { // Revert for any errors
		if len(j.GetSymbols()) > 0 {
			rr.removeSymbols(j.GetSymbols())
		} else {
			rr.removeSymbols([]string{name})
		}
		return fmt.Errorf("fail to install plugin: %s", err)
	}
	rr.store(t, name, version)
	rr.storePluginInstallScript(name, t, j)

	switch t {
	case plugin2.SINK:
		if err := meta.ReadSinkMetaFile(path.Join(rr.pluginDataDir, plugin2.PluginTypes[t], name+`.json`), true); nil != err {
			conf.Log.Errorf("readSinkFile:%v", err)
			if err := meta.ReadSinkMetaFile(path.Join(rr.pluginConfDir, plugin2.PluginTypes[t], name+`.json`), true); nil != err {
				conf.Log.Errorf("readSinkFile:%v", err)
			}
		}
	case plugin2.SOURCE:
		isScan := true
		isLookup := true
		_, err := rr.Source(name)
		if err != nil {
			isScan = false
		}
		_, err = rr.LookupSource(name)
		if err != nil {
			isLookup = false
		}
		if err := meta.ReadSourceMetaFile(path.Join(rr.pluginDataDir, plugin2.PluginTypes[t], name+`.json`), isScan, isLookup); nil != err {
			conf.Log.Errorf("readSourceFile:%v", err)
			if err := meta.ReadSourceMetaFile(path.Join(rr.pluginConfDir, plugin2.PluginTypes[t], name+`.json`), isScan, isLookup); nil != err {
				conf.Log.Errorf("readSourceFile:%v", err)
			}
		}
	}
	return nil
}

// RegisterFuncs prerequisite：function plugin of name exists
func (rr *Manager) RegisterFuncs(name string, functions []string) error {
	if len(functions) == 0 {
		return fmt.Errorf("property 'functions' must not be empty")
	}
	old := make([]string, 0)
	if ok, err := rr.funcSymbolsDb.Get(name, &old); err != nil {
		return err
	} else if ok {
		rr.removeSymbols(old)
	} else if !ok {
		rr.removeSymbols([]string{name})
	}
	err := rr.funcSymbolsDb.Set(name, functions)
	if err != nil {
		return err
	}
	return rr.storeSymbols(name, functions)
}

func (rr *Manager) Delete(t plugin2.PluginType, name string, stop bool) error {
	name = strings.Trim(name, " ")
	if name == "" {
		return fmt.Errorf("invalid name %s: should not be empty", name)
	}
	if v, ok := rr.get(t, name); ok && v == DELETED {
		conf.Log.Debugf("plugin %s is already deleted", name)
		return nil
	}
	soPath, err := rr.getSoFilePath(t, name, true)
	if err != nil {
		return err
	}
	var results []string
	paths := []string{
		soPath,
	}
	// Find etc folder
	etcPath := path.Join(rr.pluginConfDir, plugin2.PluginTypes[t], name)
	if fi, err := os.Stat(etcPath); err == nil {
		if fi.Mode().IsDir() {
			paths = append(paths, etcPath)
		}
	}
	// Find etc folder
	dataPath := path.Join(rr.pluginConfDir, plugin2.PluginTypes[t], name)
	if fi, err := os.Stat(etcPath); err == nil {
		if fi.Mode().IsDir() {
			paths = append(paths, dataPath)
		}
	}
	switch t {
	case plugin2.SOURCE:
		yamlPaths := path.Join(rr.pluginConfDir, plugin2.PluginTypes[plugin2.SOURCE], name+".yaml")
		_ = os.Remove(yamlPaths)
		srcJsonPath := path.Join(rr.pluginConfDir, plugin2.PluginTypes[plugin2.SOURCE], name+".json")
		_ = os.Remove(srcJsonPath)
		yamlPaths = path.Join(rr.pluginDataDir, plugin2.PluginTypes[plugin2.SOURCE], name+".yaml")
		_ = os.Remove(yamlPaths)
		srcJsonPath = path.Join(rr.pluginDataDir, plugin2.PluginTypes[plugin2.SOURCE], name+".json")
		_ = os.Remove(srcJsonPath)
		meta.UninstallSource(name)
	case plugin2.SINK:
		yamlPaths := path.Join(rr.pluginConfDir, plugin2.PluginTypes[plugin2.SINK], name+".yaml")
		_ = os.Remove(yamlPaths)
		sinkJsonPaths := path.Join(rr.pluginConfDir, plugin2.PluginTypes[plugin2.SINK], name+".json")
		_ = os.Remove(sinkJsonPaths)
		yamlPaths = path.Join(rr.pluginDataDir, plugin2.PluginTypes[plugin2.SINK], name+".yaml")
		_ = os.Remove(yamlPaths)
		sinkJsonPaths = path.Join(rr.pluginDataDir, plugin2.PluginTypes[plugin2.SINK], name+".json")
		_ = os.Remove(sinkJsonPaths)
		meta.UninstallSink(name)
	case plugin2.FUNCTION:
		funcJsonPath := path.Join(rr.pluginConfDir, plugin2.PluginTypes[plugin2.FUNCTION], name+".json")
		_ = os.Remove(funcJsonPath)
		funcJsonPath = path.Join(rr.pluginDataDir, plugin2.PluginTypes[plugin2.FUNCTION], name+".json")
		_ = os.Remove(funcJsonPath)
		old := make([]string, 0)
		if ok, err := rr.funcSymbolsDb.Get(name, &old); err != nil {
			return err
		} else if ok {
			rr.removeSymbols(old)
			err := rr.funcSymbolsDb.Delete(name)
			if err != nil {
				return err
			}
		} else if !ok {
			rr.removeSymbols([]string{name})
		}
	}

	for _, p := range paths {
		_, err := os.Stat(p)
		if err == nil {
			err = os.RemoveAll(p)
			if err != nil {
				results = append(results, err.Error())
			}
		} else if !os.IsNotExist(err) {
			results = append(results, fmt.Sprintf("can't find %s", p))
		}
	}
	rr.removePluginInstallScript(name, t)

	if len(results) > 0 {
		return errors.New(strings.Join(results, "\n"))
	} else {
		rr.store(t, name, DELETED)
		if stop {
			go func() {
				time.Sleep(1 * time.Second)
				os.Exit(100)
			}()
		}
		return nil
	}
}

func (rr *Manager) GetPluginInfo(t plugin2.PluginType, name string) (map[string]interface{}, bool) {
	v, ok := rr.get(t, name)
	if strings.HasPrefix(v, "v") {
		v = v[1:]
	}
	if ok {
		r := map[string]interface{}{
			"name":    name,
			"version": v,
		}
		if t == plugin2.FUNCTION {
			l := make([]string, 0)
			if ok, _ := rr.funcSymbolsDb.Get(name, &l); ok {
				r["functions"] = l
			}
			// ignore the error
		}
		return r, ok
	}
	return nil, false
}

func (rr *Manager) install(t plugin2.PluginType, name, src string, shellParas []string) (string, error) {
	var filenames []string
	tempPath := path.Join(rr.pluginDir, "temp", plugin2.PluginTypes[t], name)
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempPath)
	r, err := zip.OpenReader(src)
	if err != nil {
		return "", err
	}
	defer func(r *zip.ReadCloser) {
		_ = r.Close()
	}(r)

	haveInstallFile := false
	for _, file := range r.File {
		fileName := file.Name
		if fileName == "install.sh" {
			haveInstallFile = true
		}
	}
	if len(shellParas) != 0 && !haveInstallFile {
		return "", fmt.Errorf("have shell parameters : %s but no install.sh file", shellParas)
	}

	soPrefix := regexp.MustCompile(fmt.Sprintf(`^((%s)|(%s))(@.*)?\.so$`, name, ucFirst(name)))
	var soPath string
	var yamlFile, yamlPath, version, soName string
	expFiles := 1
	if t == plugin2.SOURCE {
		yamlFile = name + ".yaml"
		yamlPath = path.Join(rr.pluginConfDir, plugin2.PluginTypes[t], yamlFile)
		expFiles = 3
	}
	var revokeFiles []string
	defer func() {
		if err != nil {
			for _, f := range revokeFiles {
				_ = os.RemoveAll(f)
			}
		}
	}()
	yamlFileChecked := false
	soFileChecked := false
	zipFiles := make([]string, 0)
	for _, file := range r.File {
		zipFiles = append(zipFiles, file.Name)
		fileName := file.Name
		switch {
		case yamlFile == fileName:
			yamlFileChecked = true
			// skip yaml file if exists
			if _, err := os.Stat(yamlPath); err != nil && os.IsNotExist(err) {
				conf.Log.Infof("install %s due to no this file", yamlPath)
				err = filex.UnzipTo(file, filepath.Join(rr.pluginConfDir, plugin2.PluginTypes[t]), yamlFile)
				if err != nil {
					return version, err
				}
				yamlDataPath := path.Join(rr.pluginDataDir, plugin2.PluginTypes[t], yamlFile)
				err = filex.UnzipTo(file, filepath.Join(rr.pluginDataDir, plugin2.PluginTypes[t]), yamlFile)
				if err != nil {
					return version, err
				}
				revokeFiles = append(revokeFiles, yamlPath)
				filenames = append(filenames, yamlPath)
				revokeFiles = append(revokeFiles, yamlDataPath)
				filenames = append(filenames, yamlDataPath)
			} else {
				filenames = append(filenames, yamlPath)
				conf.Log.Infof("skip install %s due to already exists", yamlPath)
				continue
			}
		case fileName == name+".json":
			jsonPath := path.Join(rr.pluginConfDir, plugin2.PluginTypes[t], fileName)
			if err := filex.UnzipTo(file, filepath.Join(rr.pluginConfDir, plugin2.PluginTypes[t]), fileName); nil != err {
				conf.Log.Errorf("Failed to decompress the metadata %s file", fileName)
			} else {
				revokeFiles = append(revokeFiles, jsonPath)
			}
			jsonPath = path.Join(rr.pluginDataDir, plugin2.PluginTypes[t], fileName)
			if err := filex.UnzipTo(file, filepath.Join(rr.pluginDataDir, plugin2.PluginTypes[t]), fileName); nil != err {
				conf.Log.Errorf("Failed to decompress the metadata %s file", fileName)
			} else {
				revokeFiles = append(revokeFiles, jsonPath)
			}
		case soPrefix.Match([]byte(fileName)):
			soPath = path.Join(rr.pluginDir, plugin2.PluginTypes[t], fileName)
			err = filex.UnzipTo(file, filepath.Join(rr.pluginDir, plugin2.PluginTypes[t]), fileName)
			if err != nil {
				return version, err
			}
			filenames = append(filenames, soPath)
			revokeFiles = append(revokeFiles, soPath)
			soName, version = parseName(fileName)
			soFileChecked = true
		case strings.HasPrefix(fileName, "etc/"):
			folder := path.Join(rr.pluginConfDir, plugin2.PluginTypes[t])
			fname := strings.Replace(fileName, "etc", name, 1)

			err = filex.UnzipTo(file, folder, fname)
			if err != nil {
				return version, err
			}
			folder = path.Join(rr.pluginDataDir, plugin2.PluginTypes[t])
			fname = strings.Replace(fileName, "etc", name, 1)
			err = filex.UnzipTo(file, folder, fname)
			if err != nil {
				return version, err
			}
		default:
			err = filex.UnzipTo(file, tempPath, fileName)
			if err != nil {
				return version, err
			}
		}
	}
	if len(filenames) != expFiles {
		err = fmt.Errorf("invalid zip file: expectFiles: %v, got filenames:%v, zipFiles: %v, yamlFileChecked:%v, soFileChecked:%v", expFiles, filenames, zipFiles, yamlFileChecked, soFileChecked)
		return version, err
	} else if haveInstallFile {
		// run install script if there is
		shell := make([]string, len(shellParas))
		copy(shell, shellParas)
		spath := path.Join(tempPath, "install.sh")
		shell = append(shell, spath)
		if 1 != len(shell) {
			copy(shell[1:], shell[0:])
			shell[0] = spath
		}
		conf.Log.Infof("run install script %s", strings.Join(shell, " "))
		cmd := exec.Command("/bin/sh", shell...)
		var outb, errb bytes.Buffer
		cmd.Stdout = &outb
		cmd.Stderr = &errb
		cmd.Dir = tempPath
		err := cmd.Run()
		if err != nil {
			conf.Log.Infof(`err:%v stdout:%s stderr:%s`, err, outb.String(), errb.String())
			return version, err
		}
		conf.Log.Infof(`run install script:%s`, outb.String())
	}

	if !conf.IsTesting {
		// load the runtime first
		_, err = manager.loadRuntime(t, soName, soPath, "")
		if err != nil {
			return version, err
		}
	}

	conf.Log.Infof("install %s plugin %s", plugin2.PluginTypes[t], name)
	return version, nil
}

// binder factory implementations

func (rr *Manager) Source(name string) (api.Source, error) {
	nf, err := rr.loadRuntime(plugin2.SOURCE, name, "", "")
	if err != nil {
		return nil, err
	}
	if nf == nil {
		return nil, nil
	}
	switch t := nf.(type) {
	case api.Source:
		return t, nil
	case func() api.Source:
		return t(), nil
	default:
		return nil, fmt.Errorf("exported symbol %s is not type of api.Source or function that return api.Source", t)
	}
}

func (rr *Manager) SourcePluginInfo(name string) (plugin2.EXTENSION_TYPE, string, string) {
	_, ok := rr.GetPluginVersionBySymbol(plugin2.SOURCE, name)
	if ok {
		pluginName, _ := rr.GetPluginBySymbol(plugin2.SOURCE, name)
		installScript := ""
		pluginKey := plugin2.PluginTypes[plugin2.SOURCE] + "_" + pluginName
		_, _ = rr.plgInstallDb.Get(pluginKey, &installScript)
		return plugin2.NATIVE_EXTENSION, pluginKey, installScript
	} else {
		return plugin2.NONE_EXTENSION, "", ""
	}
}

func (rr *Manager) LookupSource(name string) (api.Source, error) {
	nf, err := rr.loadRuntime(plugin2.SOURCE, name, "", ucFirst(name)+"Lookup")
	if err != nil {
		return nil, err
	}
	if nf == nil {
		return nil, nil
	}
	switch t := nf.(type) {
	case api.Source:
		return t, nil
	case func() api.Source:
		return t(), nil
	default:
		return nil, fmt.Errorf("exported symbol %s is not type of api.LookupSource or function that return api.LookupSource", t)
	}
}

func (rr *Manager) Sink(name string) (api.Sink, error) {
	nf, err := rr.loadRuntime(plugin2.SINK, name, "", "")
	if err != nil {
		return nil, err
	}
	if nf == nil {
		return nil, nil
	}
	var s api.Sink
	switch t := nf.(type) {
	case api.Sink:
		s = t
	case func() api.Sink:
		s = t()
	default:
		return nil, fmt.Errorf("exported symbol %s is not type of api.Sink or function that return api.Sink", t)
	}
	return s, nil
}

func (rr *Manager) SinkPluginInfo(name string) (plugin2.EXTENSION_TYPE, string, string) {
	_, ok := rr.GetPluginVersionBySymbol(plugin2.SINK, name)
	if ok {
		pluginName, _ := rr.GetPluginBySymbol(plugin2.SINK, name)
		installScript := ""
		pluginKey := plugin2.PluginTypes[plugin2.SINK] + "_" + pluginName
		_, _ = rr.plgInstallDb.Get(pluginKey, &installScript)
		return plugin2.NATIVE_EXTENSION, pluginKey, installScript
	} else {
		return plugin2.NONE_EXTENSION, "", ""
	}
}

func (rr *Manager) Function(name string) (api.Function, error) {
	nf, err := rr.loadRuntime(plugin2.FUNCTION, name, "", "")
	if err != nil {
		return nil, err
	}
	if nf == nil {
		return nil, nil
	}
	var s api.Function
	switch t := nf.(type) {
	case api.Function:
		s = t
	case func() api.Function:
		s = t()
	default:
		return nil, fmt.Errorf("exported symbol %s is not type of api.Function or function that return api.Function", t)
	}
	return s, nil
}

func (rr *Manager) HasFunctionSet(name string) bool {
	_, ok := rr.get(plugin2.FUNCTION, name)
	return ok
}

func (rr *Manager) FunctionPluginInfo(funcName string) (plugin2.EXTENSION_TYPE, string, string) {
	pluginName, ok := rr.GetPluginBySymbol(plugin2.FUNCTION, funcName)
	if ok {
		installScript := ""
		pluginKey := plugin2.PluginTypes[plugin2.FUNCTION] + "_" + pluginName
		_, _ = rr.plgInstallDb.Get(pluginKey, &installScript)
		return plugin2.NATIVE_EXTENSION, pluginKey, installScript
	} else {
		return plugin2.NONE_EXTENSION, "", ""
	}
}

func (rr *Manager) ConvName(name string) (string, bool) {
	_, ok := rr.GetPluginBySymbol(plugin2.FUNCTION, name)
	if ok {
		return name, true
	}
	return name, false
}

// If not found, return nil,nil; Other errors return nil, err
func (rr *Manager) loadRuntime(t plugin2.PluginType, soName, soFilepath, symbolName string) (plugin.Symbol, error) {
	ptype := plugin2.PluginTypes[t]
	key := ptype + "/" + soName
	var (
		plug *plugin.Plugin
		ok   bool
		err  error
	)
	rr.RLock()
	plug, ok = rr.runtime[key]
	rr.RUnlock()
	if !ok {
		var soPath string
		if soFilepath != "" {
			soPath = soFilepath
		} else {
			mod, err := rr.getSoFilePath(t, soName, false)
			if err != nil {
				conf.Log.Debugf("cannot find the native plugin %s in path: %v", soName, err)
				return nil, nil
			}
			soPath = mod
		}
		conf.Log.Debugf("Opening plugin %s", soPath)
		plug, err = plugin.Open(soPath)
		if err != nil {
			conf.Log.Errorf("plugin %s open error: %v", soName, err)
			return nil, fmt.Errorf("cannot open %s: %v", soPath, err)
		}
		rr.Lock()
		rr.runtime[key] = plug
		rr.Unlock()
		conf.Log.Debugf("Successfully open plugin %s", soPath)
	}
	if symbolName == "" {
		symbolName = ucFirst(soName)
	}
	conf.Log.Debugf("Loading symbol %s", symbolName)
	nf, err := plug.Lookup(symbolName)
	if err != nil {
		conf.Log.Warnf("cannot find symbol %s, please check if it is exported: %v", symbolName, err)
		return nil, nil
	}
	conf.Log.Debugf("Successfully look-up plugin %s", symbolName)
	return nf, nil
}

// Return the lowercase version of so name. It may be upper case in path.
func (rr *Manager) getSoFilePath(t plugin2.PluginType, name string, isSoName bool) (string, error) {
	var (
		v      string
		soname string
		ok     bool
	)
	// We must identify plugin or symbol when deleting function plugin
	if isSoName {
		soname = name
	} else {
		soname, ok = rr.GetPluginBySymbol(t, name)
		if !ok {
			return "", errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("invalid symbol name %s: not exist", name))
		}
	}
	v, ok = rr.get(t, soname)
	if !ok {
		return "", errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("invalid name %s: not exist", soname))
	}

	soFile := soname + ".so"
	if v != "" {
		soFile = fmt.Sprintf("%s@%s.so", soname, v)
	}
	p := path.Join(rr.pluginDir, plugin2.PluginTypes[t], soFile)
	if _, err := os.Stat(p); err != nil {
		p = path.Join(rr.pluginDir, plugin2.PluginTypes[t], ucFirst(soFile))
	}
	if _, err := os.Stat(p); err != nil {
		return "", errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("cannot find .so file for plugin %s", soname))
	}
	return p, nil
}

func parseName(n string) (string, string) {
	result := strings.Split(n, ".so")
	result = strings.Split(result[0], "@")
	name := lcFirst(result[0])
	if len(result) > 1 {
		return name, result[1]
	}
	return name, ""
}

func ucFirst(str string) string {
	for i, v := range str {
		return string(unicode.ToUpper(v)) + str[i+1:]
	}
	return ""
}

func lcFirst(str string) string {
	for i, v := range str {
		return string(unicode.ToLower(v)) + str[i+1:]
	}
	return ""
}

func (rr *Manager) UninstallAllPlugins() {
	keys, err := rr.plgInstallDb.Keys()
	if err != nil {
		return
	}
	for _, v := range keys {
		plgType := plugin2.PluginTypeMap[strings.Split(v, "_")[0]]
		plgName := strings.Split(v, "_")[1]
		_ = rr.Delete(plgType, plgName, false)
	}
}

func (rr *Manager) GetAllPlugins() map[string]string {
	allPlgs, err := rr.plgInstallDb.All()
	if err != nil {
		return nil
	}
	delete(allPlgs, BOOT_INSTALL)
	return allPlgs
}

func (rr *Manager) GetAllPluginsStatus() map[string]string {
	allPlgs, err := rr.plgStatusDb.All()
	if err != nil {
		return nil
	}
	return allPlgs
}

const BOOT_INSTALL = "$boot_install"

// PluginImport save the plugin install information and wait for restart
func (rr *Manager) PluginImport(ctx context.Context, plugins map[string]string) map[string]string {
	errMap := map[string]string{}
	if len(plugins) == 0 {
		return nil
	}
	for k, v := range plugins {
		select {
		case <-ctx.Done():
			return errMap
		default:
		}
		err := rr.plgInstallDb.Set(k, v)
		if err != nil {
			errMap[k] = err.Error()
		}
	}
	// set the flag to install the plugins when eKuiper reboot
	err := rr.plgInstallDb.Set(BOOT_INSTALL, BOOT_INSTALL)
	if err != nil {
		errMap["flag"] = err.Error()
	}
	return errMap
}

// PluginPartialImport compare the plugin to be installed and the one in database
// if not exist in database, install;
// if exist, ignore
func (rr *Manager) PluginPartialImport(ctx context.Context, plugins map[string]string) map[string]string {
	errMap := map[string]string{}
	for k, v := range plugins {
		select {
		case <-ctx.Done():
			return errMap
		default:
		}
		plugInScript := ""
		found, _ := rr.plgInstallDb.Get(k, &plugInScript)
		if !found {
			err := rr.pluginRegisterForImport(k, v)
			if err != nil {
				errMap[k] = err.Error()
			}
		}
	}
	return errMap
}

func (rr *Manager) hasInstallFlag() bool {
	val := ""
	found, _ := rr.plgInstallDb.Get(BOOT_INSTALL, &val)
	return found
}

func (rr *Manager) clearInstallFlag() {
	_ = rr.plgInstallDb.Delete(BOOT_INSTALL)
}

func (rr *Manager) pluginRegisterForImport(key, script string) error {
	plgType := plugin2.PluginTypeMap[strings.Split(key, "_")[0]]
	sd := plugin2.NewPluginByType(plgType)
	err := json.Unmarshal(cast.StringToBytes(script), &sd)
	if err != nil {
		return err
	}
	err = rr.Register(plgType, sd)
	if err != nil {
		conf.Log.Errorf(`install native plugin %s error: %v`, key, err)
		return err
	}
	return nil
}

func (rr *Manager) pluginInstallWhenReboot() {
	allPlgs, err := rr.plgInstallDb.All()
	if err != nil {
		return
	}

	delete(allPlgs, BOOT_INSTALL)
	_ = rr.plgStatusDb.Clean()

	for k, v := range allPlgs {
		err := rr.pluginRegisterForImport(k, v)
		_ = rr.plgStatusDb.Set(k, err.Error())
	}
}
