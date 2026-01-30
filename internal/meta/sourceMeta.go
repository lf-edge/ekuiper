// Copyright 2021-2025 EMQ Technologies Co., Ltd.
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

package meta

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/lf-edge/ekuiper/v2/internal/binder/io"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/filex"
	"github.com/lf-edge/ekuiper/v2/internal/plugin"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/syncx"
)

type (
	fileSource struct {
		About      *fileAbout              `json:"about"`
		Libs       []string                `json:"libs"`
		DataSource interface{}             `json:"dataSource,omitempty"`
		ConfKeys   map[string][]*fileField `json:"properties"`
		Node       interface{}             `json:"node"`
	}
	uiSource struct {
		About      *about             `json:"about"`
		Libs       []string           `json:"libs"`
		DataSource interface{}        `json:"dataSource,omitempty"`
		ConfKeys   map[string][]field `json:"properties"`
		Node       interface{}        `json:"node"`
		Type       string             `json:"type,omitempty"`
		isScan     bool
		isLookup   bool
	}
)

func newUiSource(fi *fileSource, isScan bool, isLookup bool) (*uiSource, error) {
	if nil == fi {
		return nil, nil
	}
	var err error
	ui := new(uiSource)
	ui.Libs = fi.Libs
	ui.About = newAbout(fi.About)
	ui.Node = fi.Node
	if fi.DataSource != nil {
		ui.DataSource = fi.DataSource
	}
	ui.ConfKeys = make(map[string][]field)
	for k, fields := range fi.ConfKeys {
		if ui.ConfKeys[k], err = newField(fields); nil != err {
			return nil, err
		}
	}
	ui.isScan = isScan
	ui.isLookup = isLookup
	return ui, nil
}

var (
	gSourcemetaLock = syncx.RWMutex{}
	gSourcemetadata = make(map[string]*uiSource)
)

func UninstallSource(name string) {
	gSourcemetaLock.Lock()
	defer gSourcemetaLock.Unlock()

	if v, ok := gSourcemetadata[name+".json"]; ok {
		if nil != v.About {
			v.About.Installed = false
		}
		delete(gSourcemetadata, name+".json")
	}
	delYamlConf(fmt.Sprintf(SourceCfgOperatorKeyTemplate, name))
}

func ReadSourceMetaFile(filePath string, isScan bool, isLookup bool) error {
	fileName := filepath.Base(filePath)
	if fileName == "mqtt_source.json" {
		fileName = "mqtt.json"
	}
	ptrMeta := new(fileSource)
	_ = filex.ReadJsonUnmarshal(filePath, ptrMeta)
	if nil == ptrMeta.About {
		return fmt.Errorf("not found about of %s", filePath)
	} else {
		// TODO currently, only show installed source in ui
		ptrMeta.About.Installed = true
	}

	meta, err := newUiSource(ptrMeta, isScan, isLookup)
	if nil != err {
		return err
	}
	gSourcemetaLock.Lock()
	gSourcemetadata[fileName] = meta
	gSourcemetaLock.Unlock()
	loadConfigOperatorForSource(strings.TrimSuffix(fileName, `.json`))
	loadConfigOperatorForConnection(strings.TrimSuffix(fileName, `.json`))
	return nil
}

func ReadSourceMetaDir(scanChecker InstallChecker, lookupChecker InstallChecker) error {
	// load etc/sources meta data
	confDir, err := conf.GetConfLoc()
	if nil != err {
		return err
	}

	dir := filepath.Join(confDir, "sources")
	dirEntries, err := os.ReadDir(dir)
	if nil != err {
		return err
	}

	if err = ReadSourceMetaFile(filepath.Join(confDir, "mqtt_source.json"), true, false); nil != err {
		return err
	}

	for _, entry := range dirEntries {
		fileName := entry.Name()
		if strings.HasSuffix(fileName, ".json") {
			name := strings.TrimSuffix(fileName, ".json")
			isScan := scanChecker(name)
			isLookup := lookupChecker(name)
			if isScan || isLookup {
				filePath := filepath.Join(dir, fileName)
				if err = ReadSourceMetaFile(filePath, isScan, isLookup); nil != err {
					return err
				}
			} else {
				conf.Log.Warnf("Find source metadata file but not installed : %s", fileName)
			}
		}
	}
	return nil
}

func GetSourceMeta(sourceName, language string) (ptrSourceProperty *uiSource, err error) {
	defer func() {
		if err != nil {
			if _, ok := err.(errorx.ErrorWithCode); !ok {
				err = errorx.NewWithCode(errorx.ConfKeyError, err.Error())
			}
		}
	}()
	gSourcemetaLock.RLock()
	defer gSourcemetaLock.RUnlock()

	v, found := gSourcemetadata[sourceName+`.json`]
	if !found {
		return nil, fmt.Errorf(`%s%s`, getMsg(language, source, "not_found_plugin"), sourceName)
	}

	ui := &uiSource{}
	*ui = *v
	t, _, _ := io.GetSourcePlugin(sourceName)
	ui.Type = plugin.ExtensionTypes[t]
	return ui, nil
}

func GetSourcesPlugins(kind string) (sources []*pluginfo) {
	gSourcemetaLock.RLock()
	defer gSourcemetaLock.RUnlock()

	for fileName, v := range gSourcemetadata {
		if kind == ast.StreamKindLookup && !v.isLookup {
			continue
		} else if kind == ast.StreamKindScan && !v.isScan {
			continue
		}
		name := strings.TrimSuffix(fileName, `.json`)
		t, _, _ := io.GetSourcePlugin(name)
		n := &pluginfo{
			Name:  name,
			About: v.About,
			Type:  plugin.ExtensionTypes[t],
		}
		i := 0
		for ; i < len(sources); i++ {
			if n.Name <= sources[i].Name {
				sources = append(sources, n)
				copy(sources[i+1:], sources[i:])
				sources[i] = n
				break
			}
		}
		if len(sources) == i {
			sources = append(sources, n)
		}
	}
	return sources
}
