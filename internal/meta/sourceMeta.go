// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/pkg/ast"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/filex"
)

type (
	fileSource struct {
		About    *fileAbout              `json:"about"`
		Libs     []string                `json:"libs"`
		ConfKeys map[string][]*fileField `json:"properties"`
		Node     interface{}             `json:"node"`
	}
	uiSource struct {
		About    *about             `json:"about"`
		Libs     []string           `json:"libs"`
		ConfKeys map[string][]field `json:"properties"`
		Node     interface{}        `json:"node"`
		isScan   bool
		isLookup bool
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

var gSourcemetaLock = sync.RWMutex{}
var gSourcemetadata = make(map[string]*uiSource)

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
	fileName := path.Base(filePath)
	if "mqtt_source.json" == fileName {
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

	return err
}

func ReadSourceMetaDir(scanChecker InstallChecker, lookupChecker InstallChecker) error {
	//load etc/sources meta data
	confDir, err := conf.GetConfLoc()
	if nil != err {
		return err
	}

	dir := path.Join(confDir, "sources")
	dirEntries, err := os.ReadDir(dir)
	if nil != err {
		return err
	}

	if err = ReadSourceMetaFile(path.Join(confDir, "mqtt_source.json"), true, false); nil != err {
		return err
	}
	conf.Log.Infof("Loading metadata file for source : %s", "mqtt_source.json")

	for _, entry := range dirEntries {
		fileName := entry.Name()
		if strings.HasSuffix(fileName, ".json") {
			name := strings.TrimSuffix(fileName, ".json")
			isScan := scanChecker(name)
			isLookup := lookupChecker(name)
			if isScan || isLookup {
				filePath := path.Join(dir, fileName)
				if err = ReadSourceMetaFile(filePath, isScan, isLookup); nil != err {
					return err
				}
				conf.Log.Infof("Loading metadata file for source : %s", fileName)
			} else {
				conf.Log.Warnf("Find source metadata file but not installed : %s", fileName)
			}
		}
	}

	//load data/sources meta data
	confDir, err = conf.GetDataLoc()
	if nil != err {
		return err
	}

	dir = path.Join(confDir, "sources")
	dirEntries, err = os.ReadDir(dir)
	if nil != err {
		return err
	}

	for _, entry := range dirEntries {
		fileName := entry.Name()
		if strings.HasSuffix(fileName, ".json") {
			name := strings.TrimSuffix(fileName, ".json")
			isScan := scanChecker(name)
			isLookup := lookupChecker(name)
			if isScan || isLookup {
				filePath := path.Join(dir, fileName)
				if err = ReadSourceMetaFile(filePath, isScan, isLookup); nil != err {
					return err
				}
				conf.Log.Infof("Loading metadata file for source : %s", fileName)
			} else {
				conf.Log.Warnf("Find source metadata file but not installed : %s", fileName)
			}
		}
	}

	return nil
}

func GetSourceMeta(sourceName, language string) (ptrSourceProperty *uiSource, err error) {

	gSourcemetaLock.RLock()
	defer gSourcemetaLock.RUnlock()

	v, found := gSourcemetadata[sourceName+`.json`]
	if !found {
		return nil, fmt.Errorf(`%s%s`, getMsg(language, source, "not_found_plugin"), sourceName)
	}

	ui := new(uiSource)
	*ui = *v
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
		node := new(pluginfo)
		node.Name = strings.TrimSuffix(fileName, `.json`)
		if nil == v {
			continue
		}
		if nil == v.About {
			continue
		}
		node.About = v.About
		i := 0
		for ; i < len(sources); i++ {
			if node.Name <= sources[i].Name {
				sources = append(sources, node)
				copy(sources[i+1:], sources[i:])
				sources[i] = node
				break
			}
		}
		if len(sources) == i {
			sources = append(sources, node)
		}
	}
	return sources
}
