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

package meta

import (
	"fmt"
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
		Node     *fileNode               `json:"node"`
		Outputs  []interface{}           `json:"outputs"`
	}
	uiSource struct {
		About    *about             `json:"about"`
		Libs     []string           `json:"libs"`
		ConfKeys map[string][]field `json:"properties"`
		Node     *node              `json:"node"`
		Outputs  []interface{}      `json:"outputs"`
	}
)

func newUiSource(fi *fileSource) (*uiSource, error) {
	if nil == fi {
		return nil, nil
	}
	var err error
	ui := new(uiSource)
	ui.Libs = fi.Libs
	ui.About = newAbout(fi.About)
	ui.Node = newNode(fi.Node)
	ui.Outputs = make([]interface{}, len(fi.Outputs))
	for k, field := range fi.Outputs {
		ui.Outputs[k] = field
	}
	ui.ConfKeys = make(map[string][]field)
	for k, fields := range fi.ConfKeys {
		if ui.ConfKeys[k], err = newField(fields); nil != err {
			return nil, err
		}
	}
	return ui, nil
}

var gSourcemetaLock = sync.RWMutex{}
var gSourcemetadata = make(map[string]*uiSource)

func UninstallSource(name string) {
	gSourcemetaLock.RLock()
	defer gSourcemetaLock.RUnlock()

	if v, ok := gSourcemetadata[name+".json"]; ok {
		if nil != v.About {
			v.About.Installed = false
		}
	}
}

func ReadSourceMetaFile(filePath string, installed bool) error {
	fileName := path.Base(filePath)
	if "mqtt_source.json" == fileName {
		fileName = "mqtt.json"
	}
	ptrMeta := new(fileSource)
	_ = filex.ReadJsonUnmarshal(filePath, ptrMeta)
	if nil == ptrMeta.About {
		return fmt.Errorf("not found about of %s", filePath)
	} else {
		ptrMeta.About.Installed = installed
	}

	meta, err := newUiSource(ptrMeta)
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

func ReadSourceMetaDir(checker InstallChecker) error {
	confDir, err := conf.GetConfLoc()
	if nil != err {
		return err
	}

	dir := path.Join(confDir, "sources")
	dirEntries, err := os.ReadDir(dir)
	if nil != err {
		return err
	}

	if err = ReadSourceMetaFile(path.Join(confDir, "mqtt_source.json"), true); nil != err {
		return err
	}
	conf.Log.Infof("Loading metadata file for source : %s", "mqtt_source.json")

	for _, entry := range dirEntries {
		fileName := entry.Name()
		if strings.HasSuffix(fileName, ".json") {
			filePath := path.Join(dir, fileName)
			if err = ReadSourceMetaFile(filePath, checker(strings.TrimSuffix(fileName, ".json"))); nil != err {
				return err
			}
			conf.Log.Infof("Loading metadata file for source : %s", fileName)
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

func GetSourcesPlugins() (sources []*pluginfo) {
	gSourcemetaLock.RLock()
	defer gSourcemetaLock.RUnlock()

	for fileName, v := range gSourcemetadata {
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
