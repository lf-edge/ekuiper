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

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/filex"
)

type (
	fileFunc struct {
		Name       string        `json:"name"`
		Example    string        `json:"example"`
		Hint       *fileLanguage `json:"hint"`
		Aggregate  bool          `json:"aggregate"`
		ArgsFields []*fileField  `json:"args"`
		Node       *fileNode     `json:"node"`
		Outputs    []interface{} `json:"outputs"`
	}
	fileFuncs struct {
		About   *fileAbout  `json:"about"`
		Name    string      `json:"name"`
		Version string      `json:"version"`
		FiFuncs []*fileFunc `json:"functions"`
	}
	uiFunc struct {
		Name       string        `json:"name"`
		Example    string        `json:"example"`
		Hint       *language     `json:"hint"`
		Aggregate  bool          `json:"aggregate"`
		ArgsFields []*fileField  `json:"args"`
		Node       *node         `json:"node"`
		Outputs    []interface{} `json:"outputs"`
	}
	uiFuncs struct {
		About   *about    `json:"about"`
		Name    string    `json:"name"`
		Version string    `json:"version"`
		UiFuncs []*uiFunc `json:"functions"`
	}
)

func newUiFuncs(fi *fileFuncs) *uiFuncs {
	if nil == fi {
		return nil
	}
	uis := new(uiFuncs)
	uis.About = newAbout(fi.About)
	uis.Name = fi.Name
	for _, v := range fi.FiFuncs {
		ui := new(uiFunc)
		ui.Name = v.Name
		ui.Example = v.Example
		ui.Hint = newLanguage(v.Hint)
		ui.Aggregate = v.Aggregate
		ui.ArgsFields = v.ArgsFields
		ui.Node = newNode(v.Node)
		ui.Outputs = make([]interface{}, len(v.Outputs))
		for k, field := range v.Outputs {
			ui.Outputs[k] = field
		}
		uis.UiFuncs = append(uis.UiFuncs, ui)
	}
	return uis
}

var gFuncmetadata = make(map[string]*uiFuncs)

func ReadFuncMetaDir(checker InstallChecker) error {
	confDir, err := conf.GetConfLoc()
	if nil != err {
		return err
	}

	dir := path.Join(confDir, "functions")
	files, err := os.ReadDir(dir)
	if nil != err {
		return err
	}
	for _, file := range files {
		fname := file.Name()
		if !strings.HasSuffix(fname, ".json") {
			continue
		}

		if err := ReadFuncMetaFile(path.Join(dir, fname), checker(strings.TrimSuffix(fname, ".json"))); nil != err {
			return err
		}
	}
	return nil
}

func UninstallFunc(name string) {
	if ui, ok := gFuncmetadata[name+".json"]; ok {
		if nil != ui.About {
			ui.About.Installed = false
		}
	}
}

func ReadFuncMetaFile(filePath string, installed bool) error {
	fiName := path.Base(filePath)
	fis := new(fileFuncs)
	err := filex.ReadJsonUnmarshal(filePath, fis)
	if nil != err {
		return fmt.Errorf("filePath:%s err:%v", filePath, err)
	}
	if nil == fis.About {
		return fmt.Errorf("not found about of %s", filePath)
	} else {
		fis.About.Installed = installed
	}
	gFuncmetadata[fiName] = newUiFuncs(fis)
	conf.Log.Infof("funcMeta file : %s", fiName)
	return nil
}

func GetFunctions() (ret []*uiFuncs) {
	for _, v := range gFuncmetadata {
		ret = append(ret, v)
	}
	return ret
}
