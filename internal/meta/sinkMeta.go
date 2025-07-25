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
	"path"
	"strings"

	"github.com/lf-edge/ekuiper/v2/internal/binder/io"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/filex"
	"github.com/lf-edge/ekuiper/v2/internal/plugin"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
)

const (
	sink   = `sink`
	source = `source`
)

type (
	author struct {
		Name    string `json:"name"`
		Email   string `json:"email"`
		Company string `json:"company"`
		Website string `json:"website"`
	}
	fileLanguage struct {
		English string `json:"en_US"`
		Chinese string `json:"zh_CN"`
	}
	fileField struct {
		Name              string        `json:"name"`
		Default           interface{}   `json:"default"`
		Control           string        `json:"control"`
		ConnectionRelated bool          `json:"connection_related"`
		Optional          bool          `json:"optional"`
		Type              string        `json:"type"`
		Hint              *fileLanguage `json:"hint"`
		Label             *fileLanguage `json:"label"`
		Values            interface{}   `json:"values"`
	}
	fileAbout struct {
		Trial       bool          `json:"trial"`
		Installed   bool          `json:"installed"`
		Author      *author       `json:"author"`
		HelpUrl     *fileLanguage `json:"helpUrl"`
		Description *fileLanguage `json:"description"`
	}
	//fileNode struct {
	//	Category string        `json:"category"`
	//	Icon     string        `json:"iconPath"`
	//	Label    *fileLanguage `json:"label"`
	//}
	fileSink struct {
		About  *fileAbout   `json:"about"`
		Libs   []string     `json:"libs"`
		Fields []*fileField `json:"properties"`
		Node   interface{}  `json:"node"`
	}
	language struct {
		English string `json:"en"`
		Chinese string `json:"zh"`
	}
	about struct {
		Trial       bool      `json:"trial"`
		Installed   bool      `json:"installed"`
		Author      *author   `json:"author"`
		HelpUrl     *language `json:"helpUrl"`
		Description *language `json:"description"`
	}
	field struct {
		Exist             bool        `json:"exist"`
		Name              string      `json:"name"`
		Default           interface{} `json:"default"`
		Type              string      `json:"type"`
		Control           string      `json:"control"`
		ConnectionRelated bool        `json:"connection_related"`
		Optional          bool        `json:"optional"`
		Values            interface{} `json:"values"`
		Hint              *language   `json:"hint"`
		Label             *language   `json:"label"`
	}
	node struct {
		Category string    `json:"category"`
		Icon     string    `json:"iconPath"`
		Label    *language `json:"label"`
	}
	uiSink struct {
		About  *about      `json:"about"`
		Libs   []string    `json:"libs"`
		Fields []field     `json:"properties"`
		Node   interface{} `json:"node"`
		Type   string      `json:"type,omitempty"`
	}
)

func newLanguage(fi *fileLanguage) *language {
	if nil == fi {
		return nil
	}
	ui := new(language)
	ui.English = fi.English
	ui.Chinese = fi.Chinese
	return ui
}

func newField(fis []*fileField) (uis []field, err error) {
	for _, fi := range fis {
		if nil == fi {
			continue
		}
		ui := field{
			Name:              fi.Name,
			Type:              fi.Type,
			Control:           fi.Control,
			ConnectionRelated: fi.ConnectionRelated,
			Optional:          fi.Optional,
			Values:            fi.Values,
			Hint:              newLanguage(fi.Hint),
			Label:             newLanguage(fi.Label),
		}
		switch t := fi.Default.(type) {
		case []interface{}:
			var auxFi []*fileField
			if err = cast.MapToStruct(t, &auxFi); nil != err {
				return nil, err
			}
			if ui.Default, err = newField(auxFi); nil != err {
				return nil, err
			}
		default:
			ui.Default = fi.Default
		}
		uis = append(uis, ui)
	}
	return uis, err
}

func newAbout(fi *fileAbout) *about {
	if nil == fi {
		return nil
	}
	ui := new(about)
	ui.Trial = fi.Trial
	ui.Installed = fi.Installed
	ui.Author = fi.Author
	ui.HelpUrl = newLanguage(fi.HelpUrl)
	ui.Description = newLanguage(fi.Description)
	return ui
}

func newUiSink(fi *fileSink) (*uiSink, error) {
	if nil == fi {
		return nil, nil
	}
	var err error
	ui := new(uiSink)
	ui.Libs = fi.Libs
	ui.Node = fi.Node
	ui.About = newAbout(fi.About)
	ui.Fields, err = newField(fi.Fields)
	return ui, err
}

var gSinkmetadata = make(map[string]*uiSink) // immutable

func ReadSinkMetaDir(checker InstallChecker) error {
	confDir, err := conf.GetConfLoc()
	if nil != err {
		return err
	}
	dataDir, err := conf.GetDataLoc()
	if err != nil {
		return err
	}
	if err := readSinkMetaDir(confDir, checker); err != nil {
		return err
	}
	return readSinkMetaDir(dataDir, checker)
}

func readSinkMetaDir(folder string, checker InstallChecker) error {
	dir := path.Join(folder, "sinks")
	files, err := os.ReadDir(dir)
	if nil != err {
		return err
	}
	for _, file := range files {
		fname := file.Name()
		if !strings.HasSuffix(fname, ".json") {
			continue
		}

		filePath := path.Join(dir, fname)
		if err := ReadSinkMetaFile(filePath, checker(strings.TrimSuffix(fname, ".json"))); nil != err {
			return err
		}
	}
	return nil
}

func UninstallSink(name string) {
	if ui, ok := gSinkmetadata[name+".json"]; ok {
		if nil != ui.About {
			ui.About.Installed = false
			delete(gSinkmetadata, name+".json")
		}
	}
	delYamlConf(fmt.Sprintf(SinkCfgOperatorKeyTemplate, name))
}

func ReadSinkMetaFile(filePath string, installed bool) error {
	finame := path.Base(filePath)
	metadata := new(fileSink)
	err := filex.ReadJsonUnmarshal(filePath, metadata)
	if nil != err {
		return fmt.Errorf("filePath:%s err:%v", filePath, err)
	}
	if nil == metadata.About {
		return fmt.Errorf("not found about of %s", finame)
	} else {
		metadata.About.Installed = installed
	}
	uisink, err := newUiSink(metadata)
	if err != nil {
		return err
	}
	gSinkmetadata[finame] = uisink
	loadConfigOperatorForSink(strings.TrimSuffix(finame, `.json`))
	return nil
}

func GetSinkMeta(pluginName, language string) (s *uiSink, err error) {
	defer func() {
		if err != nil {
			if _, ok := err.(errorx.ErrorWithCode); !ok {
				err = errorx.NewWithCode(errorx.ConfKeyError, err.Error())
			}
		}
	}()

	fileName := pluginName + `.json`
	sinkMetadata := gSinkmetadata
	data, ok := sinkMetadata[fileName]
	if !ok || data == nil {
		return nil, fmt.Errorf(`%s%s`, getMsg(language, sink, "not_found_plugin"), pluginName)
	}
	t, _, _ := io.GetSinkPlugin(pluginName)
	data.Type = plugin.ExtensionTypes[t]
	return data, nil
}

type pluginfo struct {
	Name  string `json:"name"`
	About *about `json:"about"`
	Type  string `json:"type,omitempty"`
}

func GetSinks() (sinks []*pluginfo) {
	sinkMeta := gSinkmetadata
	for fileName, v := range sinkMeta {
		name := strings.TrimSuffix(fileName, `.json`)
		t, _, _ := io.GetSinkPlugin(name)
		n := &pluginfo{
			Name:  name,
			About: v.About,
			Type:  plugin.ExtensionTypes[t],
		}
		i := 0
		for ; i < len(sinks); i++ {
			if n.Name <= sinks[i].Name {
				sinks = append(sinks, n)
				copy(sinks[i+1:], sinks[i:])
				sinks[i] = n
				break
			}
		}
		if len(sinks) == i {
			sinks = append(sinks, n)
		}
	}
	return sinks
}
