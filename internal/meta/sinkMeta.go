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
	"github.com/lf-edge/ekuiper/internal"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/filex"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"io/ioutil"
	"path"
	"strings"
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
	fileSink struct {
		About  *fileAbout   `json:"about"`
		Libs   []string     `json:"libs"`
		Fields []*fileField `json:"properties"`
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

	uiSink struct {
		About  *about   `json:"about"`
		Libs   []string `json:"libs"`
		Fields []field  `json:"properties"`
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
	ui.About = newAbout(fi.About)
	ui.Fields, err = newField(fi.Fields)
	return ui, err
}

var gSinkmetadata = make(map[string]*uiSink) //immutable

func ReadSinkMetaDir(checker InstallChecker) error {
	confDir, err := conf.GetConfLoc()
	if nil != err {
		return err
	}

	dir := path.Join(confDir, "sinks")
	files, err := ioutil.ReadDir(dir)
	if nil != err {
		return err
	}
	for _, file := range files {
		fname := file.Name()
		if !strings.HasSuffix(fname, internal.JsonFileSuffix) {
			continue
		}

		filePath := path.Join(dir, fname)
		if err := ReadSinkMetaFile(filePath, checker(strings.TrimSuffix(fname, internal.JsonFileSuffix))); nil != err {
			return err
		}
	}
	return nil
}

func UninstallSink(name string) {
	if ui, ok := gSinkmetadata[name+internal.JsonFileSuffix]; ok {
		if nil != ui.About {
			ui.About.Installed = false
		}
	}
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
	gSinkmetadata[finame], err = newUiSink(metadata)
	if nil != err {
		return err
	}
	conf.Log.Infof("Loading metadata file for sink: %s", finame)
	return nil
}

func GetSinkMeta(pluginName, language string) (*uiSink, error) {
	fileName := pluginName + internal.JsonFileSuffix
	sinkMetadata := gSinkmetadata
	data, ok := sinkMetadata[fileName]
	if !ok || data == nil {
		return nil, fmt.Errorf(`%s%s`, getMsg(language, internal.Sink, "not_found_plugin"), pluginName)
	}
	return data, nil
}

type pluginfo struct {
	Name  string `json:"name"`
	About *about `json:"about"`
}

func GetSinks() (sinks []*pluginfo) {
	sinkMeta := gSinkmetadata
	for fileName, v := range sinkMeta {
		node := new(pluginfo)
		node.Name = strings.TrimSuffix(fileName, internal.JsonFileSuffix)
		node.About = v.About
		i := 0
		for ; i < len(sinks); i++ {
			if node.Name <= sinks[i].Name {
				sinks = append(sinks, node)
				copy(sinks[i+1:], sinks[i:])
				sinks[i] = node
				break
			}
		}
		if len(sinks) == i {
			sinks = append(sinks, node)
		}
	}
	return sinks
}
