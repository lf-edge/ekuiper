package plugins

import (
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xstream/api"
	"io/ioutil"
	"path"
	"strings"
)

const (
	baseProperty = `properties`
	baseOption   = `options`
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
		Name     string        `json:"name"`
		Default  interface{}   `json:"default"`
		Control  string        `json:"control"`
		Optional bool          `json:"optional"`
		Type     string        `json:"type"`
		Hint     *fileLanguage `json:"hint"`
		Label    *fileLanguage `json:"label"`
		Values   interface{}   `json:"values"`
	}
	fileAbout struct {
		Trial       bool          `json:"trial "`
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
		Trial       bool      `json:"trial "`
		Author      *author   `json:"author"`
		HelpUrl     *language `json:"helpUrl"`
		Description *language `json:"description"`
	}
	field struct {
		Exist    bool        `json:"exist"`
		Name     string      `json:"name"`
		Default  interface{} `json:"default"`
		Type     string      `json:"type"`
		Control  string      `json:"control"`
		Optional bool        `json:"optional"`
		Values   interface{} `json:"values"`
		Hint     *language   `json:"hint"`
		Label    *language   `json:"label"`
	}

	uiSink struct {
		About  *about   `json:"about"`
		Libs   []string `json:"libs"`
		Fields []*field `json:"properties"`
	}
	uiSinks struct {
		CustomProperty map[string]*uiSink `json:"customProperty"`
		BaseProperty   map[string]*uiSink `json:"baseProperty"`
		BaseOption     *uiSink            `json:"baseOption"`
	}
)

//const internal sinks
var InternalSinks = [...]string{"log", "mqtt", "rest", "nop", "edgex"}

func newLanguage(fi *fileLanguage) *language {
	if nil == fi {
		return nil
	}
	ui := new(language)
	ui.English = fi.English
	ui.Chinese = fi.Chinese
	return ui
}
func newField(fi *fileField) *field {
	if nil == fi {
		return nil
	}
	ui := new(field)
	ui.Name = fi.Name
	ui.Default = fi.Default
	ui.Type = fi.Type
	ui.Control = fi.Control
	ui.Optional = fi.Optional
	ui.Values = fi.Values
	ui.Hint = newLanguage(fi.Hint)
	ui.Label = newLanguage(fi.Label)
	return ui
}
func newAbout(fi *fileAbout) *about {
	if nil == fi {
		return nil
	}
	ui := new(about)
	ui.Trial = fi.Trial
	ui.Author = fi.Author
	ui.HelpUrl = newLanguage(fi.HelpUrl)
	ui.Description = newLanguage(fi.Description)
	return ui
}
func newUiSink(fi *fileSink) *uiSink {
	if nil == fi {
		return nil
	}
	ui := new(uiSink)
	ui.Libs = fi.Libs
	ui.About = newAbout(fi.About)
	for _, v := range fi.Fields {
		ui.Fields = append(ui.Fields, newField(v))
	}
	return ui
}

var g_sinkMetadata map[string]*uiSink //map[fileName]
func readSinkMetaDir() error {
	confDir, err := common.GetConfLoc()
	if nil != err {
		return err
	}

	dir := path.Join(confDir, "sinks")
	tmpMap := make(map[string]*uiSink)

	//The internal support sinks
	for _, sink := range InternalSinks {
		file := path.Join(confDir, "sinks", "internal", sink+".json")
		common.Log.Infof("Loading metadata file for sink: %s", file)
		meta := new(fileSink)
		err := common.ReadJsonUnmarshal(file, meta)
		if nil != err {
			return fmt.Errorf("Failed to load internal sink plugin:%s with err:%v", file, err)
		}
		tmpMap[sink+".json"] = newUiSink(meta)
	}
	files, err := ioutil.ReadDir(dir)
	if nil != err {
		return err
	}
	for _, file := range files {
		fname := file.Name()
		if !strings.HasSuffix(fname, ".json") {
			continue
		}

		filePath := path.Join(dir, fname)
		metadata := new(fileSink)
		err = common.ReadJsonUnmarshal(filePath, metadata)
		if nil != err {
			return fmt.Errorf("fname:%s err:%v", fname, err)
		}

		common.Log.Infof("sinkMeta file : %s", fname)
		tmpMap[fname] = newUiSink(metadata)
	}
	g_sinkMetadata = tmpMap
	return nil
}

func readSinkMetaFile(filePath string) error {
	ptrMetadata := new(fileSink)
	err := common.ReadJsonUnmarshal(filePath, ptrMetadata)
	if nil != err {
		return fmt.Errorf("filePath:%s err:%v", filePath, err)
	}

	sinkMetadata := g_sinkMetadata
	tmpMap := make(map[string]*uiSink)
	for k, v := range sinkMetadata {
		tmpMap[k] = v
	}
	fileName := path.Base(filePath)
	common.Log.Infof("sinkMeta file : %s", fileName)
	tmpMap[fileName] = newUiSink(ptrMetadata)
	g_sinkMetadata = tmpMap
	return nil
}
func (this *uiSinks) setCustomProperty(pluginName string) error {
	fileName := pluginName + `.json`
	sinkMetadata := g_sinkMetadata
	data := sinkMetadata[fileName]
	if nil == data {
		return fmt.Errorf(`not found pligin:%s`, fileName)
	}
	if 0 == len(this.CustomProperty) {
		this.CustomProperty = make(map[string]*uiSink)
	}
	this.CustomProperty[pluginName] = data
	return nil
}

func (this *uiSinks) setBasePropertry(pluginName string) error {
	sinkMetadata := g_sinkMetadata
	data := sinkMetadata[baseProperty+".json"]
	if nil == data {
		return fmt.Errorf(`not found pligin:%s`, baseProperty)
	}
	if 0 == len(this.BaseProperty) {
		this.BaseProperty = make(map[string]*uiSink)
	}
	this.BaseProperty[pluginName] = data
	return nil
}

func (this *uiSinks) setBaseOption() error {
	sinkMetadata := g_sinkMetadata
	data := sinkMetadata[baseOption+".json"]
	if nil == data {
		return fmt.Errorf(`not found pligin:%s`, baseOption)
	}
	this.BaseOption = data
	return nil
}

func (this *uiSinks) hintWhenNewSink(pluginName string) (err error) {
	err = this.setCustomProperty(pluginName)
	if nil != err {
		return err
	}
	err = this.setBasePropertry(pluginName)
	if nil != err {
		return err
	}
	err = this.setBaseOption()
	return err
}

func (this *uiSink) modifyPropertyNode(mapFields map[string]interface{}) (err error) {
	for i, field := range this.Fields {
		fieldVal := mapFields[field.Name]
		if nil != fieldVal {
			this.Fields[i].Default = fieldVal
		}
	}
	return nil
}
func (this *uiSinks) modifyProperty(pluginName string, mapFields map[string]interface{}) (err error) {
	customProperty := this.CustomProperty[pluginName]
	if nil != customProperty {
		customProperty.modifyPropertyNode(mapFields)
	}

	baseProperty := this.BaseProperty[pluginName]
	if nil != baseProperty {
		baseProperty.modifyPropertyNode(mapFields)
	}

	return nil
}

func (this *uiSinks) modifyOption(option *api.RuleOption) {
	baseOption := this.BaseOption
	if nil == baseOption {
		return
	}
	for i, field := range baseOption.Fields {
		switch field.Name {
		case `isEventTime`:
			baseOption.Fields[i].Default = option.IsEventTime
		case `lateTol`:
			baseOption.Fields[i].Default = option.LateTol
		case `concurrency`:
			baseOption.Fields[i].Default = option.Concurrency
		case `bufferLength`:
			baseOption.Fields[i].Default = option.BufferLength
		case `sendMetaToSink`:
			baseOption.Fields[i].Default = option.SendMetaToSink
		case `qos`:
			baseOption.Fields[i].Default = option.Qos
		case `checkpointInterval`:
			baseOption.Fields[i].Default = option.CheckpointInterval
		}
	}
}

func (this *uiSinks) hintWhenModifySink(rule *api.Rule) (err error) {
	for _, m := range rule.Actions {
		for pluginName, sink := range m {
			mapFields, _ := sink.(map[string]interface{})
			err = this.hintWhenNewSink(pluginName)
			if nil != err {
				return err
			}
			this.modifyProperty(pluginName, mapFields)
		}
	}
	this.modifyOption(rule.Options)
	return nil
}

func GetSinkMeta(pluginName string, rule *api.Rule) (ptrSinkProperty *uiSinks, err error) {
	ptrSinkProperty = new(uiSinks)
	if nil == rule {
		err = ptrSinkProperty.hintWhenNewSink(pluginName)
	} else {
		err = ptrSinkProperty.hintWhenModifySink(rule)
	}
	return ptrSinkProperty, err
}

type pluginfo struct {
	Name  string `json:"name"`
	About *about `json:"about"`
}

func GetSinks() (sinks []*pluginfo) {
	sinkMeta := g_sinkMetadata
	for fileName, v := range sinkMeta {
		if fileName == baseProperty+".json" || fileName == baseOption+".json" {
			continue
		}
		node := new(pluginfo)
		node.Name = strings.TrimSuffix(fileName, `.json`)
		node.About = v.About
		sinks = append(sinks, node)
	}
	return sinks
}
