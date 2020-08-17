package plugins

import (
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xstream/api"
	"io/ioutil"
	"path"
	"strings"
)

const (
	baseProperty = `properies`
	baseOption   = `options`
)

type (
	author struct {
		Name    string `json:"name"`
		Email   string `json:"email"`
		Company string `json:"company"`
		Website string `json:"website"`
	}
	language struct {
		English string `json:"en_US"`
		Chinese string `json:"zh_CN"`
	}
	field struct {
		Optional bool        `json:"optional"`
		Name     string      `json:"name"`
		Control  string      `json:"control"`
		Type     string      `json:"type"`
		Hint     *language   `json:"hint"`
		Label    *language   `json:"label"`
		Default  interface{} `json:"default"`
		Values   interface{} `json:"values"`
	}
	metadata struct {
		Author  *author   `json:"author"`
		HelpUrl *language `json:"helpUrl"`
		Fields  []*field  `json:"properties"`
		Libs    []string  `json:"libs"`
	}
)

var g_sinkMetadata map[string]*metadata //map[fileName]
func (this *Manager) delMetadata(pluginName string) {
	sinkMetadata := g_sinkMetadata
	if _, ok := sinkMetadata[pluginName]; !ok {
		return
	}
	tmp := make(map[string]*metadata)
	fileName := pluginName + `.json`
	for k, v := range sinkMetadata {
		if k != fileName {
			tmp[k] = v
		}
	}
	g_sinkMetadata = tmp
}
func (this *Manager) readMetadataDir(dir string) error {
	tmpMap := make(map[string]*metadata)
	infos, err := ioutil.ReadDir(dir)
	if nil != err {
		return err
	}
	//add info log
	for _, info := range infos {
		fileName := info.Name()
		if !strings.HasSuffix(fileName, ".json") {
			continue
		}

		filePath := path.Join(dir, fileName)
		byteContent, err := ioutil.ReadFile(filePath)
		if nil != err {
			return err
		}

		ptrMetadata := new(metadata)
		err = json.Unmarshal(byteContent, ptrMetadata)
		if nil != err {
			return fmt.Errorf("fileName:%s err:%v", fileName, err)
		}
		common.Log.Infof("metadata file : %s", fileName)
		tmpMap[fileName] = ptrMetadata
	}
	g_sinkMetadata = tmpMap
	return nil
}

func (this *Manager) readMetadataFile(filePath string) error {
	byteContent, err := ioutil.ReadFile(filePath)
	if nil != err {
		return err
	}

	ptrMetadata := new(metadata)
	err = json.Unmarshal(byteContent, ptrMetadata)
	if nil != err {
		return fmt.Errorf("filePath:%s err:%v", filePath, err)
	}

	sinkMetadata := g_sinkMetadata
	tmpMap := make(map[string]*metadata)
	for k, v := range sinkMetadata {
		tmpMap[k] = v
	}
	fileName := path.Base(filePath)
	common.Log.Infof("metadata file : %s", fileName)
	tmpMap[fileName] = ptrMetadata
	g_sinkMetadata = tmpMap

	return nil
}

type (
	sinkLanguage struct {
		English string `json:"en"`
		Chinese string `json:"zh"`
	}
	sinkField struct {
		Name     string        `json:"name"`
		Default  interface{}   `json:"default"`
		Control  string        `json:"control"`
		Optional bool          `json:"optional"`
		Type     string        `json:"type"`
		Hint     *sinkLanguage `json:"hint"`
		Label    *sinkLanguage `json:"label"`
		Values   interface{}   `json:"values"`
	}
	sinkPropertyNode struct {
		Fields  []*sinkField  `json:"properties"`
		HelpUrl *sinkLanguage `json:"helpUrl"`
		Libs    []string      `json:"libs"`
	}
	sinkProperty struct {
		CustomProperty map[string]*sinkPropertyNode `json:"customProperty"`
		BaseProperty   map[string]*sinkPropertyNode `json:"baseProperty"`
		BaseOption     *sinkPropertyNode            `json:"baseOption"`
	}
)

func (this *sinkLanguage) set(l *language) {
	this.English = l.English
	this.Chinese = l.Chinese
}
func (this *sinkField) setSinkField(v *field) {
	this.Name = v.Name
	this.Type = v.Type
	this.Default = v.Default
	this.Values = v.Values
	this.Control = v.Control
	this.Optional = v.Optional
	this.Hint = new(sinkLanguage)
	this.Hint.set(v.Hint)
	this.Label = new(sinkLanguage)
	this.Label.set(v.Label)
}

func (this *sinkPropertyNode) setNodeFromMetal(data *metadata) {
	this.Libs = data.Libs
	if nil != data.HelpUrl {
		this.HelpUrl = new(sinkLanguage)
		this.HelpUrl.set(data.HelpUrl)
	}
	for _, v := range data.Fields {
		field := new(sinkField)
		field.setSinkField(v)
		this.Fields = append(this.Fields, field)
	}
}

func (this *sinkProperty) setCustomProperty(pluginName string) error {
	fileName := pluginName + `.json`
	sinkMetadata := g_sinkMetadata
	data := sinkMetadata[fileName]
	if nil == data {
		return fmt.Errorf(`not find pligin:%s`, fileName)
	}
	node := new(sinkPropertyNode)
	node.setNodeFromMetal(data)
	if 0 == len(this.CustomProperty) {
		this.CustomProperty = make(map[string]*sinkPropertyNode)
	}
	this.CustomProperty[pluginName] = node
	return nil
}

func (this *sinkProperty) setBasePropertry(pluginName string) error {
	sinkMetadata := g_sinkMetadata
	data := sinkMetadata[baseProperty+".json"]
	if nil == data {
		return fmt.Errorf(`not find pligin:%s`, baseProperty)
	}
	node := new(sinkPropertyNode)
	node.setNodeFromMetal(data)
	if 0 == len(this.BaseProperty) {
		this.BaseProperty = make(map[string]*sinkPropertyNode)
	}
	this.BaseProperty[pluginName] = node
	return nil
}

func (this *sinkProperty) setBaseOption() error {
	sinkMetadata := g_sinkMetadata
	data := sinkMetadata[baseOption+".json"]
	if nil == data {
		return fmt.Errorf(`not find pligin:%s`, baseOption)
	}
	node := new(sinkPropertyNode)
	node.setNodeFromMetal(data)
	this.BaseOption = node
	return nil
}

func (this *sinkProperty) hintWhenNewSink(pluginName string) (err error) {
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

func (this *sinkPropertyNode) modifyPropertyNode(mapFields map[string]interface{}) (err error) {
	for i, field := range this.Fields {
		fieldVal := mapFields[field.Name]
		if nil != fieldVal {
			this.Fields[i].Default = fieldVal
		}
	}
	return nil
}
func (this *sinkProperty) modifyProperty(pluginName string, mapFields map[string]interface{}) (err error) {
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

func (this *sinkProperty) modifyOption(option *api.RuleOption) {
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
func (this *sinkProperty) hintWhenModifySink(rule *api.Rule) (err error) {
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

func (this *Manager) Metadata(pluginName string, rule *api.Rule) (ptrSinkProperty *sinkProperty, err error) {
	ptrSinkProperty = new(sinkProperty)
	if nil == rule {
		err = ptrSinkProperty.hintWhenNewSink(pluginName)
	} else {
		err = ptrSinkProperty.hintWhenModifySink(rule)
	}
	return ptrSinkProperty, err
}
