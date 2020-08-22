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
	language struct {
		English string `json:"en_US"`
		Chinese string `json:"zh_CN"`
	}
	field struct {
		Name     string      `json:"name"`
		Default  interface{} `json:"default"`
		Type     string      `json:"type"`
		Control  string      `json:"control"`
		Optional bool        `json:"optional"`
		Values   interface{} `json:"values"`
		Hint     *language   `json:"hint"`
		Label    *language   `json:"label"`
	}
	sinkMeta struct {
		Author  *author   `json:"author"`
		HelpUrl *language `json:"helpUrl"`
		Libs    []string  `json:"libs"`
		Fields  []*field  `json:"properties"`
	}
)

var g_sinkMetadata map[string]*sinkMeta //map[fileName]
func readSinkMetaDir() error {
	confDir, err := common.GetLoc("/plugins")
	if nil != err {
		return err
	}

	dir := path.Join(confDir, "sinks")
	tmpMap := make(map[string]*sinkMeta)
	infos, err := ioutil.ReadDir(dir)
	if nil != err {
		return err
	}
	for _, info := range infos {
		fileName := info.Name()
		if !strings.HasSuffix(fileName, ".json") {
			continue
		}

		filePath := path.Join(dir, fileName)
		ptrMetadata := new(sinkMeta)
		err = common.ReadJsonUnmarshal(filePath, ptrMetadata)
		if nil != err {
			return fmt.Errorf("fileName:%s err:%v", fileName, err)
		}

		common.Log.Infof("sinkMeta file : %s", fileName)
		tmpMap[fileName] = ptrMetadata
	}
	g_sinkMetadata = tmpMap
	return nil
}

func readSinkMetaFile(filePath string) error {
	ptrMetadata := new(sinkMeta)
	err := common.ReadJsonUnmarshal(filePath, ptrMetadata)
	if nil != err {
		return fmt.Errorf("filePath:%s err:%v", filePath, err)
	}

	sinkMetadata := g_sinkMetadata
	tmpMap := make(map[string]*sinkMeta)
	for k, v := range sinkMetadata {
		tmpMap[k] = v
	}
	fileName := path.Base(filePath)
	common.Log.Infof("sinkMeta file : %s", fileName)
	tmpMap[fileName] = ptrMetadata
	g_sinkMetadata = tmpMap
	return nil
}

type (
	hintLanguage struct {
		English string `json:"en"`
		Chinese string `json:"zh"`
	}
	hintField struct {
		Name     string        `json:"name"`
		Default  interface{}   `json:"default"`
		Control  string        `json:"control"`
		Optional bool          `json:"optional"`
		Type     string        `json:"type"`
		Hint     *hintLanguage `json:"hint"`
		Label    *hintLanguage `json:"label"`
		Values   interface{}   `json:"values"`
	}
	sinkPropertyNode struct {
		Fields  []*hintField  `json:"properties"`
		HelpUrl *hintLanguage `json:"helpUrl"`
		Libs    []string      `json:"libs"`
	}
	sinkProperty struct {
		CustomProperty map[string]*sinkPropertyNode `json:"customProperty"`
		BaseProperty   map[string]*sinkPropertyNode `json:"baseProperty"`
		BaseOption     *sinkPropertyNode            `json:"baseOption"`
	}
)

func (this *hintLanguage) set(l *language) {
	if nil == l {
		return
	}
	this.English = l.English
	this.Chinese = l.Chinese
}
func (this *hintField) setSinkField(v *field) {
	if nil == v {
		return
	}
	this.Name = v.Name
	this.Type = v.Type
	this.Default = v.Default
	this.Values = v.Values
	this.Control = v.Control
	this.Optional = v.Optional
	this.Hint = new(hintLanguage)
	this.Hint.set(v.Hint)
	this.Label = new(hintLanguage)
	this.Label.set(v.Label)
}

func (this *sinkPropertyNode) setNodeFromMetal(data *sinkMeta) {
	if nil == data {
		return
	}
	this.Libs = data.Libs
	if nil != data.HelpUrl {
		this.HelpUrl = new(hintLanguage)
		this.HelpUrl.set(data.HelpUrl)
	}
	for _, v := range data.Fields {
		field := new(hintField)
		field.setSinkField(v)
		this.Fields = append(this.Fields, field)
	}
}

func (this *sinkProperty) setCustomProperty(pluginName string) error {
	fileName := pluginName + `.json`
	sinkMetadata := g_sinkMetadata
	data := sinkMetadata[fileName]
	if nil == data {
		return fmt.Errorf(`not found pligin:%s`, fileName)
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
		return fmt.Errorf(`not found pligin:%s`, baseProperty)
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
		return fmt.Errorf(`not found pligin:%s`, baseOption)
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

func GetSinkMeta(pluginName string, rule *api.Rule) (ptrSinkProperty *sinkProperty, err error) {
	ptrSinkProperty = new(sinkProperty)
	if nil == rule {
		err = ptrSinkProperty.hintWhenNewSink(pluginName)
	} else {
		err = ptrSinkProperty.hintWhenModifySink(rule)
	}
	return ptrSinkProperty, err
}

func GetSinks() (sinks []string) {
	sinkMeta := g_sinkMetadata
	for fileName, _ := range sinkMeta {
		if fileName == baseProperty+".json" || fileName == baseOption+".json" {
			continue
		}
		sinks = append(sinks, strings.TrimSuffix(fileName, `.json`))
	}
	return sinks
}
