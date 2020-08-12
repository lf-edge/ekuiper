package plugins

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path"
	"strings"
)

const (
	baseProperty = `baseProperty`
	baseOption   = `baseOption`
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
	}
	metadata struct {
		Author  *author   `json:"author"`
		HelpUrl *language `json:"helpUrl"`
		Fields  []*field  `json:"properties"`
		Libs    []string  `json:"libs"`
	}
)

var g_sinkMetadata map[string]*metadata //map[fileName]
func (this *language) getLanguage(language string) (ret string) {
	if "chinese" == language {
		ret = this.Chinese
		if 0 == len(ret) {
			return this.English
		}
	} else {
		ret = this.English
		if 0 == len(ret) {
			return this.Chinese
		}
	}
	return ret
}

func getLabel(pluginName, fieldName, language string) string {
	sinkMetadata := g_sinkMetadata
	data := sinkMetadata[pluginName]
	if nil == data {
		return ""
	}
	for _, v := range data.Fields {
		if fieldName == v.Name {
			if nil == v.Label {
				return ""
			} else {
				return v.Label.getLanguage(language)
			}
		}
	}
	return ""
}

func getHint(pluginName, fieldName, language string) string {
	sinkMetadata := g_sinkMetadata
	data := sinkMetadata[pluginName]
	if nil == data {
		return ""
	}
	for _, v := range data.Fields {
		if fieldName == v.Name {
			if nil == v.Hint {
				return ""
			} else {
				return v.Hint.getLanguage(language)
			}
		}
	}
	return ""
}

func getHelpUrl(pluginName, language string) string {
	sinkMetadata := g_sinkMetadata
	data := sinkMetadata[pluginName]
	if nil == data {
		return ""
	}
	if nil == data.HelpUrl {
		return ""
	}
	return data.HelpUrl.getLanguage(language)
}

func (this *Manager) readMetadataDir(dir string) error {
	tmpMap := make(map[string]*metadata)
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
		byteContent, err := ioutil.ReadFile(filePath)
		if nil != err {
			return err
		}

		ptrMetadata := new(metadata)
		err = json.Unmarshal(byteContent, ptrMetadata)
		if nil != err {
			return fmt.Errorf("fileName:%s err:%v", fileName, err)
		}
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
	tmpMap[fileName] = ptrMetadata
	g_sinkMetadata = tmpMap

	return nil
}

type (
	sinkField struct {
		Optional bool        `json:"optional"`
		Name     string      `json:"name"`
		Control  string      `json:"control"`
		Hint     string      `json:"hint"`
		Label    string      `json:"label"`
		Type     string      `json:"type"`
		Default  interface{} `json:"default"`
	}
	sinkPropertyNode struct {
		HelpUrl string       `json:"helpUrl"`
		Fields  []*sinkField `json:"properties"`
		Libs    []string     `json:"libs"`
	}
	sinkProperty struct {
		CustomProperty map[string]*sinkPropertyNode `json:"customProperty"`
		BaseProperty   *sinkPropertyNode            `json:"baseProperty"`
		BaseOption     *sinkPropertyNode            `json:"baseOption"`
	}
)

func (this *sinkField) setSinkField(v *field, language string) {
	this.Name = v.Name
	this.Type = v.Type
	this.Default = v.Default
	this.Control = v.Control
	this.Optional = v.Optional
	if nil != v.Hint {
		this.Hint = v.Hint.getLanguage(language)
	}
	if nil != v.Label {
		this.Label = v.Label.getLanguage(language)
	}
}

func (this *sinkPropertyNode) setNodeFromMetal(data *metadata, language string) {
	this.Libs = data.Libs
	if nil != data.HelpUrl {
		this.HelpUrl = data.HelpUrl.getLanguage(language)
	}
	for _, v := range data.Fields {
		field := new(sinkField)
		field.setSinkField(v, language)
		this.Fields = append(this.Fields, field)
	}
}

func (this *sinkProperty) setCustomProperty(pluginName, language string) error {
	fileName := pluginName + `.json`
	sinkMetadata := g_sinkMetadata
	data := sinkMetadata[fileName]
	if nil == data {
		return fmt.Errorf(`not find pligin:%s`, fileName)
	}
	node := new(sinkPropertyNode)
	node.setNodeFromMetal(data, language)
	if 0 == len(this.CustomProperty) {
		this.CustomProperty = make(map[string]*sinkPropertyNode)
	}
	this.CustomProperty[pluginName] = node
	return nil
}

func (this *sinkProperty) setBasePropertry(language string) error {
	sinkMetadata := g_sinkMetadata
	data := sinkMetadata[baseProperty+".json"]
	if nil == data {
		return fmt.Errorf(`not find pligin:%s`, baseProperty)
	}
	node := new(sinkPropertyNode)
	node.setNodeFromMetal(data, language)
	this.BaseProperty = node
	return nil
}

func (this *sinkProperty) setBaseOption(language string) error {
	sinkMetadata := g_sinkMetadata
	data := sinkMetadata[baseOption+".json"]
	if nil == data {
		return fmt.Errorf(`not find pligin:%s`, baseOption)
	}
	node := new(sinkPropertyNode)
	node.setNodeFromMetal(data, language)
	this.BaseOption = node
	return nil
}

func (this *sinkProperty) hintWhenNewSink(name, language string) (err error) {
	err = this.setCustomProperty(name, language)
	if nil != err {
		return err
	}
	err = this.setBasePropertry(language)
	if nil != err {
		return err
	}
	err = this.setBaseOption(language)
	return err
}

func (this *Manager) HintSink(name, language string) (ret []byte, err error) {
	ptrSinkProperty := new(sinkProperty)
	err = ptrSinkProperty.hintWhenNewSink(name, language)
	return json.Marshal(ptrSinkProperty)
}

/*
func (this *sinkProperty) showCustomProperty(pluginName, language string, action map[string]interface{}) (err error) {
	err = this.setCustomProperty(pluginName, language)
	if nil != err {
		return err
	}
for ack,acv := range action{
  for cusk,cusv := range this.CustomProperty{

  }
}
	for k, v := range this.CustomProperty {
		if k == pluginName {
			v.HelpUrl = getHelpUrl(pluginName, language)
			for _, field := range v.Fields {
        for key val := range action{
            if key == field.Name{
				field.Default=
				field.Label = getLabel(pluginName, field.Name, language)
				field.Hint = getHint(pluginName, field.Name, language)

            }
        }
			}
		}
	}
	return nil
}

/*
func (this *sinkProperty)showBaseProperty(name string, action map[string]interface{}) (err error) {
		BaseProperty   *sinkPropertyNode            `json:"baseProperty"`
}
func (this *sinkProperty)showBaseOption(name string, action map[string]interface{}) (err error) {
		BaseOption     *sinkPropertyNode            `json:"baseOption"`
}
*/
//func (this *sinkProperty) hintWhenModifySink(rule *api.Rule, language string) (err error) {
/*
	rule, err := this.GetRuleByName(pluginName)
	if err != nil {
		return err
	}
	err = this.setBasePropertry(name, language)
	if nil != err {
		return err
	}
	err = this.setBaseOption(name, language)
	if nil != err {
		return err
	}
*/
/*
	for _, m := range rule.Actions {
		for pluginName, action := range m {
			props, ok := action.(map[string]interface{})
			if !ok {
				return fmt.Errorf(`illegal %s format`, pluginName)
			}

			err = this.showCustomProperty(pluginName, language, props)
			if nil != err {
				return err
			}
		}
	}
	return err
}
*/
