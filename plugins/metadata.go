package plugins

import (
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xstream/api"
	"io/ioutil"
	"path"
	"strings"
	"sync"
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
		Control  string      `json:"control"`
		Type     string      `json:"type"`
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
	sourceMeta struct {
		Author   *author             `json:"author"`
		HelpUrl  *language           `json:"helpUrl"`
		Libs     []string            `json:"libs"`
		ConfKeys map[string][]*field `json:"properties"`
	}
)

var (
	g_sourceMetadata map[string]*sourceMeta //map[fileName]
	g_sourceMutex    sync.Mutex
)

func (this *Manager) readSourceMetaFile(filePath string) (*sourceMeta, error) {
	ptrMeta := new(sourceMeta)
	err := common.ReadJsonUnmarshal(filePath, ptrMeta)
	if nil != err || nil == ptrMeta.ConfKeys {
		return nil, fmt.Errorf("file:%s err:%v", filePath, err)
	}

	yamlData := make(map[string]map[string]interface{})
	filePath = strings.TrimSuffix(filePath, `.json`) + `.yaml`
	err = common.ReadYamlUnmarshal(filePath, &yamlData)
	if nil != err {
		return nil, fmt.Errorf("file:%s err:%v", filePath, err)
	}
	common.Log.Infof("sourceMeta file : %s", filePath)

	for key, _ := range yamlData {
		var fields []*field
		tmpFields := ptrMeta.ConfKeys[key]
		if nil == tmpFields {
			defFields := ptrMeta.ConfKeys["default"]
			for _, pfield := range defFields {
				p := new(field)
				*p = *pfield
				fields = append(fields, p)
				ptrMeta.ConfKeys[key] = fields
			}
		}
	}

	for key, kvs := range yamlData {
		fields := ptrMeta.ConfKeys[key]
		for i, field := range fields {
			if v, ok := kvs[field.Name]; ok {
				fields[i].Default = v
			}
		}
	}
	return ptrMeta, err
}

func (this *Manager) readSourceMetaDir() error {
	confDir, err := common.GetConfLoc()
	if nil != err {
		return err
	}

	dir := path.Join(confDir, "sources")
	infos, err := ioutil.ReadDir(dir)
	if nil != err {
		return err
	}

	tmpMap := make(map[string]*sourceMeta)
	tmpMap["mqtt_source.json"], err = this.readSourceMetaFile(path.Join(confDir, "mqtt_source.json"))
	for _, info := range infos {
		fileName := info.Name()
		if strings.HasSuffix(fileName, ".json") {
			filePath := path.Join(dir, fileName)
			tmpMap[fileName], err = this.readSourceMetaFile(filePath)
			if nil != err {
				return err
			}
		}
	}

	g_sourceMutex.Lock()
	g_sourceMetadata = tmpMap
	g_sourceMutex.Unlock()
	return nil
}

var g_sinkMetadata map[string]*sinkMeta //map[fileName]
func (this *Manager) readSinkMetaDir() error {
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

func (this *Manager) readMetaDir() error {
	err := this.readSourceMetaDir()
	if nil != err {
		return err
	}
	return this.readSinkMetaDir()
}

func (this *Manager) readSinkMetaFile(filePath string) error {
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

/*
func (this *Manager) delMetadata(pluginName string) {
	sinkMetadata := g_sinkMetadata
	if _, ok := sinkMetadata[pluginName]; !ok {
		return
	}
	tmp := make(map[string]*sinkMeta)
	fileName := pluginName + `.json`
	foruOB k, v := range sinkMetadata {
		if k != fileName {
			tmp[k] = v
		}
	}
	g_sinkMetadata = tmp
}
*/

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
	this.English = l.English
	this.Chinese = l.Chinese
}
func (this *hintField) setSinkField(v *field) {
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

func (this *Manager) SinkMetadata(pluginName string, rule *api.Rule) (ptrSinkProperty *sinkProperty, err error) {
	ptrSinkProperty = new(sinkProperty)
	if nil == rule {
		err = ptrSinkProperty.hintWhenNewSink(pluginName)
	} else {
		err = ptrSinkProperty.hintWhenModifySink(rule)
	}
	return ptrSinkProperty, err
}

func (this *Manager) SourceMetadata(pluginName string) (ptrSourceProperty *sourceMeta, err error) {
	g_sourceMutex.Lock()
	defer g_sourceMutex.Unlock()
	if data, ok := g_sourceMetadata[pluginName+".json"]; ok {
		return data, nil
	}
	return nil, fmt.Errorf("not found plugin %s", pluginName)
}

func (this *Manager) GetSinks() (sinks []string) {
	sinkMeta := g_sinkMetadata
	for fileName, _ := range sinkMeta {
		if fileName == baseProperty+".json" || fileName == baseOption+".json" {
			continue
		}
		sinks = append(sinks, strings.TrimSuffix(fileName, `.json`))
	}
	return sinks
}

func (this *Manager) GetSources() (sources []string) {
	g_sourceMutex.Lock()
	defer g_sourceMutex.Unlock()
	for fileName, _ := range g_sourceMetadata {
		sources = append(sources, strings.TrimSuffix(fileName, `.json`))
	}
	return sources
}

func (this *Manager) GetSourceConfKeys(pluginName string) (keys []string) {
	g_sourceMutex.Lock()
	defer g_sourceMutex.Unlock()
	meta := g_sourceMetadata[pluginName+".json"]
	if nil == meta {
		return nil
	}
	for k, _ := range meta.ConfKeys {
		keys = append(keys, k)
	}
	return keys
}

func (this *Manager) DelSourceConfKey(pluginName, confKey string) error {
	g_sourceMutex.Lock()
	meta := g_sourceMetadata[pluginName+".json"]
	if nil == meta {
		g_sourceMutex.Unlock()
		return fmt.Errorf("not found plugin %s", pluginName)
	}
	if nil == meta.ConfKeys {
		g_sourceMutex.Unlock()
		return fmt.Errorf("not found confKey %s", confKey)
	}
	delete(meta.ConfKeys, confKey)
	g_sourceMutex.Unlock()
	return saveSourceConf(pluginName)
}

func (this *Manager) AddSourceConfKey(pluginName, confKey, content string) error {
	reqField := make(map[string]interface{})
	err := json.Unmarshal([]byte(content), &reqField)
	if nil != err {
		return err
	}

	g_sourceMutex.Lock()
	meta := g_sourceMetadata[pluginName+".json"]
	if nil == meta {
		g_sourceMutex.Unlock()
		return fmt.Errorf("not found plugin %s", pluginName)
	}

	if nil == meta.ConfKeys {
		g_sourceMutex.Unlock()
		return fmt.Errorf("not found confKey %s", confKey)
	}

	if 0 != len(meta.ConfKeys[confKey]) {
		g_sourceMutex.Unlock()
		return fmt.Errorf("exist confKey %s", confKey)
	}

	defFields := meta.ConfKeys["default"]
	if 0 == len(defFields) {
		g_sourceMutex.Unlock()
		return fmt.Errorf("not found confKey default")
	}
	var newConfKey []*field
	for _, defField := range defFields {
		p := new(field)
		*p = *defField
		newConfKey = append(newConfKey, p)
	}

	for k, v := range reqField {
		for i, field := range newConfKey {
			if k == field.Name {
				newConfKey[i].Default = v
				break
			}
		}
	}

	meta.ConfKeys[confKey] = newConfKey
	g_sourceMutex.Unlock()
	return saveSourceConf(pluginName)
}
func (this *Manager) UpdateSourceConfKey(pluginName, confKey, content string) error {
	reqField := make(map[string]interface{})
	err := json.Unmarshal([]byte(content), &reqField)
	if nil != err {
		return err
	}

	g_sourceMutex.Lock()
	meta := g_sourceMetadata[pluginName+".json"]
	if nil == meta {
		g_sourceMutex.Unlock()
		return fmt.Errorf("not found plugin %s", pluginName)
	}

	if nil == meta.ConfKeys {
		g_sourceMutex.Unlock()
		return fmt.Errorf("not found confKey %s", confKey)
	}

	oldFields := meta.ConfKeys[confKey]
	if 0 == len(oldFields) {
		g_sourceMutex.Unlock()
		return fmt.Errorf("not found confKey %s", confKey)
	}

	for k, v := range reqField {
		for i, field := range oldFields {
			if k == field.Name {
				oldFields[i].Default = v
				break
			}
		}
	}
	g_sourceMutex.Unlock()
	return saveSourceConf(pluginName)
}

func saveSourceConf(pluginName string) error {
	confDir, err := common.GetConfLoc()
	if nil != err {
		return err
	}
	filePath := path.Join(confDir, "sources", pluginName+".yaml")
	if "mqtt_source" == pluginName {
		filePath = path.Join(confDir, pluginName+".yaml")
	}

	g_sourceMutex.Lock()
	meta := g_sourceMetadata[pluginName+".json"]
	if nil == meta {
		g_sourceMutex.Unlock()
		return fmt.Errorf("not found plugin %s", pluginName)
	}
	confData := make(map[string]map[string]interface{})
	for key, fields := range meta.ConfKeys {
		confKey := make(map[string]interface{})
		for _, field := range fields {
			confKey[field.Name] = field.Default
		}
		confData[key] = confKey
	}
	g_sourceMutex.Unlock()
	return common.WriteYamlMarshal(filePath, confData)
}
