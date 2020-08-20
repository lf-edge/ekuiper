package plugins

import (
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/common"
	"io/ioutil"
	"path"
	"strings"
)

type (
	sourceMeta struct {
		Author   *author             `json:"author"`
		HelpUrl  *language           `json:"helpUrl"`
		Libs     []string            `json:"libs"`
		ConfKeys map[string][]*field `json:"properties"`
	}
	sourceProperty struct {
		cf   map[string]map[string]interface{}
		meta *sourceMeta
	}
)

var g_sourceProperty map[string]*sourceProperty

func (this *Manager) readSourceMetaFile(filePath string) (*sourceProperty, error) {
	ptrMeta := new(sourceMeta)
	err := common.ReadJsonUnmarshal(filePath, ptrMeta)
	if nil != err || 0 == len(ptrMeta.ConfKeys) {
		return nil, fmt.Errorf("file:%s err:%v", filePath, err)
	}
	if 0 == len(ptrMeta.ConfKeys["default"]) {
		return nil, fmt.Errorf("not found default confKey %s", filePath)
	}

	yamlData := make(map[string]map[string]interface{})
	filePath = strings.TrimSuffix(filePath, `.json`) + `.yaml`
	err = common.ReadYamlUnmarshal(filePath, &yamlData)
	if nil != err {
		return nil, fmt.Errorf("file:%s err:%v", filePath, err)
	}
	if 0 == len(yamlData["default"]) {
		return nil, fmt.Errorf("not found default confKey from %s", filePath)
	}

	property := new(sourceProperty)
	property.cf = yamlData
	property.meta = ptrMeta

	return property, err
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

	tmpMap := make(map[string]*sourceProperty)
	tmpMap["mqtt_source.json"], err = this.readSourceMetaFile(path.Join(confDir, "mqtt_source.json"))
	if nil != err {
		return err
	}

	for _, info := range infos {
		fileName := info.Name()
		if strings.HasSuffix(fileName, ".json") {
			filePath := path.Join(dir, fileName)
			tmpMap[fileName], err = this.readSourceMetaFile(filePath)
			if nil != err {
				return err
			}
			common.Log.Infof("sourceMeta file : %s", fileName)
		}
	}
	g_sourceProperty = tmpMap
	return nil
}

func (this *Manager) GetSourceMeta(pluginName string) (ptrSourceProperty *sourceMeta, err error) {
	property, ok := g_sourceProperty[pluginName+".json"]
	if ok {
		property.cfToMeta()
		return property.meta, nil
	}
	return nil, fmt.Errorf("not found plugin %s", pluginName)
}

func (this *Manager) GetSources() (sources []string) {
	for fileName, _ := range g_sourceProperty {
		sources = append(sources, strings.TrimSuffix(fileName, `.json`))
	}
	return sources
}

func (this *Manager) GetSourceConfKeys(pluginName string) (keys []string) {
	property := g_sourceProperty[pluginName+".json"]
	if nil == property {
		return keys
	}
	for k, _ := range property.cf {
		keys = append(keys, k)
	}
	return keys
}

func (this *Manager) DelSourceConfKey(pluginName, confKey string) error {
	property := g_sourceProperty[pluginName+".json"]
	if nil == property {
		return fmt.Errorf("not found plugin %s", pluginName)
	}
	if nil == property.cf {
		return fmt.Errorf("not found confKey %s", confKey)
	}
	delete(property.cf, confKey)
	g_sourceProperty[pluginName+".json"] = property
	return property.saveCf(pluginName)
}

func (this *Manager) AddSourceConfKey(pluginName, confKey, content string) error {
	reqField := make(map[string]interface{})
	err := json.Unmarshal([]byte(content), &reqField)
	if nil != err {
		return err
	}

	property := g_sourceProperty[pluginName+".json"]
	if nil == property {
		return fmt.Errorf("not found plugin %s", pluginName)
	}

	if nil == property.cf {
		return fmt.Errorf("not found confKey %s", confKey)
	}

	if 0 != len(property.cf[confKey]) {
		return fmt.Errorf("exist confKey %s", confKey)
	}

	property.cf[confKey] = reqField
	g_sourceProperty[pluginName+".json"] = property
	return property.saveCf(pluginName)
}

func (this *Manager) UpdateSourceConfKey(pluginName, confKey, content string) error {
	reqField := make(map[string]interface{})
	err := json.Unmarshal([]byte(content), &reqField)
	if nil != err {
		return err
	}

	property := g_sourceProperty[pluginName+".json"]
	if nil == property {
		return fmt.Errorf("not found plugin %s", pluginName)
	}

	if nil == property.cf {
		return fmt.Errorf("not found confKey %s", confKey)
	}

	if 0 == len(property.cf[confKey]) {
		return fmt.Errorf("not found confKey %s", confKey)
	}

	for k, v := range reqField {
		property.cf[confKey][k] = v
	}
	g_sourceProperty[pluginName+".json"] = property
	return property.saveCf(pluginName)
}

func (this *sourceProperty) newFields(fields []*field, m map[string]interface{}, sli *[]*field) error {
	for k, v := range m {
		p := new(field)
		for _, fd := range fields {
			if fd.Name == k {
				*p = *fd
				*sli = append(*sli, p)

				switch t := v.(type) {
				case map[interface{}]interface{}:
					tt := common.ConvertMap(t)
					var tmpSli, tmpFields []*field
					p.Default = &tmpSli
					b, err := json.Marshal(fd.Default)
					if nil != err {
						return err
					}
					err = json.Unmarshal(b, &tmpFields)
					if nil != err {
						return err
					}
					this.newFields(tmpFields, tt, &tmpSli)
				case map[string]interface{}:
					var tmpSli []*field
					p.Default = &tmpSli
					this.newFields(fields, t, &tmpSli)
				default:
					p.Default = v
				}
				break
			}
		}
	}
	return nil
}

func (this *sourceProperty) cfToMeta() {
	fields := this.meta.ConfKeys["default"]
	ret := make(map[string][]*field)
	for k, kvs := range this.cf {
		var sli []*field
		this.newFields(fields, kvs, &sli)
		ret[k] = sli
	}
	this.meta.ConfKeys = ret
}

func (this *sourceProperty) saveCf(pluginName string) error {
	confDir, err := common.GetConfLoc()
	if nil != err {
		return err
	}

	dir := path.Join(confDir, "sources")
	if "mqtt_source" == pluginName {
		dir = confDir
	}
	filePath := path.Join(dir, pluginName+".yaml")
	for key, kvs := range this.cf {
		for k, v := range kvs {
			switch t := v.(type) {
			case map[interface{}]interface{}:
				kvs[k] = common.ConvertMap(t)
				this.cf[key] = kvs
			}
		}
	}

	return common.WriteYamlMarshal(filePath, this.cf)

}
