package plugins

import (
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/common"
	"io/ioutil"
	"net/http"
	"path"
	"reflect"
	"strings"
)

type (
	fileSource struct {
		About    *fileAbout              `json:"about"`
		Libs     []string                `json:"libs"`
		ConfKeys map[string][]*fileField `json:"properties"`
	}
	uiSource struct {
		About    *about              `json:"about"`
		Libs     []string            `json:"libs"`
		ConfKeys map[string][]*field `json:"properties"`
	}
	sourceProperty struct {
		cf   map[string]map[string]interface{}
		meta *uiSource
	}
)

func isInternalSource(fiName string) bool {
	internal := []string{`edgex.json`, `httppull.json`, `mqtt.json`}
	for _, v := range internal {
		if v == fiName {
			return true
		}
	}
	return false
}
func newUiSource(fi *fileSource) (*uiSource, error) {
	if nil == fi {
		return nil, nil
	}
	var err error
	ui := new(uiSource)
	ui.Libs = fi.Libs
	ui.About = newAbout(fi.About)
	ui.ConfKeys = make(map[string][]*field)

	for k, fields := range fi.ConfKeys {
		if ui.ConfKeys[k], err = newField(fields); nil != err {
			return nil, err
		}

	}
	return ui, nil
}

var g_sourceProperty map[string]*sourceProperty

func (m *Manager) uninstalSource(name string) {
	if v, ok := g_sourceProperty[name+".json"]; ok {
		if ui := v.meta; nil != ui {
			if nil != ui.About {
				ui.About.Installed = false
			}
		}
	}
}
func (m *Manager) readSourceMetaFile(filePath string) error {
	fileName := path.Base(filePath)
	if "mqtt_source.json" == fileName {
		fileName = "mqtt.json"
	}
	ptrMeta := new(fileSource)
	err := common.ReadJsonUnmarshal(filePath, ptrMeta)
	if nil != err || 0 == len(ptrMeta.ConfKeys) {
		return fmt.Errorf("file:%s err:%v", filePath, err)
	}
	if 0 == len(ptrMeta.ConfKeys["default"]) {
		return fmt.Errorf("not found default confKey %s", filePath)
	}
	if nil == ptrMeta.About {
		return fmt.Errorf("not found about of %s", filePath)
	} else if isInternalSource(fileName) {
		ptrMeta.About.Installed = true
	} else {
		_, ptrMeta.About.Installed = m.registry.Get(SOURCE, strings.TrimSuffix(fileName, `.json`))
	}

	yamlData := make(map[string]map[string]interface{})
	filePath = strings.TrimSuffix(filePath, `.json`) + `.yaml`
	err = common.ReadYamlUnmarshal(filePath, &yamlData)
	if nil != err {
		return fmt.Errorf("file:%s err:%v", filePath, err)
	}
	if 0 == len(yamlData["default"]) {
		return fmt.Errorf("not found default confKey from %s", filePath)
	}

	property := new(sourceProperty)
	property.cf = yamlData
	property.meta, err = newUiSource(ptrMeta)
	if nil != err {
		return err
	}

	g_sourceProperty[fileName] = property
	return err
}

func (m *Manager) readSourceMetaDir() error {
	g_sourceProperty = make(map[string]*sourceProperty)
	confDir, err := common.GetConfLoc()
	if nil != err {
		return err
	}

	dir := path.Join(confDir, "sources")
	infos, err := ioutil.ReadDir(dir)
	if nil != err {
		return err
	}

	if err = m.readSourceMetaFile(path.Join(confDir, "mqtt_source.json")); nil != err {
		return err
	}

	for _, info := range infos {
		fileName := info.Name()
		if strings.HasSuffix(fileName, ".json") {
			filePath := path.Join(dir, fileName)
			if err = m.readSourceMetaFile(filePath); nil != err {
				return err
			}
			common.Log.Infof("sourceMeta file : %s", fileName)
		}
	}
	return nil
}

func GetSourceConf(pluginName string) (b []byte, err *multilingualMsg) {
	err = new(multilingualMsg)
	err.msg = new(language)
	if property, ok := g_sourceProperty[pluginName+".json"]; ok {
		cf := make(map[string]map[string]interface{})
		for key, kvs := range property.cf {
			aux := make(map[interface{}]interface{})
			for k, v := range kvs {
				aux[k] = v
			}
			cf[key] = common.ConvertMap(aux)
		}
		if b, e := json.Marshal(cf); nil == e {
			return b, nil
		} else {
			err.code = http.StatusBadRequest
			err.msg.setEn(e.Error())
			err.msg.setZh(fmt.Sprintf(`json 格式化错误：%s`, e.Error()))
			return nil, err
		}
	}
	err.code = http.StatusNotFound
	err.msg.setEn(`not found pligin:` + baseOption)
	err.msg.setZh(`没有找到插件：` + baseOption)
	return nil, err
}

func GetSourceMeta(pluginName string) (ptrSourceProperty *uiSource, err *multilingualMsg) {
	err = new(multilingualMsg)
	err.msg = new(language)
	property, ok := g_sourceProperty[pluginName+".json"]
	if ok {
		return property.cfToMeta()
	}
	err.code = http.StatusNotFound
	err.msg.setEn(`not found pligin:` + pluginName)
	err.msg.setZh(`没有找到插件：` + pluginName)
	return nil, err
}

func GetSources() (sources []*pluginfo) {
	for fileName, v := range g_sourceProperty {
		node := new(pluginfo)
		node.Name = strings.TrimSuffix(fileName, `.json`)
		if nil == v.meta {
			continue
		}
		if nil == v.meta.About {
			continue
		}
		node.About = v.meta.About
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

func GetSourceConfKeys(pluginName string) (keys []string) {
	property := g_sourceProperty[pluginName+".json"]
	if nil == property {
		return keys
	}
	for k, _ := range property.cf {
		keys = append(keys, k)
	}
	return keys
}

func DelSourceConfKey(pluginName, confKey string) (err *multilingualMsg) {
	err = new(multilingualMsg)
	err.msg = new(language)
	property := g_sourceProperty[pluginName+".json"]
	if nil == property {
		err.code = http.StatusNotFound
		err.msg.setZh(`没有找到插件：` + pluginName)
		err.msg.setEn(`not found plugin : ` + pluginName)
		return err
	}
	if nil == property.cf {
		err.code = http.StatusNotFound
		err.msg.setZh(`没有找到配置项：` + confKey)
		err.msg.setEn(`not found confKey: ` + confKey)
		return err
	}
	delete(property.cf, confKey)
	return property.saveCf(pluginName)
}

func AddSourceConfKey(pluginName, confKey string, content []byte) (err *multilingualMsg) {
	err = new(multilingualMsg)
	err.msg = new(language)
	reqField := make(map[string]interface{})
	if e := json.Unmarshal(content, &reqField); nil != e {
		msg := e.Error()
		err.code = http.StatusBadRequest
		err.msg.setZh(`解析数据错误：` + msg)
		err.msg.setEn(msg)
		return err
	}

	property := g_sourceProperty[pluginName+".json"]
	if nil == property {
		err.code = http.StatusNotFound
		err.msg.setZh(`没有找到插件：` + pluginName)
		err.msg.setEn(`not found plugin : ` + pluginName)
		return err
	}

	if nil == property.cf {
		property.cf = make(map[string]map[string]interface{})
	}

	if 0 != len(property.cf[confKey]) {
		err.code = http.StatusBadRequest
		err.msg.setZh(`配置项已经存在：` + confKey)
		err.msg.setEn(`exist confKey: ` + confKey)
		return err
	}

	property.cf[confKey] = reqField
	g_sourceProperty[pluginName+".json"] = property
	return property.saveCf(pluginName)
}

func AddSourceConfKeyField(pluginName, confKey string, content []byte) (err *multilingualMsg) {
	err = new(multilingualMsg)
	err.msg = new(language)
	reqField := make(map[string]interface{})
	e := json.Unmarshal(content, &reqField)
	if nil != e {
		msg := e.Error()
		err.code = http.StatusBadRequest
		err.msg.setZh(`解析数据错误：` + msg)
		err.msg.setEn(msg)
		return err
	}

	property := g_sourceProperty[pluginName+".json"]
	if nil == property {
		err.code = http.StatusNotFound
		err.msg.setZh(`没有找到插件：` + pluginName)
		err.msg.setEn(`not found plugin : ` + pluginName)
		return err
	}

	if nil == property.cf {
		err.code = http.StatusNotFound
		err.msg.setZh(`没有找到配置项：` + confKey)
		err.msg.setEn(`not found confKey: ` + confKey)
		return err
	}

	if nil == property.cf[confKey] {
		err.code = http.StatusNotFound
		err.msg.setZh(`没有找到配置项：` + confKey)
		err.msg.setEn(`not found confKey: ` + confKey)
		return err
	}

	for k, v := range reqField {
		property.cf[confKey][k] = v
	}
	return property.saveCf(pluginName)
}

func recursionDelMap(cf, fields map[string]interface{}) (err *multilingualMsg) {
	err = new(multilingualMsg)
	err.msg = new(language)
	for k, v := range fields {
		if nil == v {
			delete(cf, k)
			continue
		}

		if delKey, ok := v.(string); ok {
			if 0 == len(delKey) {
				delete(cf, k)
				continue
			}

			var auxCf map[string]interface{}
			if nil != common.MapToStruct(cf[k], &auxCf) {
				err.code = http.StatusNotFound
				err.msg.setZh(fmt.Sprintf("找不到删除字段：%s.%s", k, delKey))
				err.msg.setEn(fmt.Sprintf("not found second key:%s.%s", k, delKey))
				return err
			}
			cf[k] = auxCf
			delete(auxCf, delKey)
			continue
		}
		if reflect.Map == reflect.TypeOf(v).Kind() {
			var auxCf, auxFields map[string]interface{}
			if nil != common.MapToStruct(cf[k], &auxCf) {
				err.code = http.StatusNotFound
				err.msg.setZh(fmt.Sprintf("找不到删除字段：%s.%s", k, v))
				err.msg.setEn(fmt.Sprintf("not found second key:%s.%s", k, v))
				return err
			}
			cf[k] = auxCf
			if nil != common.MapToStruct(v, &auxFields) {
				err.code = http.StatusBadRequest
				err.msg.setZh(fmt.Sprintf("类型转换错误：%s.%v", k, v))
				err.msg.setEn(fmt.Sprintf("format err:%s.%v", k, v))
				return err
			}
			if err := recursionDelMap(auxCf, auxFields); nil != err {
				return err
			}
		}
	}
	return nil
}

func DelSourceConfKeyField(pluginName, confKey string, content []byte) (err *multilingualMsg) {
	err = new(multilingualMsg)
	err.msg = new(language)
	reqField := make(map[string]interface{})
	e := json.Unmarshal(content, &reqField)
	if nil != e {
		msg := e.Error()
		err.code = http.StatusBadRequest
		err.msg.setZh(`解析 json 错误：` + msg)
		err.msg.setEn(msg)
		return err
	}

	property := g_sourceProperty[pluginName+".json"]
	if nil == property {
		err.code = http.StatusNotFound
		err.msg.setZh(`没有找到插件：` + pluginName)
		err.msg.setEn(`not found plugin : ` + pluginName)
		return err
	}

	if nil == property.cf {
		err.code = http.StatusNotFound
		err.msg.setZh(`没有找到配置项：` + confKey)
		err.msg.setEn(`not found confKey: ` + confKey)
		return err
	}

	if nil == property.cf[confKey] {
		err.code = http.StatusNotFound
		err.msg.setZh(`没有找到配置项：` + confKey)
		err.msg.setEn(`not found confKey: ` + confKey)
		return err
	}

	err = recursionDelMap(property.cf[confKey], reqField)
	if nil != err {
		return err
	}
	return property.saveCf(pluginName)
}

func recursionNewFields(template []*field, conf map[string]interface{}, ret *[]*field) (err *multilingualMsg) {
	for i := 0; i < len(template); i++ {
		p := new(field)
		*p = *template[i]
		*ret = append(*ret, p)
		v, ok := conf[template[i].Name]
		if ok {
			p.Exist = true
		} else {
			p.Exist = false
			continue
		}

		var auxRet, auxTemplate []*field
		p.Default = &auxRet
		if nil == v {
			p.Default = v
		} else {
			if reflect.Map == reflect.TypeOf(v).Kind() {
				var nextCf map[string]interface{}
				if tmp, ok := v.(map[interface{}]interface{}); ok {
					nextCf = common.ConvertMap(tmp)
				} else {
					if e := common.MapToStruct(v, &nextCf); nil != e {
						msg := e.Error()
						err = new(multilingualMsg)
						err.code = http.StatusBadRequest
						err.msg = new(language)
						err.msg.setZh(`类型转换失败：` + msg)
						err.msg.setEn(msg)
						return err
					}
				}
				if e := common.MapToStruct(template[i].Default, &auxTemplate); nil != e {
					msg := e.Error()
					err = new(multilingualMsg)
					err.msg = new(language)
					err.code = http.StatusBadRequest
					err.msg.setZh(`类型转换失败：` + msg)
					err.msg.setEn(msg)
					return err
				}
				if err = recursionNewFields(auxTemplate, nextCf, &auxRet); nil != err {
					return err
				}
			} else {
				p.Default = v
			}
		}
	}
	return nil
}

func (this *sourceProperty) cfToMeta() (*uiSource, *multilingualMsg) {
	fields := this.meta.ConfKeys["default"]
	ret := make(map[string][]*field)
	for k, kvs := range this.cf {
		var sli []*field
		err := recursionNewFields(fields, kvs, &sli)
		if nil != err {
			return nil, err
		}
		ret[k] = sli
	}
	meta := new(uiSource)
	*meta = *(this.meta)
	meta.ConfKeys = ret
	return meta, nil
}

func (this *sourceProperty) saveCf(pluginName string) (err *multilingualMsg) {
	err = new(multilingualMsg)
	err.msg = new(language)
	confDir, e := common.GetConfLoc()
	if nil != e {
		msg := e.Error()
		err.code = http.StatusBadRequest
		err.msg.setZh(`获取不到配置文件的存储路径：` + msg)
		err.msg.setEn(msg)
		return err
	}

	dir := path.Join(confDir, "sources")
	if "mqtt" == pluginName {
		pluginName = "mqtt_source"
		dir = confDir
	}
	filePath := path.Join(dir, pluginName+".yaml")
	e = common.WriteYamlMarshal(filePath, this.cf)
	if nil != e {
		msg := e.Error()
		err.code = http.StatusBadRequest
		err.msg.setZh(`写入配置文件错误：` + msg)
		err.msg.setEn(msg)
		return err
	}
	return nil
}
