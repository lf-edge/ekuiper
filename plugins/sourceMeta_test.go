package plugins

import (
	//"net/http"
	//"net/http/httptest"
	"encoding/json"
	"testing"
	// "encoding/json"
	"fmt"
	"reflect"
)

var (
	g_file     string = `httppull.json`
	g_plugin   string = `httppull`
	g_cf       string = `{"default":{"url":"http://localhost","method":"post","interval":10000,"timeout":5000,"body":"{}","bodyType":"json","headers":{"Accept":"application/json"}},"ck1":{"url":"127.0.0.1:9527","method":"get","interval":1000,"headers":{"Accept":"application/json"}},"ck2":{"method":"delete","interval":100,"url":"http://localhost:9090/pull"}}`
	g_template string = `{"author":{"name":"Jiyong Huang","email":"huangjy@emqx.io","company":"EMQ Technologies Co., Ltd","website":"https://www.emqx.io"},"libs":[],"helpUrl":{"en_US":"https://github.com/emqx/kuiper/blob/master/docs/en_US/rules/sources/http_pull.md","zh_CN":"https://github.com/emqx/kuiper/blob/master/docs/zh_CN/rules/sources/http_pull.md"},"properties":{"default":[{"name":"url","default":"127.0.0.1:5536","optional":false,"control":"text","type":"string","hint":{"en_US":"The URL where to get the result.","zh_CN":"获取结果的 URL"},"label":{"en_US":"URL","zh_CN":"路径"}},{"name":"method","default":"","optional":false,"control":"text","type":"string","hint":{"en_US":"HTTP method, it could be post, get, put & delete.","zh_CN":"HTTP 方法，它可以是 post、get、put 和 delete。"},"label":{"en_US":"HTTP method","zh_CN":"HTTP 方法"}},{"name":"interval","default":1000,"optional":false,"control":"text","type":"int","hint":{"en_US":"The interval between the requests, time unit is ms.","zh_CN":"请求之间的间隔时间，单位为 ms"},"label":{"en_US":"Interval","zh_CN":"间隔时间"}},{"name":"timeout","default":5000,"optional":false,"control":"text","type":"int","hint":{"en_US":"The timeout for http request, time unit is ms.","zh_CN":"http 请求的超时时间，单位为 ms"},"label":{"en_US":"Timeout","zh_CN":"超时时间"}},{"name":"incremental","default":false,"optional":false,"control":"text","type":"bool","hint":{"en_US":"If it's set to true, then will compare with last result; If response of two requests are the same, then will skip sending out the result.","zh_CN":"如果将其设置为 true，则将与最后的结果进行比较； 如果两个请求的响应相同，则将跳过发送结果。"},"label":{"en_US":"Incremental","zh_CN":"递增"}},{"name":"body","default":"{}","optional":false,"control":"text","type":"string","hint":{"en_US":"The body of request","zh_CN":"请求的正文"},"label":{"en_US":"Body","zh_CN":"正文"}},{"name":"bodyType","default":"json","optional":false,"control":"text","type":"string","hint":{"en_US":"Body type, it could be none|text|json|html|xml|javascript|format.","zh_CN":"正文类型,可以是 none|text|json|html|xml|javascript| 格式"},"label":{"en_US":"Body type","zh_CN":"正文类型"}},{"name":"headers","default":[{"name":"Accept","default":"application/json","optional":false,"control":"text","type":"string","hint":{"en_US":"HTTP headers","zh_CN":"HTTP标头"},"label":{"en_US":"HTTP headers","zh_CN":"HTTP标头"}}],"optional":false,"control":"text","type":"string","hint":{"en_US":"The HTTP request headers that you want to send along with the HTTP request.","zh_CN":"需要与 HTTP 请求一起发送的 HTTP 请求标头。"},"label":{"en_US":"HTTP headers","zh_CN":"HTTP标头"}}]}}`
)

func TestGetSourceMeta(t *testing.T) {
	source := new(sourceProperty)
	var cf map[string]map[string]interface{}
	if err := json.Unmarshal([]byte(g_cf), &cf); nil != err {
		t.Error(err)
	}

	var fileMeta = new(fileSource)
	if err := json.Unmarshal([]byte(g_template), fileMeta); nil != err {
		t.Error(err)
	}
	meta, err := newUiSource(fileMeta)
	if nil != err {
		t.Error(err)
	}
	source.cf = cf
	source.meta = meta
	g_sourceProperty = make(map[string]*sourceProperty)
	g_sourceProperty[g_file] = source

	showMeta, err := GetSourceMeta(g_plugin, "zh_CN")
	if nil != err {
		t.Error(err)
	}
	if err := compare(source, showMeta); nil != err {
		t.Error(err)
	}
	addData := `{"url":"127.0.0.1","method":"post","headers":{"Accept":"json"}}`
	delData := `{"method":"","headers":{"Accept":""}}`

	if err := AddSourceConfKey(g_plugin, "new", "zh_CN", []byte(addData)); nil != err {
		t.Error(err)
	}
	if err := isAddData(addData, cf[`new`]); nil != err {
		t.Error(err)
	}

	if err := DelSourceConfKeyField(g_plugin, "new", "zh_CN", []byte(delData)); nil != err {
		t.Error(err)
	}
	if err := isDelData(delData, cf[`new`]); nil != err {
		t.Error(err)
	}
}

func isDelData(js string, cf map[string]interface{}) error {
	var delNode map[string]interface{}
	if err := json.Unmarshal([]byte(js), &delNode); nil != err {
		return err
	}
	for delk, delv := range delNode {
		if nil == delv {
			if _, ok := cf[delk]; ok {
				return fmt.Errorf("%s still exists", delk)
			}
		}

		switch t := delv.(type) {
		case string:
			if 0 == len(t) {
				if _, ok := cf[delk]; ok {
					return fmt.Errorf("%s still exists", delk)
				}
			}
		case map[string]interface{}:
			if b, err := json.Marshal(t); nil != err {
				return fmt.Errorf("request format error")
			} else {
				var auxCf map[string]interface{}
				if err := marshalUn(cf[delk], &auxCf); nil == err {
					if err := isDelData(string(b), auxCf); nil != err {
						return err
					}
				}
			}
		}

	}
	return nil
}
func isAddData(js string, cf map[string]interface{}) error {
	var addNode map[string]interface{}
	if err := json.Unmarshal([]byte(js), &addNode); nil != err {
		return err
	}
	for addk, _ := range addNode {
		if _, ok := cf[addk]; !ok {
			return fmt.Errorf("not found key:%s", addk)
		}
	}
	return nil
}
func marshalUn(input, output interface{}) error {
	jsonString, err := json.Marshal(input)
	if err != nil {
		return err
	}
	return json.Unmarshal(jsonString, output)
}

func compareUiCf(ui []*field, cf map[string]interface{}) (err error) {
	for i := 0; i < len(ui); i++ {
		if !ui[i].Exist {
			continue
		}
		if v, ok := cf[ui[i].Name]; ok {
			if v == ui[i].Default {
				continue
			}
			if nil == v || nil == ui[i].Default {
				return fmt.Errorf("default of %s is nil", ui[i].Name)
			}
			if reflect.Map == reflect.TypeOf(v).Kind() {
				var auxUi []*field
				if err = marshalUn(ui[i].Default, &auxUi); nil != err {
					return err
				}
				var auxCf map[string]interface{}
				if err = marshalUn(v, &auxCf); nil != err {
					return err
				}
				if err = compareUiCf(auxUi, auxCf); nil != err {
					return err
				}
			} else if ui[i].Default != v {
				return fmt.Errorf("not equal->%s:{cf:%v,ui:%v}", ui[i].Name, v, ui[i])
			}
		} else {
			return fmt.Errorf("%s is not in the configuration file", ui[i].Name)
		}

	}
	return nil
}

func compareUiTp(ui, tp []*field) (err error) {
	for i := 0; i < len(ui); i++ {
		j := 0
		for ; j < len(tp); j++ {
			if ui[i].Name != tp[j].Name {
				continue
			}

			if ui[i].Type != tp[j].Type {
				return fmt.Errorf("not equal->%s type:{tp:%v,ui:%v}", ui[i].Name, tp[j].Type, ui[i].Type)
			}
			if ui[i].Control != tp[j].Control {
				return fmt.Errorf("not equal->%s control:{tp:%v,ui:%v}", ui[i].Name, tp[j].Control, ui[i].Control)
			}
			if ui[i].Optional != tp[j].Optional {
				return fmt.Errorf("not equal->%s optional:{tp:%v,ui:%v}", ui[i].Name, tp[j].Optional, ui[i].Optional)
			}
			if ui[i].Values != tp[j].Values {
				return fmt.Errorf("not equal->%s values:{tp:%v,ui:%v}", ui[i].Name, tp[j].Values, ui[i].Values)
			}

			if ui[i].Hint != tp[j].Hint {
				if nil == ui[i].Hint || nil == tp[j].Hint {
					return fmt.Errorf("hint of %s is nil", ui[i].Name)
				}
				if ui[i].Hint.English != tp[j].Hint.English {
					return fmt.Errorf("not equal->%s hint.en_US:{tp:%v,ui:%v}", ui[i].Name, tp[j].Hint.English, ui[i].Hint.English)
				}
				if ui[i].Hint.Chinese != tp[j].Hint.Chinese {
					return fmt.Errorf("not equal->%s hint.zh_CN:{tp:%v,ui:%v}", ui[i].Name, tp[j].Hint.Chinese, ui[i].Hint.Chinese)
				}
			}

			if ui[i].Label != tp[j].Label {
				if nil == ui[i].Label || nil == tp[j].Label {
					return fmt.Errorf("label of %s is nil", ui[i].Name)
				}
				if ui[i].Label.English != tp[j].Label.English {
					return fmt.Errorf("not equal->%s label.en_US:{tp:%v,ui:%v}", ui[i].Name, tp[j].Label.English, ui[i].Label.English)
				}
				if ui[i].Label.Chinese != tp[j].Label.Chinese {
					return fmt.Errorf("not equal->%s label.zh_CN:{tp:%v,ui:%v}", ui[i].Name, tp[j].Label.Chinese, ui[i].Label.Chinese)
				}
			}

			if !ui[i].Exist {
				if nil == ui[i].Default || nil == tp[j].Default {
					return fmt.Errorf("The default of %s is nil", ui[i].Name)
					if reflect.Slice == reflect.TypeOf(ui[i].Default).Kind() {
						var auxUi, auxTp []*field
						if err = marshalUn(ui[i].Default, &auxUi); nil != err {
							return err
						}
						if err = marshalUn(tp[j].Default, &auxTp); nil != err {
							return err
						}

						if err = compareUiTp(auxUi, auxTp); nil != err {
							return err
						}
					} else if ui[i].Default != tp[j].Default {
						return fmt.Errorf("not equal->%s default:{tp:%v,ui:%v}", ui[i].Name, tp[j].Default, ui[i].Default)
					}
				}
			}

			break
		}
		if len(tp) == j {
			return fmt.Errorf("%s is not in the template file", ui[i].Name)
		}
	}
	return nil
}

func compare(source *sourceProperty, uiMeta *uiSource) (err error) {
	tp := source.meta.ConfKeys["default"]
	for k, v := range source.cf {
		ui := uiMeta.ConfKeys[k]
		if err = compareUiCf(ui, v); nil != err {
			return err
		}
		if err = compareUiTp(ui, tp); nil != err {
			return err
		}
	}
	return nil
}
