package filex

import (
	"encoding/json"
	"gopkg.in/yaml.v3"
	"io/ioutil"
)

func ReadJsonUnmarshal(path string, ret interface{}) error {
	sliByte, err := ioutil.ReadFile(path)
	if nil != err {
		return err
	}
	err = json.Unmarshal(sliByte, ret)
	if nil != err {
		return err
	}
	return nil
}
func WriteYamlMarshal(path string, data interface{}) error {
	y, err := yaml.Marshal(data)
	if nil != err {
		return err
	}
	return ioutil.WriteFile(path, y, 0666)
}

func ReadYamlUnmarshal(path string, ret interface{}) error {
	sliByte, err := ioutil.ReadFile(path)
	if nil != err {
		return err
	}
	err = yaml.Unmarshal(sliByte, ret)
	if nil != err {
		return err
	}
	return nil
}
