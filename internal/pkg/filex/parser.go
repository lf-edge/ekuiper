// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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

package filex

import (
	"github.com/lf-edge/ekuiper/pkg/message"
	"gopkg.in/yaml.v3"
	"io/ioutil"
)

func ReadJsonUnmarshal(path string, ret interface{}) error {
	sliByte, err := ioutil.ReadFile(path)
	if nil != err {
		return err
	}
	err = message.Unmarshal(sliByte, ret)
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
