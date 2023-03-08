// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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

package mqtt

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestCheckType(t *testing.T) {
	strModel := `{"deviceModels":[{"name":"device1","properties":[{"name":"temperature","dataType":"int"},{"name":"humidity","dataType":"int"}]},{"name":"device2","properties":[{"name":"temperature","dataType":"string"},{"name":"humidity","dataType":"string"}]}]}`
	mode := new(deviceModel)
	json.Unmarshal([]byte(strModel), mode)

	datas := []map[string]interface{}{
		{"temperature": 1, "humidity": 1},
		{"temperature": "1", "humidity": "1"},
		{"temperature": 1, "humidity": "1"},
		{"temperature": "1", "humidity": 1},
		{"temperature": nil, "humidity": "1"},
	}

	topics := []string{`$ke/events/device/device1/data/update`, `$ke/events/device/device2/data/update`}

	for _, topic := range topics {
		for _, data := range datas {
			mode.checkType(data, topic)
			for k, v := range data {
				deviceid := topicToDeviceid(topic)
				modelType := mode.findDataType(deviceid, k)
				dataType := reflect.TypeOf(v).String()
				if modelType != dataType {
					t.Errorf("data:%s=%s mode:%s=%s\n", k, dataType, k, modelType)
				}
			}
		}
	}
}
