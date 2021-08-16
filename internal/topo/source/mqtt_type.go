// Copyright 2021 EMQ Technologies Co., Ltd.
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

package source

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type (
	property struct {
		Name     string `json:"name"`
		DataType string `json:"dataType"`
	}
	device struct {
		Name       string      `json:"name"`
		Properties []*property `json:"properties"`
	}
	deviceModel struct {
		Devices []*device `json:"deviceModels"`
	}
)

type modelVersion interface {
	checkType(map[string]interface{}, string) []string
}

func modelFactory(_ string) modelVersion {
	return new(deviceModel)
}
func (this *property) getName() string {
	return this.Name
}
func (this *property) getDataType() string {
	return this.DataType
}
func (this *device) getName() string {
	return this.Name
}
func (this *device) findDataType(name string) string {
	for _, v := range this.Properties {
		if strings.ToLower(v.getName()) == strings.ToLower(name) {
			return v.getDataType()
		}
	}
	return ""
}
func (this *deviceModel) findDataType(deviceId, dataName string) string {
	for _, v := range this.Devices {
		if strings.ToLower(v.getName()) == strings.ToLower(deviceId) {
			return v.findDataType(dataName)
		}
	}
	return ""
}
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
func intToBool(i int) bool {
	if 0 == i {
		return false
	}
	return true
}
func changeType(modelType string, data interface{}) (interface{}, string) {
	var err error
	dt := reflect.TypeOf(data)
	if dt == nil {
		return data, fmt.Sprintf("not support type : %v", nil)
	}
	switch dt.Kind() {
	case reflect.Bool:
		b, _ := data.(bool)
		switch modelType {
		case "int":
			data = boolToInt(b)
		case "bool":
			return data, ""
		default:
			return data, fmt.Sprintf("not support modelType : %s", modelType)
		}
	case reflect.Int:
		i, _ := data.(int)
		switch modelType {
		case "int":
			return data, ""
		case "float":
			data = float64(i)
		case "boolean":
			data = intToBool(i)
		case "string":
			data = strconv.Itoa(i)
		default:
			return data, fmt.Sprintf("not support modelType : %s", modelType)
		}
	case reflect.String:
		s, _ := data.(string)
		switch modelType {
		case "string":
			return data, ""
		case "float":
			data, err = strconv.ParseFloat(s, 64)
			if nil != err {
				return data, fmt.Sprintf("%v", err)
			}
		case "int":
			data, err = strconv.Atoi(s)
			if nil != err {
				return data, fmt.Sprintf("%v", err)
			}
		default:
			return data, fmt.Sprintf("not support modelType : %s", modelType)
		}
	case reflect.Float64:
		f, _ := data.(float64)
		switch modelType {
		case "double", "float":
			return data, ""
		case "int":
			data = int(f)
		case "string":
			data = strconv.FormatFloat(f, 'f', -1, 64)
		default:
			return data, fmt.Sprintf("not support modelType : %s", modelType)
		}
	default:
		return data, fmt.Sprintf("not support type : %v", dt.Kind())
	}
	return data, ""
}
func topicToDeviceid(topic string) string {
	sliStr := strings.Split(topic, `/`)
	if 4 > len(sliStr) {
		return ""
	}
	return sliStr[3]
}
func (this *deviceModel) checkType(m map[string]interface{}, topic string) []string {
	var sliErr []string
	strErr := ""
	for k, v := range m {
		deviceid := topicToDeviceid(topic)
		if 0 == len(deviceid) {
			sliErr = append(sliErr, fmt.Sprintf("not find deviceid : %s", topic))
			continue
		}
		modelType := this.findDataType(deviceid, k)
		if 0 == len(modelType) {
			continue
		}
		m[k], strErr = changeType(modelType, v)
		if 0 != len(strErr) {
			sliErr = append(sliErr, strErr)
			delete(m, k)
		}
	}
	return sliErr
}
