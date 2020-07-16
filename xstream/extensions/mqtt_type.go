package extensions

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
		if v.getName() == name {
			return v.getDataType()
		}
	}
	return ""
}
func (this *deviceModel) findDataType(deviceId, dataName string) string {
	for _, v := range this.Devices {
		if v.getName() == deviceId {
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
	dataType := reflect.TypeOf(data).Kind()
	switch dataType {
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
			data, _ = strconv.ParseFloat(s, 64)
		case "int":
			data, _ = strconv.Atoi(s)
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
		return data, fmt.Sprintf("not support type : %v", dataType)
	}
	return data, ""
}
func topicToDeviceid(topic string) string {
	sliStr := strings.Split(topic, "+")
	if 2 != len(sliStr) {
		return ""
	}
	sliStr = strings.Split(sliStr[1], `/`)
	if 0 == len(sliStr) {
		return ""
	}
	return sliStr[0]
}
func checkType(mode *deviceModel, m map[string]interface{}, topic string) []string {
	var sliErr []string
	strErr := ""
	for k, v := range m {
		deviceid := topicToDeviceid(topic)
		if 0 == len(deviceid) {
			sliErr = append(sliErr, fmt.Sprintf("not find deviceid : %s", topic))
			continue
		}
		modelType := mode.findDataType(deviceid, k)
		if 0 == len(modelType) {
			continue
		}
		m[k], strErr = changeType(modelType, v)
		if 0 != len(strErr) {
			sliErr = append(sliErr, strErr)
		}
	}
	return sliErr
}
