package extensions

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
	}

	topics := []string{`$ke/events/device/device1/data/update`, `$ke/events/device/device2/data/update`}

	for _, topic := range topics {
		for _, data := range datas {
			strErrs := mode.checkType(data, topic)
			for k, v := range data {
				deviceid := topicToDeviceid(topic)
				modelType := mode.findDataType(deviceid, k)
				dataType := reflect.TypeOf(v).String()
				if modelType != dataType {
					t.Errorf("data:%s=%s mode:%s=%s err:%v\n", k, dataType, k, modelType, strErrs)
				}
			}
		}
	}
}
